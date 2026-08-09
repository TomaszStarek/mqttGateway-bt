using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Protocol;
using System;
using System.Globalization;
using System.IO.Ports;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace MqttModbusGateway
{
    /// <summary>
    /// Manages the lifecycle of a single CEM3-G-BT torque wrench connected via
    /// Bluetooth Classic (SPP profile) exposed as a virtual serial COM port.
    ///
    /// Responsibilities:
    /// <list type="bullet">
    ///   <item>Open and maintain the serial connection, auto-reconnect on loss.</item>
    ///   <item>Read DF-3 frames line-by-line and parse them into <see cref="FasteningEvent"/> objects.</item>
    ///   <item>Publish fastening events and connectivity state to AWS IoT via MQTT.</item>
    ///   <item>Execute AT write commands received from the cloud (see <see cref="ExecuteCommandAsync"/>).</item>
    /// </list>
    ///
    /// Topic layout:
    /// <list type="bullet">
    ///   <item><c>{thingName}/{address}/data</c>  — fastening event payload</item>
    ///   <item><c>{thingName}/{address}/state</c> — connectivity state (connected / disconnected)</item>
    ///   <item><c>{thingName}/{address}/commands</c> — inbound commands (routed by <see cref="Gateway"/>)</item>
    /// </list>
    ///
    /// DF-3 frame format (manual §6.4):
    /// <code>
    ///   RE,001,+100.0nm,+090,deg,OO,1234567,18/12/31,12:59:59CRLF
    /// </code>
    /// </summary>
    internal sealed class DeviceWorker : IDisposable
    {
        // -----------------------------------------------------------------------
        // Constants
        // -----------------------------------------------------------------------

        /// <summary>Number of fields in a valid DF-3 frame when split on ','.</summary>
        private const int Df3FieldCount = 9;

        /// <summary>
        /// How long to wait between reconnection attempts when the COM port is unavailable.
        /// </summary>
        private const int ReconnectDelayMs = 5_000;

        /// <summary>
        /// Read timeout on the serial port in milliseconds.
        /// The wrench sends data asynchronously, so a generous timeout is fine;
        /// the important thing is that ReadLine() eventually throws on disconnect.
        /// </summary>
        private const int SerialReadTimeoutMs = 10_000;

        // -----------------------------------------------------------------------
        // Fields
        // -----------------------------------------------------------------------

        private readonly DeviceConfig _cfg;
        private readonly string _thingName;
        private readonly IMqttClient _mqtt;
        private readonly CancellationTokenSource _cts = new();

        private Task? _workerTask;
        private SerialPort? _port;
        private bool _connected;

        private DateTime _lastResponseUtc = DateTime.MinValue;

        private float _targetTorqueHighNm;
        private float _targetTorqueLowNm;
        private int _targetAngleHighDeg;
        private int _targetAngleLowDeg;
        private int _doubleDetectionAngleDeg;
        private float _targetTorqueTriggerNm;

        /// <summary>ID of the job step from the last received "configureJob" command.</summary>
        private int _currentStepId;

        private bool _initialStateSent = false;
        private readonly ILogger<DeviceWorker> _logger;

        private readonly SemaphoreSlim _commandLock = new(1, 1);

        /// <summary>
        /// Last observed memory counter from the wrench (1–999).
        /// Initialised to -1 so the first received frame is always treated as a new event.
        /// </summary>
        private int _lastEventCount = -1;

        // -----------------------------------------------------------------------
        // Constructor
        // -----------------------------------------------------------------------

        /// <summary>
        /// Initialises a new <see cref="DeviceWorker"/> for the specified wrench.
        /// Call <see cref="Start"/> to begin listening.
        /// </summary>
        /// <param name="cfg">Wrench connection parameters (COM port, baud rate, etc.).</param>
        /// <param name="thingName">AWS IoT Thing name used as the MQTT topic prefix.</param>
        /// <param name="mqtt">Connected MQTT client used for publishing telemetry.</param>
        public DeviceWorker(DeviceConfig cfg, string thingName, IMqttClient mqtt, ILogger<DeviceWorker> logger)
        {
            _cfg = cfg;
            _thingName = thingName;
            _mqtt = mqtt;
            _logger = logger;
        }

        /// <summary>
        /// The COM port / address this worker is bound to. Used by <see cref="Gateway"/>
        /// to route commands arriving on <c>{thingName}/{address}/commands</c> to this worker.
        /// </summary>
        public string Address => _cfg.Address;

        /// <summary>The gateway-assigned device identifier for this worker.</summary>
        public string DeviceId => _cfg.DeviceId;

        // -----------------------------------------------------------------------
        // Lifecycle
        // -----------------------------------------------------------------------

        /// <summary>
        /// Starts the background read loop on a thread-pool task.
        /// Returns immediately.
        /// </summary>
        public void Start()
        {
            _workerTask = Task.Run(() => ReadLoopAsync(_cts.Token));
        }

        /// <summary>
        /// Cancels the read loop and waits for it to finish, then closes the port.
        /// </summary>
        public async Task StopAsync()
        {
            try
            {
                _cts.Cancel();
                if (_workerTask is not null)
                    await _workerTask;
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                _logger.LogInformation($"[{_cfg.DeviceId}] Worker cancelled gracefully StopAsync.");
            }

            ClosePort();
            await PublishStateAsync(connected: false, CancellationToken.None, disconnectReason: "Worker stopped by app");
            _logger.LogInformation($"[{_cfg.DeviceId}] Worker stopped.");
        }

        // -----------------------------------------------------------------------
        // Main read loop
        // -----------------------------------------------------------------------

        /// <summary>
        /// Main loop: opens the COM port, reads DF-3 frames line by line,
        /// parses and publishes fastening events.  Reconnects automatically on error.
        /// </summary>
        private async Task ReadLoopAsync(CancellationToken ct)
        {
            _logger.LogInformation($"[{_cfg.DeviceId}] Worker started");

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    if (_port is null || !_port.IsOpen)
                    {
                        await TryOpenPortAsync(ct);
                        await Task.Delay(1000, ct);
                        continue;
                    }

                    await SendHeartbeatAsync();

                    if (_connected &&
                        DateTime.UtcNow - _lastResponseUtc > TimeSpan.FromSeconds(10))
                    {
                        _logger.LogInformation(
                            $"[{_cfg.DeviceId}] Heartbeat timeout. Reconnecting...");

                        await HandleDisconnectAsync(ct, reason: "Heartbeat timeout. Reconnecting");
                    }

                    await Task.Delay(1000, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    _logger.LogInformation($"[{_cfg.DeviceId}] Worker cancelled gracefully ReadLoopAsync.");
                }
                catch (Exception ex)
                {
                    _logger.LogCritical($"[{_cfg.DeviceId}] Loop error: {ex.Message}");
                    await HandleDisconnectAsync(ct, reason: ex.Message);
                    await Task.Delay(ReconnectDelayMs, ct);
                }
            }
        }

        // -----------------------------------------------------------------------
        // Port management
        // -----------------------------------------------------------------------

        /// <summary>
        /// Attempts to open the configured COM port.
        /// On failure, <see cref="_port"/> is set to <c>null</c>.
        /// </summary>
        private Task? _readTask;

        private async Task TryOpenPortAsync(CancellationToken ct)
        {
            ClosePort();
            try
            {
                _port = new SerialPort(_cfg.Address, _cfg.BaudRate)
                {
                    DataBits = 8,
                    Parity = Parity.None,
                    StopBits = StopBits.One,
                    Handshake = Handshake.RequestToSend,
                    Encoding = Encoding.ASCII,
                    NewLine = "\r\n",
                    ReadTimeout = SerialReadTimeoutMs
                };

                await Task.Run(() => _port.Open(), ct);

                _initialStateSent = true;
                await PublishStateAsync(connected: true, ct);

                _readTask = Task.Run(() => ReadLinesAsync(ct), ct);

                _logger.LogInformation($"[{_cfg.DeviceId}] Port {_cfg.Address} otwarty.");
            }
            catch (Exception ex)
            {


                if (_connected || !_initialStateSent)
                {
                    _logger.LogInformation($"[{_cfg.DeviceId}] Nie można otworzyć {_cfg.Address}: {ex.Message}");
                    _connected = false;
                    _initialStateSent = true;
                    await PublishStateAsync(connected: false, _cts.Token, disconnectReason: ex.Message);
                }

                ClosePort();
            }
        }

        private async Task ReadLinesAsync(CancellationToken ct)
        {
            using var reader = new StreamReader(_port!.BaseStream, Encoding.ASCII,
                                                 detectEncodingFromByteOrderMarks: false,
                                                 leaveOpen: true);
            try
            {
                while (!ct.IsCancellationRequested && _port.IsOpen)
                {
                    string? line = await reader.ReadLineAsync(ct);

                    if (line is not null)
                        await ProcessLineAsync(line.Trim());
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogInformation($"[{_cfg.DeviceId}] Worker cancelled gracefully ReadLinesAsync.");
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"[{_cfg.DeviceId}] Błąd odczytu: {ex.Message}");
                await HandleDisconnectAsync(ct);
            }
        }


        private async Task ProcessLineAsync(string line)
        {
            if (string.IsNullOrWhiteSpace(line))
                return;

            if (!_connected)
            {
                _connected = true;
                await PublishStateAsync(true, _cts.Token);
            }

            _lastResponseUtc = DateTime.UtcNow;

            if (line.StartsWith("E", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            _logger.LogInformation($"[{_cfg.DeviceId}] RX: {line}");

            if (!line.StartsWith("RE,", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation($"[{_cfg.DeviceId}] Ignored: {line}");
                return;
            }

            if (TryParseDf3Frame(line, out FasteningEvent? ev) && ev is not null)
            {
                if (ev.EventCount != _lastEventCount)
                {
                    _lastEventCount = ev.EventCount;

                    await PublishJsonAsync($"{_thingName}/log", ev, _cts.Token);

                    var res = new ResultsToSend(
                        TargetTorqueLowNm: ev.TargetTorqueLowNm,
                        TargetTorqueHighNm: ev.TargetTorqueHighNm,
                        ConvertedTorqueNm: ev.ConvertedTorqueNm,
                        TargetTorqueTriggerNm: ev.TargetTorqueTriggerNm,
                        DoubleDetectionAngleDeg: ev.DoubleDetectionAngleDeg,
                        TargetAngleLowDeg: ev.TargetAngleLowDeg,
                        TargetAngleHighDeg: ev.TargetAngleHighDeg,
                        TotalAngleDeg: ev.TotalAngleDeg,
                        IsLoosening: ev.IsLoosening,
                        DeviceId: _cfg.Address,
                        Timestamp: ev.Timestamp,
                        Result: ev.Result,
                        StepId: ev.StepId
                    );

                    await PublishJsonAsync($"{_thingName}/{_cfg.Address}/data", res, _cts.Token);



                    _logger.LogInformation(
                        $"[{_cfg.DeviceId}] Event #{ev.EventCount} — " +
                        $"StepId: {ev.StepId}, " +
                        $"TorqueTaget: {ev.TargetTorqueLowNm:F2}-{ev.TargetTorqueHighNm}, " +
                        $"Torque: {ev.ConvertedTorqueNm:F2}, " +
                        $"AngleTarget: {ev.TargetAngleLowDeg:F2}-{ev.TargetAngleHighDeg}, " +
                        $"Angle: {ev.TotalAngleDeg}, " +
                        $"Judgment: {ev.Result}");
                }
            }
            else
            {
                _logger.LogWarning($"[{_cfg.DeviceId}] Parse failed: {line}");
            }
        }


        private async Task SendHeartbeatAsync()
        {
            if (_port is null || !_port.IsOpen)
                return;

            try
            {
                string cmd = "q\r\n";

                byte[] bytes = Encoding.ASCII.GetBytes(cmd);

                await Task.Run(() =>
                    _port.Write(bytes, 0, bytes.Length));

                //  Console.WriteLine($"[{_cfg.DeviceId}] Heartbeat sent");
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"[{_cfg.DeviceId}] Heartbeat write failed: {ex.Message}");

                throw;
            }
        }


        /// <summary>
        /// Called when a communication error is detected.
        /// Publishes a disconnected state (if previously connected) and closes the port.
        /// </summary>
        private async Task HandleDisconnectAsync(CancellationToken ct, string reason = "communication error")
        {

            if (_connected)
            {
                _connected = false;
                await PublishStateAsync(connected: false, ct, disconnectReason: reason);
            }

            ClosePort();
        }

        /// <summary>
        /// Closes and disposes the serial port without throwing.
        /// </summary>
        private void ClosePort()
        {
            try { _port?.Close(); } catch { }
            try { _port?.Dispose(); } catch { }
            _port = null;
        }

        // -----------------------------------------------------------------------
        // DF-3 frame parser
        // -----------------------------------------------------------------------

        /// <summary>
        /// Parses a single DF-3 data frame into a <see cref="FasteningEvent"/>.
        ///
        /// Frame layout:
        /// <code>
        /// Field  1: "RE"              — header
        /// Field  2: "001"             — 3-digit memory counter
        /// Field  3: "+100.0nm"        — signed torque + unit (no space)
        /// Field  4: "+090"            — signed angle in degrees
        /// Field  5: "deg"             — angle unit literal
        /// Field  6: "OO"             — 2-char judgment code
        /// Field  7: "1234567"         — 7-char device ID / serial number
        /// Field  8: "18/12/31"        — date yy/mm/dd
        /// Field  9: "12:59:59"        — time hh:mm:ss
        /// </code>
        /// </summary>
        private bool TryParseDf3Frame(string line, out FasteningEvent? ev)
        {
            ev = null;

            if (string.IsNullOrWhiteSpace(line))
                return false;

            line = line.Trim();

            if (!line.StartsWith("RE,", StringComparison.OrdinalIgnoreCase))
                return false;

            string[] fields = line.Split(',');

            if (fields.Length != 10)
            {
                _logger.LogWarning(
                    $"[{_cfg.DeviceId}] Invalid field count: {fields.Length}");
                return false;
            }

            // --------------------------------------------------
            // 0 = RE
            // 1 = counter
            // 2 = torque value
            // 3 = torque unit
            // 4 = angle
            // 5 = deg
            // 6 = judgment
            // 7 = serial
            // 8 = date
            // 9 = time
            // --------------------------------------------------

            // EVENT COUNT
            if (!int.TryParse(fields[1].Trim(), out int eventCount))
                return false;

            // TORQUE
            string torqueRaw = fields[2].Trim();

            if (!float.TryParse(
                    torqueRaw,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out float torque))
            {
                return false;
            }

            string torqueUnit = fields[3].Trim();

            bool isLoosening = torque < 0;

            torque = Math.Abs(torque);

            // ANGLE
            if (!int.TryParse(fields[4].Trim(), out int angleSigned))
                return false;

            int angleAbs = Math.Abs(angleSigned);

            // JUDGMENT
            string judgment = fields[6].Trim().ToUpperInvariant();

            bool torqueOk = judgment[0] == 'O';
            bool angleOk = judgment[1] == 'O';

            bool resultOk = torqueOk && angleOk;

            // SERIAL
            string frameSerial = fields[7].Trim();

            if (!string.IsNullOrEmpty(_cfg.SerialNumber) &&
                !string.Equals(
                    frameSerial,
                    _cfg.SerialNumber,
                    StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation(
                    $"[{_cfg.DeviceId}] Serial mismatch: {frameSerial}");

                return false;
            }

            // TIMESTAMP
            string timestamp = BuildTimestamp(
                fields[8].Trim(),
                fields[9].Trim());

            ev = new FasteningEvent(
                TargetTorqueHighNm: _targetTorqueHighNm,
                TargetTorqueLowNm: _targetTorqueLowNm,
                TargetAngleHighDeg: _targetAngleHighDeg,
                TargetAngleLowDeg: _targetAngleLowDeg,
                DoubleDetectionAngleDeg: _doubleDetectionAngleDeg,
                TargetTorqueTriggerNm: _targetTorqueTriggerNm,

                EventCount: eventCount,

                ConvertedTorqueNm: torque,

                IsLoosening: isLoosening,

                TotalAngleDeg: angleAbs,

                Timestamp: timestamp,

                Result: resultOk,

                DeviceId: _cfg.DeviceId,

                StepId: _currentStepId
            );

            return true;
        }


        /// <summary>
        /// Constructs an ISO 8601 UTC timestamp string from the date and time
        /// fields embedded in the DF-3 frame.
        /// </summary>
        /// <param name="datePart">Date in <c>yy/mm/dd</c> format.</param>
        /// <param name="timePart">Time in <c>hh:mm:ss</c> format.</param>
        private static string BuildTimestamp(string datePart, string timePart)
        {
            try
            {
                // datePart = "18/12/31" → year=2018, month=12, day=31
                string[] dp = datePart.Split('/');
                string[] tp = timePart.Split(':');

                int year = 2000 + int.Parse(dp[0]);
                int month = int.Parse(dp[1]);
                int day = int.Parse(dp[2]);
                int hour = int.Parse(tp[0]);
                int min = int.Parse(tp[1]);
                int sec = int.Parse(tp[2]);

                // The wrench clock is local time; we tag it as UTC here.
                // In a real deployment you may want to apply a UTC offset.
                var dt = new DateTime(year, month, day, hour, min, sec, DateTimeKind.Utc);
                return dt.ToString("o");
            }
            catch
            {
                return DateTime.UtcNow.ToString("o");
            }
        }

        // -----------------------------------------------------------------------
        // Cloud commands
        // -----------------------------------------------------------------------

        /// <summary>
        /// Sends an AT command string to the wrench over the open serial port.
        /// The wrench manual §6.5 defines the available commands (AT037, AT045, AT046, AT023).
        /// If the port is not open the command is silently dropped with a warning.
        /// </summary>
        /// <param name="cmd">Command payload received from the MQTT commands topic.</param>
        public async Task ExecuteCommandAsync(Command cmd)
        {
            if (_port is null || !_port.IsOpen)
            {
                _logger.LogWarning($"[{_cfg.DeviceId}] Command dropped — port not open.");
                return;
            }

            try
            {
                // AT commands must be terminated with CRLF (manual §6.5).
                string frame = cmd.AtCommand!.TrimEnd() + "\r\n";
                byte[] bytes = Encoding.ASCII.GetBytes(frame);

                // Serial writes are synchronous; run on thread pool.
                await Task.Run(() => _port.Write(bytes, 0, bytes.Length));

                _logger.LogInformation($"[{_cfg.DeviceId}] Command sent: {cmd.AtCommand}");
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"[{_cfg.DeviceId}] Command error: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // MQTT helpers
        // -----------------------------------------------------------------------

        /// <summary>
        /// Publishes a connectivity state change to <c>{thingName}/{address}/state</c>.
        /// </summary>
        private async Task PublishStateAsync(bool connected, CancellationToken ct, string? disconnectReason = null)
        {
            var payload = new
            {
                connected,
                disconnectReason = connected ? null : (disconnectReason ?? "unknown"),
            };

            await PublishJsonAsync($"{_thingName}/{_cfg.Address}/state", payload, ct);
        }

        /// <summary>
        /// Serialises <paramref name="payload"/> to JSON (camelCase) and publishes it
        /// to <paramref name="topic"/> with QoS 1.
        /// The method is a no-op when the MQTT client is not connected.
        /// </summary>
        private async Task PublishJsonAsync<T>(string topic, T payload, CancellationToken ct)
        {
            if (!_mqtt.IsConnected)
                return;

            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            var msg = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(json)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqtt.PublishAsync(msg, ct);
        }

        /// <summary>
        /// Applies a "configureJob" command: stores the target/limit values so subsequent
        /// fastening events are tagged with them, remembers <paramref name="stepId"/>,
        /// and pushes the AT037 / AT045 / AT046 frames to the wrench.
        /// </summary>
        /// <param name="torqueHighNm">Upper torque limit [Nm] (target * TorqueMaxPercentage / 100).</param>
        /// <param name="torqueLowNm">Lower torque limit [Nm] (target * TorqueMinPercentage / 100).</param>
        /// <param name="angleHighDeg">Upper angle limit [deg]. Effectively unused — sent as protocol max.</param>
        /// <param name="angleLowDeg">Lower/minimum angle [deg], from the configurator.</param>
        /// <param name="doubleDetectionAngleDeg">Not used — always 0.</param>
        /// <param name="triggerNm">Torque at which angle measurement starts [Nm]
        /// (target * RotationStartThresholdPercentage / 100).</param>
        /// <param name="stepId">ID of the selected job step, stored and attached to future events.</param>
        public async Task ConfigureJobAsync(
            float torqueHighNm,
            float torqueLowNm,
            int angleHighDeg,
            int angleLowDeg,
            int doubleDetectionAngleDeg,
            float triggerNm,
            int stepId)
        {
            await _commandLock.WaitAsync();

            try
            {
                UpdateTargets(
                    torqueHighNm,
                    torqueLowNm,
                    angleHighDeg,
                    angleLowDeg,
                    doubleDetectionAngleDeg,
                    triggerNm,
                    stepId);

                // ------------------------------------------------
                // AT037
                // torque upper/lower
                // ------------------------------------------------

                string at037 =
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "AT037,{0:00.00},{1:00.00}",
                        torqueHighNm,
                        torqueLowNm);

                await ExecuteRawInternalAsync(at037);

                await Task.Delay(200);

                // ------------------------------------------------
                // AT045
                // trigger torque
                // ------------------------------------------------

                string at045 =
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "AT045,{0:00.00}",
                        triggerNm);

                await ExecuteRawInternalAsync(at045);

                await Task.Delay(200);

                // ------------------------------------------------
                // AT046
                // angle limits (double-detection, low, high) — all 3-digit fields
                // ------------------------------------------------

                string at046 =
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "AT046,{0:000},{1:000},{2:000}",
                        doubleDetectionAngleDeg,
                        angleLowDeg,
                        angleHighDeg);

                await ExecuteRawInternalAsync(at046);

                _logger.LogInformation(
                    $"[{_cfg.DeviceId}] Job configured — StepId={stepId}, " +
                    $"Torque={torqueLowNm:F2}-{torqueHighNm:F2}Nm, Trigger={triggerNm:F2}Nm, " +
                    $"Angle>={angleLowDeg}deg (upper unchecked)");
            }
            finally
            {
                _commandLock.Release();
            }
        }

        private async Task ExecuteRawInternalAsync(string command)
        {
            if (_port is null || !_port.IsOpen)
            {
                _logger.LogInformation(
                    $"[{_cfg.DeviceId}] Port not open");

                return;
            }

            string frame = command.TrimEnd() + "\r\n";

            byte[] bytes = Encoding.ASCII.GetBytes(frame);

            await Task.Run(() =>
                _port.Write(bytes, 0, bytes.Length));

            _logger.LogInformation(
                $"[{_cfg.DeviceId}] TX: {command}");
        }

        public async Task ExecuteRawAsync(string command)
        {
            await _commandLock.WaitAsync();

            try
            {
                await ExecuteRawInternalAsync(command);
            }
            finally
            {
                _commandLock.Release();
            }
        }

        public void UpdateTargets(
            float torqueHighNm,
            float torqueLowNm,
            int angleHighDeg,
            int angleLowDeg,
            int doubleDetectionAngleDeg,
            float triggerNm,
            int stepId)
        {
            _targetTorqueHighNm = torqueHighNm;
            _targetTorqueLowNm = torqueLowNm;

            _targetAngleHighDeg = angleHighDeg;
            _targetAngleLowDeg = angleLowDeg;

            _doubleDetectionAngleDeg =
                doubleDetectionAngleDeg;

            _targetTorqueTriggerNm = triggerNm;

            _currentStepId = stepId;
        }

        // -----------------------------------------------------------------------
        // IDisposable
        // -----------------------------------------------------------------------

        /// <inheritdoc/>
        public void Dispose()
        {
            _cts.Cancel();
            ClosePort();
            _cts.Dispose();
        }
    }
}