using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Protocol;
using System;
using System.Globalization;
using System.IO;
using System.IO.Ports;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MqttModbusGateway
{
    internal sealed class DeviceWorker : IDisposable
    {
        private const int Df3FieldCount = 9;
        private const int ReconnectDelayMs = 5_000;
        private const int SerialReadTimeoutMs = 10_000;

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

        private bool _initialStateSent = false;
        private readonly ILogger<DeviceWorker> _logger;

        private readonly SemaphoreSlim _commandLock = new(1, 1);

        private int _lastEventCount = -1;

        private int _currentStepId;
        private int _currentBatchId;
        private int _currentUserId;
        private int _currentDeviceId;

        public DeviceWorker(DeviceConfig cfg, string thingName, IMqttClient mqtt, ILogger<DeviceWorker> logger)
        {
            _cfg = cfg;
            _thingName = thingName;
            _mqtt = mqtt;
            _logger = logger;
        }

        public string Address => _cfg.Address;
        public string DeviceId => _cfg.DeviceId;
        public string CleanAddress => _cfg.CleanAddress;

        public void Start()
        {
            _workerTask = Task.Run(() => ReadLoopAsync(_cts.Token));
        }

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

                    if (_connected && DateTime.UtcNow - _lastResponseUtc > TimeSpan.FromSeconds(10))
                    {
                        _logger.LogInformation($"[{_cfg.DeviceId}] Heartbeat timeout. Reconnecting...");
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

        private Task? _readTask;

        private async Task TryOpenPortAsync(CancellationToken ct)
        {
            ClosePort();
            await Task.Delay(300, ct);
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

                _logger.LogInformation($"[{_cfg.DeviceId}] Port {_cfg.CleanAddress} opened.");
            }
            catch (Exception ex)
            {
                if (_connected || !_initialStateSent)
                {
                    _logger.LogInformation($"[{_cfg.DeviceId}] Cannot open {_cfg.CleanAddress}: {ex.Message}");
                    _connected = false;
                    _initialStateSent = true;
                    await PublishStateAsync(connected: false, _cts.Token, disconnectReason: ex.Message);
                }

                ClosePort();
            }
        }

        private async Task ReadLinesAsync(CancellationToken ct)
        {
            try
            {
                using var reader = new StreamReader(_port!.BaseStream, Encoding.ASCII);

                while (!ct.IsCancellationRequested && _port is not null && _port.IsOpen)
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
                _logger.LogWarning($"[{_cfg.DeviceId}] Read error/disconnect: {ex.Message}");
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
                return;

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
                        DeviceId: ev.DeviceId,
                        StepId: ev.StepId,
                        BatchId: ev.BatchId,
                        UserId: ev.UserId,
                        ConvertedTorqueNm: ev.ConvertedTorqueNm,
                        TargetTorqueLowNm: ev.TargetTorqueLowNm,
                        TargetTorqueHighNm: ev.TargetTorqueHighNm,
                        TotalAngleDeg: ev.TotalAngleDeg,
                        IsLoosening: !ev.IsLoosening, // Poprawiono z sztywnej wartości true
                        Result: ev.Result,
                        TargetSpeedRpm: 0,
                        FasteningTimeMs: 0,
                        A1Deg: 0,
                        A2Deg: 0,
                        SnugTorqueAngle: 0
                    );

                    // POPRAWIONO LINIKĘ PONIŻEJ (dodano cudzysłów i nawias klamrowy)
                    await PublishJsonAsync($"{_thingName}/{_cfg.CleanAddress}/data", res, _cts.Token);

                    _logger.LogInformation(
                        $"[{_cfg.DeviceId}] Event #{ev.EventCount} — " +
                        $"StepId: {ev.StepId}, BatchId: {ev.BatchId}, UserId: {ev.UserId}, " +
                        $"Torque: {ev.ConvertedTorqueNm:F2} Nm, Result: {(ev.Result ? "PASS" : "FAIL")}");
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

                await Task.Run(() => _port.Write(bytes, 0, bytes.Length));
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"[{_cfg.DeviceId}] Heartbeat write failed: {ex.Message}");
                throw;
            }
        }

        private async Task HandleDisconnectAsync(CancellationToken ct, string reason = "communication error")
        {
            if (_connected)
            {
                _connected = false;
                await PublishStateAsync(connected: false, ct, disconnectReason: reason);
            }

            ClosePort();
        }

        private void ClosePort()
        {
            try
            {
                if (_port is not null && _port.IsOpen)
                {
                    _port.Close();
                }
            }
            catch { }

            try { _port?.Dispose(); } catch { }
            _port = null;
        }

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
                _logger.LogWarning($"[{_cfg.DeviceId}] Invalid field count: {fields.Length}");
                return false;
            }

            if (!int.TryParse(fields[1].Trim(), out int eventCount))
                return false;

            string torqueRaw = fields[2].Trim();

            if (!float.TryParse(
                    torqueRaw,
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out float torque))
            {
                return false;
            }

            string torqueUnit = fields[3].Trim();
            bool isLoosening = torque < 0;
            torque = Math.Abs(torque);

            if (!int.TryParse(fields[4].Trim(), out int angleSigned))
                return false;

            int angleAbs = Math.Abs(angleSigned);

            string judgment = fields[6].Trim().ToUpperInvariant();
            bool torqueOk = judgment[0] == 'O';
            bool angleOk = true;
            bool resultOk = torqueOk && angleOk;

            string frameSerial = fields[7].Trim();

            if (!string.IsNullOrEmpty(_cfg.SerialNumber) &&
                !string.Equals(frameSerial, _cfg.SerialNumber, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation($"[{_cfg.DeviceId}] Serial mismatch: {frameSerial}");
                return false;
            }

            string timestamp = BuildTimestamp(fields[8].Trim(), fields[9].Trim());

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
                DeviceId: _currentDeviceId,
                StepId: _currentStepId,
                BatchId: _currentBatchId,
                UserId: _currentUserId
            );

            return true;
        }

        private static string BuildTimestamp(string datePart, string timePart)
        {
            try
            {
                string[] dp = datePart.Split('/');
                string[] tp = timePart.Split(':');

                int year = 2000 + int.Parse(dp[0]);
                int month = int.Parse(dp[1]);
                int day = int.Parse(dp[2]);
                int hour = int.Parse(tp[0]);
                int min = int.Parse(tp[1]);
                int sec = int.Parse(tp[2]);

                var dt = new DateTime(year, month, day, hour, min, sec, DateTimeKind.Utc);
                return dt.ToString("o");
            }
            catch
            {
                return DateTime.UtcNow.ToString("o");
            }
        }

        public async Task ExecuteCommandAsync(Command cmd)
        {
            if (_port is null || !_port.IsOpen)
            {
                _logger.LogWarning($"[{_cfg.DeviceId}] Command dropped — port not open.");
                return;
            }

            try
            {
                string frame = cmd.AtCommand!.TrimEnd() + "\r\n";
                byte[] bytes = Encoding.ASCII.GetBytes(frame);

                await Task.Run(() => _port.Write(bytes, 0, bytes.Length));

                _logger.LogInformation($"[{_cfg.DeviceId}] Command sent: {cmd.AtCommand}");
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"[{_cfg.DeviceId}] Command error: {ex.Message}");
            }
        }

        private async Task PublishStateAsync(bool connected, CancellationToken ct, string? disconnectReason = null)
        {
            var payload = new
            {
                connected,
                disconnectReason = connected ? null : (disconnectReason ?? "unknown"),
            };

            await PublishJsonAsync($"{_thingName}/{_cfg.CleanAddress}/state", payload, ct);
        }

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

        public async Task ConfigureJobAsync(
            float torqueHighNm,
            float torqueLowNm,
            int angleHighDeg,
            int angleLowDeg,
            int doubleDetectionAngleDeg,
            float triggerNm,
            int stepId,
            int batchId,
            int userId,
            int deviceId)
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
                    stepId,
                    batchId,
                    userId,
                    deviceId);

                string at037 = string.Format(CultureInfo.InvariantCulture, "AT037,{0:00.00},{1:00.00}", torqueHighNm, torqueLowNm);
                await ExecuteRawInternalAsync(at037);
                await Task.Delay(500);

                ///angle turned off for now, because they dont use angle
                //string at045 = string.Format(CultureInfo.InvariantCulture, "AT045,{0:00.00}", triggerNm);
                //await ExecuteRawInternalAsync(at045);
                //await Task.Delay(500);

                //string at046 = string.Format(CultureInfo.InvariantCulture, "AT046,{0:000},{1:000},{2:000}", doubleDetectionAngleDeg, angleLowDeg, angleHighDeg);
                //await ExecuteRawInternalAsync(at046);

                _logger.LogInformation(
                    $"[{_cfg.DeviceId}] Job configured — StepId={stepId}, BatchId={batchId}, UserId={userId}, " +
                    $"Torque={torqueLowNm:F2}-{torqueHighNm:F2}Nm, Trigger={triggerNm:F2}Nm, Angle>={angleLowDeg}deg");
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
            int stepId,
            int batchId,
            int userId,
            int deviceId)
        {
            _targetTorqueHighNm = torqueHighNm;
            _targetTorqueLowNm = torqueLowNm;
            _targetAngleHighDeg = angleHighDeg;
            _targetAngleLowDeg = angleLowDeg;
            _doubleDetectionAngleDeg = doubleDetectionAngleDeg;
            _targetTorqueTriggerNm = triggerNm;

            _currentStepId = stepId;
            _currentBatchId = batchId;
            _currentUserId = userId;
            _currentDeviceId = deviceId;
        }

        private async Task ExecuteRawInternalAsync(string command)
        {
            if (_port is null || !_port.IsOpen)
            {
                _logger.LogInformation($"[{_cfg.DeviceId}] Port not open");
                return;
            }

            string frame = command.TrimEnd() + "\r\n";
            byte[] bytes = Encoding.ASCII.GetBytes(frame);

            await Task.Run(() => _port.Write(bytes, 0, bytes.Length));

            _logger.LogInformation($"[{_cfg.DeviceId}] TX: {command}");
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

        public void Dispose()
        {
            _cts.Cancel();
            ClosePort();
            _cts.Dispose();
            _commandLock.Dispose();
        }
    }
}