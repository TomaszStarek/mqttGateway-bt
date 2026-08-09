using Microsoft.Extensions.Logging;
using MQTTnet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MqttModbusGateway
{
    /// <summary>
    /// Gateway that bridges AWS IoT Core (MQTT) and one or more
    /// Tohnichi CEM3-G-BT torque wrenches connected via Bluetooth SPP
    /// (each wrench appears as a virtual COM port after OS pairing).
    ///
    /// Roles:
    /// <list type="bullet">
    ///   <item>Maintains a persistent, TLS-authenticated MQTT connection to AWS IoT Core
    ///         with automatic reconnect.</item>
    ///   <item>Reacts to a JSON configuration message on <c>{thingName}/config</c>
    ///         by spawning a <see cref="DeviceWorker"/> for every declared wrench.</item>
    ///   <item>Routes commands arriving on <c>{thingName}/{address}/commands</c>
    ///         to the <see cref="DeviceWorker"/> bound to that address (COM port).</item>
    /// </list>
    ///
    /// Configuration payload example (published to <c>{thingName}/config</c>):
    /// <code>
    /// {
    ///   "devices": [
    ///     { "deviceId": "LINE1-A", "comPort": "COM5",  "baudRate": 9600 },
    ///     { "deviceId": "LINE1-B", "comPort": "COM7",  "baudRate": 9600, "serialNumber": "7046200" },
    ///     { "deviceId": "LINE2-A", "comPort": "COM10", "baudRate": 9600 }
    ///   ]
    /// }
    /// </code>
    ///
    /// Command payload example (published to <c>{thingName}/{address}/commands</c>):
    /// <code>
    /// {
    ///   "type": "configureJob",
    ///   "targetTorqueNm": 10.0,
    ///   "torqueMinPercentage": 10,
    ///   "torqueMaxPercentage": 10,
    ///   "minAngleDeg": 15,
    ///   "rotationStartThresholdPercentage": 10,
    ///   "stepId": 1
    /// }
    /// </code>
    /// </summary>
    internal sealed class Gateway : IAsyncDisposable
    {
        /// <summary>
        /// Upper angle bound sent to the wrench when the caller doesn't want the
        /// upper angle limit enforced. The AT046 frame uses 3-digit fields, so this
        /// is the practical "don't check" maximum (999), not a literal 9999.
        /// </summary>
        private const int UnboundedAngleHighDeg = 999;

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<Gateway> _logger;
        private readonly string _thingName;
        private readonly string _mqttBroker;
        private readonly string _certPath;
        private readonly string _keyPath;
        private readonly string _caPath;

        private IMqttClient? _mqtt;

        /// <summary>Active device workers, keyed by DeviceId.</summary>
        private readonly Dictionary<string, DeviceWorker> _workers = new();

        /// <summary>
        /// Initialises a new <see cref="Gateway"/> instance.
        /// Call <see cref="RunAsync"/> to establish the MQTT connection and start processing.
        /// </summary>
        /// <param name="thingName">AWS IoT Thing name. Used as the MQTT client ID and topic root.</param>
        /// <param name="mqttBroker">AWS IoT Core endpoint hostname (port 8883 is always used).</param>
        /// <param name="certPath">Path to the device certificate PEM file.</param>
        /// <param name="keyPath">Path to the private key PEM file.</param>
        /// <param name="caPath">Path to the Amazon Root CA PEM file.</param>
        public Gateway(
            string thingName,
            string mqttBroker,
            string certPath,
            string keyPath,
            string caPath,
            ILoggerFactory loggerFactory)
        {
            _thingName = thingName;
            _mqttBroker = mqttBroker;
            _certPath = certPath;
            _keyPath = keyPath;
            _caPath = caPath;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<Gateway>();
        }

        // -----------------------------------------------------------------------
        // Public entry point
        // -----------------------------------------------------------------------

        /// <summary>
        /// Connects to AWS IoT Core, subscribes to required topics, then
        /// blocks until <paramref name="ct"/> is cancelled (e.g. Ctrl+C).
        /// </summary>
        public async Task RunAsync(CancellationToken ct)
        {
            await ConnectMqttAsync(ct);

            _logger.LogInformation("Gateway running. Waiting for config message on " +
                              $"'{_thingName}/config'. Press Ctrl+C to exit.");

            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Gateway shutting down…");
            }
        }

        // -----------------------------------------------------------------------
        // MQTT setup
        // -----------------------------------------------------------------------

        /// <summary>
        /// Creates the MQTT client, registers event handlers, connects to the broker,
        /// and subscribes to all required topics.
        /// </summary>
        private async Task ConnectMqttAsync(CancellationToken ct)
        {
            _mqtt = new MqttClientFactory().CreateMqttClient();

            _mqtt.ApplicationMessageReceivedAsync += OnMessageAsync;

            _mqtt.DisconnectedAsync += async _ =>
            {
                _logger.LogInformation("MQTT: disconnected from AWS IoT — reconnecting in 5 s…");
                await Task.Delay(5_000);

                try
                {
                    await _mqtt.ConnectAsync(_mqtt.Options, CancellationToken.None);
                    _logger.LogInformation("MQTT: reconnected ✓");
                }
                catch (Exception ex)
                {
                    _logger.LogCritical($"MQTT: reconnect failed — {ex.Message}");
                }
            };

            await TryMqttConnectAsync(ct);

            await _mqtt.SubscribeAsync($"{_thingName}/config");
            await _mqtt.SubscribeAsync($"{_thingName}/+/commands");

            _logger.LogInformation("MQTT: subscriptions active.");
        }

        /// <summary>
        /// Validates TLS credential files, builds MQTT options (mutual TLS 1.2),
        /// and performs the initial connection.
        /// </summary>
        private async Task TryMqttConnectAsync(CancellationToken ct)
        {
            if (!File.Exists(_certPath)) throw new FileNotFoundException($"Device cert not found: {_certPath}");
            if (!File.Exists(_keyPath)) throw new FileNotFoundException($"Private key not found: {_keyPath}");
            if (!File.Exists(_caPath)) throw new FileNotFoundException($"Root CA not found: {_caPath}");

            try
            {
                using var tempCert = X509Certificate2.CreateFromPemFile(_certPath, _keyPath);
                var clientCert = new X509Certificate2(tempCert.Export(X509ContentType.Pkcs12));
                var caCert = new X509Certificate2(_caPath);

                var options = new MqttClientOptionsBuilder()
                    .WithTcpServer(_mqttBroker, 8883)
                    .WithClientId(_thingName)
                    .WithTimeout(TimeSpan.FromSeconds(30))
                    .WithTlsOptions(o => o
                        .UseTls()
                        .WithSslProtocols(System.Security.Authentication.SslProtocols.Tls12)
                        .WithClientCertificates(new X509Certificate2Collection(clientCert))
                        .WithTrustChain(new X509Certificate2Collection(caCert))
                    .WithCertificateValidationHandler(ctx =>
                    {
                        var cert2 = new X509Certificate2(ctx.Certificate);
                        ctx.Chain.ChainPolicy.ExtraStore.Add(caCert);
                        ctx.Chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                        ctx.Chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                        return ctx.Chain.Build(cert2);
                    }))
                    .Build();

                _logger.LogInformation($"MQTT: connecting to {_mqttBroker}…");
                await _mqtt!.ConnectAsync(options, ct);
                _logger.LogInformation("MQTT: connected");
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"MQTT: connection error — {ex.Message}");
                throw;
            }
        }

        // -----------------------------------------------------------------------
        // MQTT message routing
        // -----------------------------------------------------------------------

        /// <summary>
        /// Routes inbound MQTT messages to the appropriate handler.
        ///
        /// Supported topics:
        /// <list type="bullet">
        ///   <item><c>{thingName}/config</c>              → <see cref="HandleConfigMessageAsync"/></item>
        ///   <item><c>{thingName}/{address}/commands</c>  → <see cref="HandleCommandMessageAsync"/></item>
        /// </list>
        /// </summary>
        private async Task OnMessageAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            try
            {
                string topic = args.ApplicationMessage.Topic;
                string payload = args.ApplicationMessage.ConvertPayloadToString();

                _logger.LogInformation(new string('-', 50));
                _logger.LogInformation($"[MQTT IN] {DateTime.Now:HH:mm:ss}  {topic}");
                _logger.LogInformation($"Payload : {payload}");
                _logger.LogInformation(new string('-', 50));

                if (topic == $"{_thingName}/config")
                {
                    await HandleConfigMessageAsync(payload);
                    return;
                }

                if (topic.StartsWith(_thingName) && topic.EndsWith("/commands"))
                {
                    await HandleCommandMessageAsync(topic, payload);
                    return;
                }

                _logger.LogInformation($"[MQTT] Unhandled topic: {topic}");
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"[MQTT] Message handler error: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // Config handler
        // -----------------------------------------------------------------------

        /// <summary>
        /// Processes a configuration payload.
        ///
        /// For each device declared in the config:
        /// <list type="number">
        ///   <item>If a worker already exists for that <c>DeviceId</c>, stop and remove it.</item>
        ///   <item>Create a new <see cref="DeviceWorker"/> and start it.</item>
        /// </list>
        ///
        /// Workers for devices that are no longer in the config are left running
        /// (send an updated config without them to remove, or restart the gateway).
        /// </summary>
        private async Task HandleConfigMessageAsync(string json)
        {
            ConfigRoot? config;

            try
            {
                config = JsonSerializer.Deserialize<ConfigRoot>(json,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"[Config] JSON parse error: {ex.Message}");
                return;
            }

            if (config is null || config.Devices.Count == 0)
            {
                _logger.LogInformation("[Config] Received empty or invalid configuration — ignoring.");
                return;
            }

            if (!config.IsActive)
            {
                _logger.LogInformation("[Config] isActive=false — stopping all device workers.");

                foreach (var key in _workers.Keys.ToList())
                {
                    await _workers[key].StopAsync();
                    _workers[key].Dispose();
                    _workers.Remove(key);
                    _logger.LogInformation($"[Config] Stopped worker for {key}");
                }

                return;
            }

            foreach (DeviceConfig deviceCfg in config.Devices)
            {
                // IpAddress pełni rolę COM portu
                if (string.IsNullOrWhiteSpace(deviceCfg.Address))
                {
                    _logger.LogInformation($"[Config] Skipping device Id={deviceCfg.Id} — missing IpAddress/ComPort.");
                    continue;
                }

                if (_workers.TryGetValue(deviceCfg.DeviceId, out DeviceWorker? existing))
                {
                    _logger.LogInformation($"[Config] Replacing existing worker for {deviceCfg.DeviceId}…");
                    await existing.StopAsync();
                    existing.Dispose();
                    _workers.Remove(deviceCfg.DeviceId);
                }

                var workerLogger = _loggerFactory.CreateLogger<DeviceWorker>(); // logger for new worker
                var worker = new DeviceWorker(deviceCfg, _thingName, _mqtt!, workerLogger);
                _workers[deviceCfg.DeviceId] = worker;
                worker.Start();

                _logger.LogInformation(
                    $"[Config] Worker started — DeviceId={deviceCfg.DeviceId}, " +
                    $"Port={deviceCfg.Address}, Baud={deviceCfg.BaudRate}");
            }

            _logger.LogInformation($"[Config] Active workers: {_workers.Count}");
        }

        // -----------------------------------------------------------------------
        // Command handler
        // -----------------------------------------------------------------------

        /// <summary>
        /// Forwards a command from <c>{thingName}/{address}/commands</c> to the worker
        /// bound to that address (COM port).
        ///
        /// Supported command types:
        /// <list type="bullet">
        ///   <item><c>"raw"</c> — sends <c>atCommand</c> verbatim, e.g. <c>{ "type": "raw", "atCommand": "AT037,45.00,30.00" }</c>.</item>
        ///   <item><c>"configureJob"</c> — see <see cref="Command"/> for the full payload shape.</item>
        /// </list>
        /// </summary>
        private async Task HandleCommandMessageAsync(
            string topic,
            string json)
        {
            string[] parts = topic.Split('/');

            if (parts.Length != 3)
            {
                _logger.LogCritical(
                    $"[CMD] Unexpected topic format: {topic}");

                return;
            }

            // parts[1] is the wrench address (COM port), not the DeviceId —
            // matches the {address}/data and {address}/state topics DeviceWorker publishes on.
            string address = parts[1];

            DeviceWorker? worker = _workers.Values.FirstOrDefault(w =>
                string.Equals(w.Address, address, StringComparison.OrdinalIgnoreCase));

            if (worker is null)
            {
                _logger.LogInformation(
                    $"[CMD] No active worker for address '{address}'.");

                return;
            }

            Command? cmd;

            try
            {
                cmd = JsonSerializer.Deserialize<Command>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });
            }
            catch (Exception ex)
            {
                _logger.LogCritical(
                    $"[CMD] JSON parse error: {ex.Message}");

                return;
            }

            if (cmd is null)
            {
                _logger.LogCritical(
                    "[CMD] Null command");

                return;
            }

            _logger.LogInformation(
                $"[CMD] Type = {cmd.Type}");

            switch (cmd.Type?.ToLowerInvariant())
            {
                // ----------------------------------------
                // CONFIGURE JOB
                // ----------------------------------------

                case "configurejob":

                    // Absolute torque limits are % of the target torque (from the configurator).
                    float torqueLowNm = cmd.TargetTorqueNm * cmd.TorqueMinPercentage / 100f;
                    float torqueHighNm = cmd.TargetTorqueNm * cmd.TorqueMaxPercentage / 100f;

                    // Angle upper limit is not enforced — send the protocol's practical max.
                    int angleLowDeg = cmd.MinAngleDeg;
                    int angleHighDeg = UnboundedAngleHighDeg;

                    // Double-detection angle is not used in this deployment.
                    int doubleDetectionAngleDeg = 0;

                    // Torque at which angle measurement starts, also a % of target torque.
                    float triggerNm = cmd.TargetTorqueNm * cmd.RotationStartThresholdPercentage / 100f;

                    await worker.ConfigureJobAsync(
                        torqueHighNm,
                        torqueLowNm,
                        angleHighDeg,
                        angleLowDeg,
                        doubleDetectionAngleDeg,
                        triggerNm,
                        cmd.StepId);

                    break;

                // ----------------------------------------
                // RAW AT
                // ----------------------------------------

                case "raw":

                    if (string.IsNullOrWhiteSpace(cmd.AtCommand))
                    {
                        _logger.LogInformation(
                            "[CMD] Raw command missing AtCommand");

                        return;
                    }

                    await worker.ExecuteRawAsync(
                        cmd.AtCommand);

                    break;

                // ----------------------------------------
                // UNKNOWN
                // ----------------------------------------

                default:

                    _logger.LogInformation(
                        $"[CMD] Unknown command type: {cmd.Type}");

                    break;
            }
        }

        // -----------------------------------------------------------------------
        // IAsyncDisposable
        // -----------------------------------------------------------------------

        /// <summary>
        /// Stops all workers and tears down the MQTT connection.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            _logger.LogInformation("Stopping gateway…");

            // Stop all workers in parallel for faster shutdown.
            await Task.WhenAll(_workers.Values.Select(async w =>
            {
                await w.StopAsync();
                w.Dispose();
            }));

            _workers.Clear();

            if (_mqtt is not null)
            {
                if (_mqtt.IsConnected)
                    await _mqtt.DisconnectAsync();

                _mqtt.Dispose();
            }

            _logger.LogInformation("Gateway stopped.");
        }
    }
}