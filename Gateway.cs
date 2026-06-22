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
    ///   <item>Routes AT write-commands arriving on <c>{thingName}/{deviceId}/commands</c>
    ///         to the appropriate <see cref="DeviceWorker"/>.</item>
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
    /// </summary>
    internal sealed class Gateway : IAsyncDisposable
    {
        // -----------------------------------------------------------------------
        // Fields
        // -----------------------------------------------------------------------

        private readonly string _thingName;
        private readonly string _mqttBroker;
        private readonly string _certPath;
        private readonly string _keyPath;
        private readonly string _caPath;

        private IMqttClient? _mqtt;

        /// <summary>Active device workers, keyed by DeviceId.</summary>
        private readonly Dictionary<string, DeviceWorker> _workers = new();

        // -----------------------------------------------------------------------
        // Constructor
        // -----------------------------------------------------------------------

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
            string caPath)
        {
            _thingName  = thingName;
            _mqttBroker = mqttBroker;
            _certPath   = certPath;
            _keyPath    = keyPath;
            _caPath     = caPath;
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

            Console.WriteLine("Gateway running. Waiting for config message on " +
                              $"'{_thingName}/config'. Press Ctrl+C to exit.");

            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Gateway shutting down…");
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
                Console.WriteLine("MQTT: disconnected from AWS IoT — reconnecting in 5 s…");
                await Task.Delay(5_000);

                try
                {
                    await _mqtt.ConnectAsync(_mqtt.Options, CancellationToken.None);
                    Console.WriteLine("MQTT: reconnected ✓");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"MQTT: reconnect failed — {ex.Message}");
                }
            };

            await TryMqttConnectAsync(ct);

            // Subscribe to:
            //   {thingName}/config          — new wrench configuration
            //   {thingName}/+/commands      — write commands for individual wrenches
            await _mqtt.SubscribeAsync($"{_thingName}/config");
            await _mqtt.SubscribeAsync($"{_thingName}/+/commands");

            Console.WriteLine("MQTT: subscriptions active.");
        }

        /// <summary>
        /// Validates TLS credential files, builds MQTT options (mutual TLS 1.2),
        /// and performs the initial connection.
        /// </summary>
        private async Task TryMqttConnectAsync(CancellationToken ct)
        {
            if (!File.Exists(_certPath)) throw new FileNotFoundException($"Device cert not found: {_certPath}");
            if (!File.Exists(_keyPath))  throw new FileNotFoundException($"Private key not found: {_keyPath}");
            if (!File.Exists(_caPath))   throw new FileNotFoundException($"Root CA not found: {_caPath}");

            try
            {
                using var tempCert  = X509Certificate2.CreateFromPemFile(_certPath, _keyPath);
                var       clientCert = new X509Certificate2(tempCert.Export(X509ContentType.Pkcs12));
                var       caCert     = new X509Certificate2(_caPath);

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

                Console.WriteLine($"MQTT: connecting to {_mqttBroker}…");
                await _mqtt!.ConnectAsync(options, ct);
                Console.WriteLine("MQTT: connected");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"MQTT: connection error — {ex.Message}");
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
        ///   <item><c>{thingName}/config</c>               → <see cref="HandleConfigMessageAsync"/></item>
        ///   <item><c>{thingName}/{deviceId}/commands</c>  → <see cref="HandleCommandMessageAsync"/></item>
        /// </list>
        /// </summary>
        private async Task OnMessageAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            try
            {
                string topic   = args.ApplicationMessage.Topic;
                string payload = args.ApplicationMessage.ConvertPayloadToString();

                Console.WriteLine(new string('-', 50));
                Console.WriteLine($"[MQTT IN] {DateTime.Now:HH:mm:ss}  {topic}");
                Console.WriteLine($"Payload : {payload}");
                Console.WriteLine(new string('-', 50));

                if (topic == $"{_thingName}/config")
                {
                //    payload = """
                //{
                //  "devices": [
                //    {
                //      "deviceId": "LINE1",
                //      "comPort": "COM11"
                //    }

                //  ]
                //}
                //""";

                    await HandleConfigMessageAsync(payload);
                    return;
                }

                if (topic.StartsWith(_thingName) && topic.EndsWith("/commands"))
                {
                    await HandleCommandMessageAsync(topic, payload);
                    return;
                }

                Console.WriteLine($"[MQTT] Unhandled topic: {topic}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MQTT] Message handler error: {ex.Message}");
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
                Console.WriteLine($"[Config] JSON parse error: {ex.Message}");
                return;
            }

            if (config is null || config.Devices.Count == 0)
            {
                Console.WriteLine("[Config] Received empty or invalid configuration — ignoring.");
                return;
            }

            if (!config.IsActive)
            {
                Console.WriteLine("[Config] isActive=false — stopping all device workers.");

                foreach (var key in _workers.Keys.ToList())
                {
                    await _workers[key].StopAsync();
                    _workers[key].Dispose();
                    _workers.Remove(key);
                    Console.WriteLine($"[Config] Stopped worker for {key}");
                }

                return;
            }

            foreach (DeviceConfig deviceCfg in config.Devices)
            {
                // IpAddress pełni rolę COM portu
                if (string.IsNullOrWhiteSpace(deviceCfg.Address))
                {
                    Console.WriteLine($"[Config] Skipping device Id={deviceCfg.Id} — missing IpAddress/ComPort.");
                    continue;
                }

                if (_workers.TryGetValue(deviceCfg.DeviceId, out DeviceWorker? existing))
                {
                    Console.WriteLine($"[Config] Replacing existing worker for {deviceCfg.DeviceId}…");
                    await existing.StopAsync();
                    existing.Dispose();
                    _workers.Remove(deviceCfg.DeviceId);
                }

                var worker = new DeviceWorker(deviceCfg, _thingName, _mqtt!);
                _workers[deviceCfg.DeviceId] = worker;
                worker.Start();

                Console.WriteLine(
                    $"[Config] Worker started — DeviceId={deviceCfg.DeviceId}, " +
                    $"Port={deviceCfg.Address}, Baud={deviceCfg.BaudRate}");
            }

            Console.WriteLine($"[Config] Active workers: {_workers.Count}");
        }

        // -----------------------------------------------------------------------
        // Command handler
        // -----------------------------------------------------------------------

        /// <summary>
        /// Forwards an AT command from the MQTT commands topic to the target wrench worker.
        ///
        /// Expected topic format: <c>{thingName}/{deviceId}/commands</c>
        ///
        /// Payload example:
        /// <code>
        /// { "atCommand": "AT037,45.00,30.00" }
        /// </code>
        /// </summary>
        private async Task HandleCommandMessageAsync(
            string topic,
            string json)
        {
            string[] parts = topic.Split('/');

            if (parts.Length != 3)
            {
                Console.WriteLine(
                    $"[CMD] Unexpected topic format: {topic}");

                return;
            }

            string deviceId = parts[1];

            if (!_workers.TryGetValue(
                    deviceId,
                    out DeviceWorker? worker))
            {
                Console.WriteLine(
                    $"[CMD] No active worker for device '{deviceId}'.");

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
                Console.WriteLine(
                    $"[CMD] JSON parse error: {ex.Message}");

                return;
            }

            if (cmd is null)
            {
                Console.WriteLine(
                    "[CMD] Null command");

                return;
            }

            Console.WriteLine(
                $"[CMD] Type = {cmd.Type}");

            switch (cmd.Type?.ToLowerInvariant())
            {
                // ----------------------------------------
                // CONFIGURE JOB
                // ----------------------------------------

                case "configurejob":

                    await worker.ConfigureJobAsync(
                        cmd.TorqueHighNm,
                        cmd.TorqueLowNm,
                        cmd.AngleHighDeg,
                        cmd.AngleLowDeg,
                        cmd.DoubleDetectionAngleDeg,
                        cmd.TriggerNm);

                    break;

                // ----------------------------------------
                // RAW AT
                // ----------------------------------------

                case "raw":

                    if (string.IsNullOrWhiteSpace(cmd.AtCommand))
                    {
                        Console.WriteLine(
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

                    Console.WriteLine(
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
            Console.WriteLine("Stopping gateway…");

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

            Console.WriteLine("Gateway stopped.");
        }
    }
}
