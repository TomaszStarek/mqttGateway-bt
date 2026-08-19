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
    internal sealed class Gateway : IAsyncDisposable
    {
        private const int UnboundedAngleHighDeg = 999;

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<Gateway> _logger;
        private readonly string _thingName;
        private readonly string _mqttBroker;
        private readonly string _certPath;
        private readonly string _keyPath;
        private readonly string _caPath;

        private IMqttClient? _mqtt;
        private readonly Dictionary<string, DeviceWorker> _workers = new();

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
                if (string.IsNullOrWhiteSpace(deviceCfg.Address))
                {
                    _logger.LogInformation($"[Config] Skipping device Id={deviceCfg.Id} — missing Address.");
                    continue;
                }

                if (_workers.TryGetValue(deviceCfg.DeviceId, out DeviceWorker? existing))
                {
                    _logger.LogInformation($"[Config] Replacing existing worker for {deviceCfg.DeviceId}…");
                    await existing.StopAsync();
                    existing.Dispose();
                    _workers.Remove(deviceCfg.DeviceId);
                }

                var workerLogger = _loggerFactory.CreateLogger<DeviceWorker>();
                var worker = new DeviceWorker(deviceCfg, _thingName, _mqtt!, workerLogger);
                _workers[deviceCfg.DeviceId] = worker;
                worker.Start();

                _logger.LogInformation(
                    $"[Config] Worker started — DeviceId={deviceCfg.DeviceId}, " +
                    $"Port={deviceCfg.Address}, Baud={deviceCfg.BaudRate}");
            }

            _logger.LogInformation($"[Config] Active workers: {_workers.Count}");
        }

        private async Task HandleCommandMessageAsync(string topic, string json)
        {
            string[] parts = topic.Split('/');
            if (parts.Length != 3)
            {
                _logger.LogCritical($"[CMD] Unexpected topic format: {topic}");
                return;
            }

            string address = parts[1];

            DeviceWorker? worker = _workers.Values.FirstOrDefault(w =>
                string.Equals(w.Address, address, StringComparison.OrdinalIgnoreCase));

            if (worker is null)
            {
                _logger.LogInformation($"[CMD] No active worker for address '{address}'.");
                return;
            }

            Command? cmd;
            try
            {
                cmd = JsonSerializer.Deserialize<Command>(json, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"[CMD] JSON parse error: {ex.Message}");
                return;
            }

            if (cmd is null) return;

            if (!string.IsNullOrEmpty(cmd.Type) && cmd.Type.Equals("raw", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.IsNullOrWhiteSpace(cmd.AtCommand))
                {
                    await worker.ExecuteRawAsync(cmd.AtCommand);
                }
                return;
            }

            float torqueLowNm = cmd.TargetTorqueNm * cmd.TorqueMinPercentage / 100f;
            float torqueHighNm = cmd.TargetTorqueNm * cmd.TorqueMaxPercentage / 100f;
            int angleLowDeg = cmd.MinAngleDeg;
            int angleHighDeg = UnboundedAngleHighDeg;
            int doubleDetectionAngleDeg = 0;
            float triggerNm = cmd.TargetTorqueNm * cmd.RotationStartThresholdPercentage / 100f;

            await worker.ConfigureJobAsync(
                torqueHighNm,
                torqueLowNm,
                angleHighDeg,
                angleLowDeg,
                doubleDetectionAngleDeg,
                triggerNm,
                cmd.StepId,
                cmd.BatchId,
                cmd.UserId);
        }

        public async ValueTask DisposeAsync()
        {
            _logger.LogInformation("Stopping gateway…");

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