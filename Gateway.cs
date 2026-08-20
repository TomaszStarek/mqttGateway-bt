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
        private readonly SemaphoreSlim _reconnectLock = new(1, 1);
        private readonly CancellationTokenSource _cts = new();

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

            _logger.LogInformation($"Gateway running. Waiting for config on '{_thingName}/config'.");

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

            _mqtt.DisconnectedAsync += e =>
            {
                _logger.LogWarning($"MQTT: Disconnected ({e.Reason}). Retrying in background...");
                _ = Task.Run(() => ReconnectLoopAsync(_cts.Token));
                return Task.CompletedTask;
            };

            await TryMqttConnectAsync(ct);
            await SubscribeTopicsAsync();
        }

        private async Task SubscribeTopicsAsync()
        {
            if (_mqtt is null || !_mqtt.IsConnected) return;

            await _mqtt.SubscribeAsync($"{_thingName}/config");
            await _mqtt.SubscribeAsync($"{_thingName}/+/commands");
            _logger.LogInformation("MQTT: Subscriptions updated.");
        }

        private async Task ReconnectLoopAsync(CancellationToken ct)
        {
            if (!await _reconnectLock.WaitAsync(0)) return;

            try
            {
                while (!ct.IsCancellationRequested && (_mqtt == null || !_mqtt.IsConnected))
                {
                    await Task.Delay(5_000, ct);

                    try
                    {
                        _logger.LogInformation("MQTT: Reconnecting to AWS IoT...");
                        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

                        await _mqtt!.ConnectAsync(_mqtt.Options, linkedCts.Token);
                        await SubscribeTopicsAsync();

                        _logger.LogInformation("MQTT: Reconnected successfully ✓");
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"MQTT: Reconnect failed ({ex.Message}) — retrying in 5s...");
                    }
                }
            }
            finally
            {
                _reconnectLock.Release();
            }
        }

        private async Task TryMqttConnectAsync(CancellationToken ct)
        {
            if (!File.Exists(_certPath)) throw new FileNotFoundException($"Device cert not found: {_certPath}");
            if (!File.Exists(_keyPath)) throw new FileNotFoundException($"Private key not found: {_keyPath}");
            if (!File.Exists(_caPath)) throw new FileNotFoundException($"Root CA not found: {_caPath}");

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

            _logger.LogInformation($"MQTT: Connecting to {_mqttBroker}…");
            await _mqtt!.ConnectAsync(options, ct);
            _logger.LogInformation("MQTT: Connected");
        }

        private async Task OnMessageAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            string topic = args.ApplicationMessage.Topic;
            string payload = args.ApplicationMessage.ConvertPayloadToString();

            _logger.LogInformation($"[MQTT IN] {topic} -> {payload}");

            try
            {
                if (topic == $"{_thingName}/config")
                {
                    await HandleConfigMessageAsync(payload);
                }
                else if (topic.StartsWith(_thingName) && topic.EndsWith("/commands"))
                {
                    await HandleCommandMessageAsync(topic, payload);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"[MQTT] Error processing topic '{topic}': {ex.Message}");
            }
        }

        private async Task HandleConfigMessageAsync(string json)
        {
            ConfigRoot? config = null;
            try
            {
                config = JsonSerializer.Deserialize<ConfigRoot>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch (JsonException ex)
            {
                _logger.LogWarning($"[Config] Invalid JSON payload: {ex.Message}");
                return;
            }

            if (config is null || !config.IsActive || config.Devices.Count == 0)
            {
                _logger.LogInformation("[Config] Empty or inactive configuration — stopping all workers.");

                foreach (var key in _workers.Keys.ToList())
                {
                    await StopAndRemoveWorkerAsync(key);
                }
                return;
            }

            var newDeviceIds = config.Devices.Select(d => d.DeviceId).ToHashSet();

            // Usunięcie wycofanych wkrętaków
            foreach (var key in _workers.Keys.ToList())
            {
                if (!newDeviceIds.Contains(key))
                {
                    await StopAndRemoveWorkerAsync(key);
                }
            }

            // Start / podmiana wkrętaków z nowej konfiguracji
            foreach (DeviceConfig deviceCfg in config.Devices)
            {
                if (string.IsNullOrWhiteSpace(deviceCfg.Address)) continue;

                if (_workers.ContainsKey(deviceCfg.DeviceId))
                {
                    await StopAndRemoveWorkerAsync(deviceCfg.DeviceId);
                }

                var workerLogger = _loggerFactory.CreateLogger<DeviceWorker>();
                var worker = new DeviceWorker(deviceCfg, _thingName, _mqtt!, workerLogger);
                _workers[deviceCfg.DeviceId] = worker;
                worker.Start();

                _logger.LogInformation($"[Config] Worker started: {deviceCfg.DeviceId} ({deviceCfg.Address})");
            }
        }

        private async Task StopAndRemoveWorkerAsync(string deviceId)
        {
            if (_workers.TryGetValue(deviceId, out var worker))
            {
                await worker.StopAsync();
                worker.Dispose();
                _workers.Remove(deviceId);
                _logger.LogInformation($"[Config] Stopped worker: {deviceId}");
            }
        }

        private async Task HandleCommandMessageAsync(string topic, string json)
        {
            string[] parts = topic.Split('/');
            if (parts.Length != 3) return;

            string address = parts[1];

            DeviceWorker? worker = _workers.Values.FirstOrDefault(w =>
                string.Equals(w.Address, address, StringComparison.OrdinalIgnoreCase));

            if (worker is null)
            {
                _logger.LogWarning($"[CMD] No worker found for address '{address}'.");
                return;
            }

            Command? cmd = null;
            try
            {
                cmd = JsonSerializer.Deserialize<Command>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch (JsonException ex)
            {
                _logger.LogWarning($"[CMD] Invalid command JSON: {ex.Message}");
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

            float torqueLowNm = cmd.TargetTorqueNm * 90 / 100f;
            float torqueHighNm = cmd.TargetTorqueNm * 120 / 100f;
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
                cmd.UserId,
                cmd.DeviceId);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _logger.LogInformation("Stopping gateway…");

            foreach (var worker in _workers.Values)
            {
                await worker.StopAsync();
                worker.Dispose();
            }
            _workers.Clear();

            if (_mqtt is not null)
            {
                if (_mqtt.IsConnected)
                    await _mqtt.DisconnectAsync();

                _mqtt.Dispose();
            }

            _cts.Dispose();
            _reconnectLock.Dispose();
            _logger.LogInformation("Gateway stopped.");
        }
    }
}