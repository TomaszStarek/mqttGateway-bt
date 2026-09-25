using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqttModbusGateway;

namespace MqttModbusService;

public class ServiceWorker : BackgroundService
{
    private readonly ILogger<ServiceWorker> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private Gateway? _gateway;

    public ServiceWorker(ILogger<ServiceWorker> logger, ILoggerFactory loggerFactory)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting the MQTT - Modbus gateway service... new ver");

        var computerName = Environment.MachineName;
        var thingName = $"{computerName}-bt";

        // Zmień tylko tę wartość: "dev", "stg" albo "prd".
        const string selectedEnvironment = "stg";

        var (broker, certificateFile, privateKeyFile, caFile) =
            selectedEnvironment switch
            {
                "dev" =>
                (
                    "a36o17791e5o3h-ats.iot.eu-central-1.amazonaws.com",
                    "certificate.pem.crt",
                    "private.pem.key",
                    "AmazonRootCA1.pem"
                ),

                "stg" =>
                (
                    "aecb2nvsmjqlh-ats.iot.eu-central-1.amazonaws.com",
                    "1fa8573fd3c4a52a4a67ac68216f0312f72b617a06d3f02876c0a101b545c53b-certificate.pem.crt",
                    "1fa8573fd3c4a52a4a67ac68216f0312f72b617a06d3f02876c0a101b545c53b-private.pem.key",
                    "AmazonRootCA1.pem"
                ),

                "prd" =>
                (
                    "alkaawcdk2yqx-ats.iot.eu-central-1.amazonaws.com",
                    "991249704daaaea6a53af320a7fed35494e052766f7892a88c195cbf22a9ad18-certificate.pem.crt",
                    "991249704daaaea6a53af320a7fed35494e052766f7892a88c195cbf22a9ad18-private.pem.key",
                    "AmazonRootCA1.pem"
                ),

                _ => throw new InvalidOperationException(
                    $"Nieznane środowisko: '{selectedEnvironment}'. Dozwolone wartości: dev, stg, prd.")
            };

        var baseDir = AppContext.BaseDirectory;

        // Ścieżka będzie np. certs\prd, certs\stg albo certs\dev.
        var certDir = Path.Combine(
            baseDir,
            "certs",
            selectedEnvironment);

        var certPath = Path.Combine(certDir, certificateFile);
        var keyPath = Path.Combine(certDir, privateKeyFile);
        var caPath = Path.Combine(certDir, caFile);

        _logger.LogInformation(
            "Wybrane środowisko: {Environment}, broker: {Broker}",
            selectedEnvironment,
            broker);

        try
        {
            if (!File.Exists(certPath))
            {
                throw new FileNotFoundException(
                    $"Nie znaleziono certyfikatu. Oczekiwana ścieżka: {certPath}");
            }

            if (!File.Exists(keyPath))
            {
                throw new FileNotFoundException(
                    $"Nie znaleziono klucza prywatnego. Oczekiwana ścieżka: {keyPath}");
            }

            if (!File.Exists(caPath))
            {
                throw new FileNotFoundException(
                    $"Nie znaleziono certyfikatu CA. Oczekiwana ścieżka: {caPath}");
            }

            _gateway = new Gateway(thingName, broker, certPath, keyPath, caPath, _loggerFactory);

            await _gateway.RunAsync(stoppingToken);

            _logger.LogInformation("The gateway is working properly. Waiting for close or reconfiguration signals.");

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("The service received a system stop signal.");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Critical error while running the service!");
            throw;
        }
        finally
        {
            if (_gateway is not null)
            {
                _logger.LogInformation("Closing the gateway and releasing resources (Graceful Shutdown)...");
                await _gateway.DisposeAsync();
            }
        }

        _logger.LogInformation("The service has been safely stopped.");
    }
}