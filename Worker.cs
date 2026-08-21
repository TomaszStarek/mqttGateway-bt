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
     //   computerName = "plkwim0rpi250";
        string thingName = $"{computerName}-bt";
        //  string broker = "a36o17791e5o3h-ats.iot.eu-central-1.amazonaws.com";
        string broker = "aecb2nvsmjqlh-ats.iot.eu-central-1.amazonaws.com";

        // Ustalenie ścieżki bazowej aplikacji (kluczowe dla prawidłowego działania certyfikatów na produkcji!)
        string baseDir = AppContext.BaseDirectory;
        //string certPath = Path.Combine(baseDir, "certs/certificate.pem.crt");
        //string keyPath = Path.Combine(baseDir, "certs/private.pem.key");
        //string caPath = Path.Combine(baseDir, "certs/AmazonRootCA1.pem");
        string certPath = Path.Combine(baseDir, "certs", "1fa8573fd3c4a52a4a67ac68216f0312f72b617a06d3f02876c0a101b545c53b-certificate.pem.crt");
        string keyPath = Path.Combine(baseDir, "certs", "1fa8573fd3c4a52a4a67ac68216f0312f72b617a06d3f02876c0a101b545c53b-private.pem.key");
        string caPath = Path.Combine(baseDir, "certs", "AmazonRootCA1.pem");

        try
        {
            // Tworzymy instancję Twojej klasy Gateway
            _gateway = new Gateway(thingName, broker, certPath, keyPath, caPath, _loggerFactory);

            await _gateway.RunAsync(stoppingToken);

            _logger.LogInformation("The gateway is working properly. Waiting for close or reconfiguration signals.");

            // Usługa teraz "czuwa". Pętla kręci się dopóki Windows nie wyśle sygnału STOP (który anuluje stoppingToken)
            while (!stoppingToken.IsCancellationRequested)
            {
                // Delikatne uśpienie wątku monitorującego (np. co 1 sekundę)
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
            throw; // Rzucenie wyjątku przekaże błąd do Event Viewera i pozwoli systemowi zrestartować usługę
        }
        finally
        {
            // Gdy pętla zostanie przerwana, dzięki 'await using' lub ręcznemu DisposeAsync
            // wywołujemy Twój kod czyszczący połączenia MQTT i zatrzymujący DeviceWorkers.
            if (_gateway is not null)
            {
                _logger.LogInformation("Closing the gateway and releasing resources (Graceful Shutdown)...");
                await _gateway.DisposeAsync();
            }
        }

        _logger.LogInformation("The service has been safely stopped.");
    }
}