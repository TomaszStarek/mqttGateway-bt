//using MQTTnet;
//using MQTTnet.Extensions.ManagedClient;
//using MQTTnet.Protocol;
//using NModbus;                  
//using System.Net.Sockets;
//using System.Security.Cryptography.X509Certificates;
//using System.Text;
//using System.Text.Json;


//namespace MqttModbusGateway;


//class Program
//{
//    static async Task Main()
//    {
//        var cts = new CancellationTokenSource();
//        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };


//        var computerName = System.Environment.MachineName;
//        string thingName = $"{computerName}-bt";

//        thingName = "PLKWIM0M25ST17-bt";
//        string broker = "a36o17791e5o3h-ats.iot.eu-central-1.amazonaws.com";

//        string certPath = "certs/certificate.pem.crt";
//        string keyPath = "certs/private.pem.key";
//        string caPath = "certs/AmazonRootCA1.pem";

//        await using var gateway = new Gateway(thingName, broker, certPath, keyPath, caPath);

//        await gateway.RunAsync(cts.Token);
//    }
//}
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqttModbusService;

IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options =>
    {
        // Ta nazwa identyfikuje usługę w przystawce services.msc
        options.ServiceName = "MqttModbusGatewayService";
    })
    .ConfigureLogging(logging =>
    {
        // Usuwamy domyslnych dostawcow logow, w tym Event Log. Na tym komputerze zapis do Event Log
        // konczy sie wyjatkiem (brak System.Threading.AccessControl) i wywala BackgroundService
        // juz przy pierwszym wpisie Information. Zostaje konsola (przy uruchomieniu .exe), Debug
        // i plik.
        logging.ClearProviders();
        logging.AddConsole();
        logging.AddDebug();

        // Dodatkowy log do pliku: C:\ProgramData\MqttModbusGateway\logs\gateway-yyyyMMdd.log
        // (czytany "na zywo" przez okno "Log na zywo" w MqttGatewayDeployer).
        var logDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
            "MqttModbusGateway", "logs");
        logging.AddProvider(new FileLoggerProvider(logDir));
    })
    .ConfigureServices(services =>
    {
        // Rejestrujemy klasę zarządzającą naszą bramką
        services.AddHostedService<ServiceWorker>();
    })
    .Build();

// Uruchomienie aplikacji. 
// Działa jako konsola u Ciebie na PC, działa jako usługa na serwerze korporacyjnym.
await host.RunAsync();