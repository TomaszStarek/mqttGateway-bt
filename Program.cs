using MQTTnet;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Protocol;
using NModbus;                  
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;


namespace MqttModbusGateway;


class Program
{
    static async Task Main()
    {
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };


        var computerName = System.Environment.MachineName;
        string thingName = $"{computerName}-bt";

        thingName = "PLKWIM0M25ST17-bt";
        string broker = "a36o17791e5o3h-ats.iot.eu-central-1.amazonaws.com";

        string certPath = "certs/certificate.pem.crt";
        string keyPath = "certs/private.pem.key";
        string caPath = "certs/AmazonRootCA1.pem";

        await using var gateway = new Gateway(thingName, broker, certPath, keyPath, caPath);

        await gateway.RunAsync(cts.Token);
    }
}