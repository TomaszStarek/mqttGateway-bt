using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MqttModbusGateway
{
    /// <summary>
    /// Automatyczne PAROWANIE kluczy, ktore Windows juz "widzi", ale nie sa sparowane
    /// (np. po zmianie portu COM / usunieciu i ponownym dodaniu urzadzenia - wtedy Windows
    /// pokazuje powiadomienie "Add a device" i do jego klikniecia klucz sie nie laczy).
    ///
    /// Zasady bezpieczenstwa:
    ///  * paruje TYLKO urzadzenia, ktorych nazwa pasuje do jednego z wyrazen regularnych z
    ///    appsettings.json (BluetoothAutoPair:NamePatterns) - zadne inne urzadzenie nie jest ruszane;
    ///  * dziala tylko gdy jest wlaczone (BluetoothAutoPair:Enabled = true; domyslnie WYLACZONE);
    ///  * skanuje tylko gdy co najmniej jeden skonfigurowany klucz jest rozlaczony - przy
    ///    zdrowych polaczeniach nie robi nic;
    ///  * domyslnie NIE wykonuje aktywnego skanowania radiowego (IssueInquiry=false) - uzywa listy
    ///    urzadzen, ktore Windows juz zna, wiec nie zaklóca trwajacych polaczen SPP;
    ///  * ten sam adres jest ponawiany najwyzej raz na minute.
    ///
    /// Samo parowanie zatwierdza BluetoothAutoAcceptor (callback autoryzacji w tej samej usludze).
    /// Po udanym parowaniu wlaczana jest usluga SPP (tylko ENABLE, bez wylaczania - zeby nie
    /// przestawiac numerow COM tak jak robil to dawny "kicker").
    /// Numer COM dla swiezo sparowanego klucza przydziela Windows - ten kod go nie zmienia.
    /// </summary>
    internal sealed class BluetoothAutoPairer : IDisposable
    {
        private const uint ERROR_SUCCESS = 0;
        private const uint BLUETOOTH_SERVICE_ENABLE = 0x01;
        private const int MitmNotRequiredBonding = 2; // BLUETOOTH_MITM_ProtectionNotRequiredBonding
        private static readonly Guid SerialPortServiceClass = new("00001101-0000-1000-8000-00805F9B34FB");
        private static readonly TimeSpan RetryCooldown = TimeSpan.FromSeconds(60);

        [StructLayout(LayoutKind.Sequential)]
        private struct SYSTEMTIME
        {
            public ushort wYear, wMonth, wDayOfWeek, wDay, wHour, wMinute, wSecond, wMilliseconds;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct BLUETOOTH_DEVICE_INFO
        {
            public int dwSize;
            public long Address;
            public uint ulClassofDevice;
            public int fConnected;
            public int fRemembered;
            public int fAuthenticated;
            public SYSTEMTIME stLastSeen;
            public SYSTEMTIME stLastUsed;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 248)]
            public string szName;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct BLUETOOTH_DEVICE_SEARCH_PARAMS
        {
            public int dwSize;
            public int fReturnAuthenticated;
            public int fReturnRemembered;
            public int fReturnUnknown;
            public int fReturnConnected;
            public int fIssueInquiry;
            public byte cTimeoutMultiplier;
            public IntPtr hRadio;
        }

        [DllImport("bthprops.cpl", SetLastError = true)]
        private static extern IntPtr BluetoothFindFirstDevice(ref BLUETOOTH_DEVICE_SEARCH_PARAMS pbtsp, ref BLUETOOTH_DEVICE_INFO pbtdi);

        [DllImport("bthprops.cpl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool BluetoothFindNextDevice(IntPtr hFind, ref BLUETOOTH_DEVICE_INFO pbtdi);

        [DllImport("bthprops.cpl")]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool BluetoothFindDeviceClose(IntPtr hFind);

        [DllImport("bthprops.cpl", SetLastError = true)]
        private static extern uint BluetoothAuthenticateDeviceEx(
            IntPtr hwndParentIn, IntPtr hRadioIn, ref BLUETOOTH_DEVICE_INFO pbtdiInout,
            IntPtr pbtOobData, int authenticationRequirement);

        [DllImport("bthprops.cpl", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern uint BluetoothAuthenticateDevice(
            IntPtr hwndParent, IntPtr hRadio, ref BLUETOOTH_DEVICE_INFO pbtbi, string pszPasskey, uint ulPasskeyLength);

        [DllImport("bthprops.cpl", SetLastError = true)]
        private static extern uint BluetoothSetServiceState(
            IntPtr hRadio, ref BLUETOOTH_DEVICE_INFO pbtdi, ref Guid pGuidService, uint dwServiceFlags);

        private readonly ILogger _logger;
        private readonly Func<bool> _shouldScan;
        private readonly bool _enabled;
        private readonly bool _issueInquiry;
        private readonly int _intervalSec;
        private readonly List<Regex> _patterns = new();
        private readonly Dictionary<long, DateTime> _lastAttempt = new();
        private readonly CancellationTokenSource _cts = new();

        public BluetoothAutoPairer(ILogger logger, IConfiguration config, Func<bool> shouldScan)
        {
            _logger = logger;
            _shouldScan = shouldScan;

            var sec = config.GetSection("BluetoothAutoPair");
            _enabled = sec.GetValue<bool>("Enabled", false);
            _issueInquiry = sec.GetValue<bool>("IssueInquiry", false);
            _intervalSec = Math.Max(5, sec.GetValue<int>("ScanIntervalSeconds", 20));

            foreach (var pattern in sec.GetSection("NamePatterns").Get<string[]>() ?? Array.Empty<string>())
            {
                try { _patterns.Add(new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds(200))); }
                catch (ArgumentException ex) { _logger.LogWarning("[BT-Pair] Niepoprawne wyrazenie '{Pattern}' w NamePatterns: {Msg}", pattern, ex.Message); }
            }
        }

        public void Start(CancellationToken stoppingToken)
        {
            if (!_enabled)
            {
                _logger.LogInformation("[BT-Pair] Automatyczne parowanie kluczy jest WYLACZONE (BluetoothAutoPair:Enabled=false).");
                return;
            }

            if (_patterns.Count == 0)
            {
                _logger.LogWarning("[BT-Pair] Wlaczone, ale NamePatterns jest puste - nie paruje nic (zabezpieczenie przed parowaniem dowolnych urzadzen).");
                return;
            }

            _logger.LogInformation(
                "[BT-Pair] Automatyczne parowanie WLACZONE. Wzorce nazw: {Patterns}; skan co {Sec} s (tylko gdy jakis klucz jest rozlaczony); aktywny inquiry: {Inq}.",
                string.Join(", ", _patterns.Select(p => p.ToString())), _intervalSec, _issueInquiry);

            var token = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, stoppingToken).Token;
            _ = Task.Run(() => LoopAsync(token));
        }

        private async Task LoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(TimeSpan.FromSeconds(_intervalSec), ct); }
                catch (OperationCanceledException) { break; }

                try
                {
                    if (!_shouldScan()) continue;

                    foreach (var dev in Scan())
                    {
                        if (ct.IsCancellationRequested) break;
                        if (dev.fAuthenticated != 0) continue; // juz sparowane

                        var name = (dev.szName ?? "").Trim();
                        if (!_patterns.Any(r => SafeMatch(r, name))) continue;

                        long mac = dev.Address & 0xFFFFFFFFFFFF;
                        if (_lastAttempt.TryGetValue(mac, out var last) && DateTime.UtcNow - last < RetryCooldown) continue;
                        _lastAttempt[mac] = DateTime.UtcNow;

                        TryPair(dev, name);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("[BT-Pair] Blad petli parowania: {Msg}", ex.Message);
                }
            }
        }

        private static bool SafeMatch(Regex r, string input)
        {
            try { return r.IsMatch(input); } catch (RegexMatchTimeoutException) { return false; }
        }

        private List<BLUETOOTH_DEVICE_INFO> Scan()
        {
            var found = new List<BLUETOOTH_DEVICE_INFO>();

            var search = new BLUETOOTH_DEVICE_SEARCH_PARAMS
            {
                dwSize = Marshal.SizeOf<BLUETOOTH_DEVICE_SEARCH_PARAMS>(),
                fReturnAuthenticated = 1,
                fReturnRemembered = 1,
                fReturnUnknown = 1,
                fReturnConnected = 1,
                fIssueInquiry = _issueInquiry ? 1 : 0,
                cTimeoutMultiplier = 2, // x1.28 s
                hRadio = IntPtr.Zero,
            };

            var info = new BLUETOOTH_DEVICE_INFO { dwSize = Marshal.SizeOf<BLUETOOTH_DEVICE_INFO>() };

            IntPtr handle = BluetoothFindFirstDevice(ref search, ref info);
            if (handle == IntPtr.Zero) return found; // brak radia / brak urzadzen

            try
            {
                do
                {
                    found.Add(info);
                    info = new BLUETOOTH_DEVICE_INFO { dwSize = Marshal.SizeOf<BLUETOOTH_DEVICE_INFO>() };
                }
                while (BluetoothFindNextDevice(handle, ref info));
            }
            finally
            {
                BluetoothFindDeviceClose(handle);
            }

            return found;
        }

        private void TryPair(BLUETOOTH_DEVICE_INFO dev, string name)
        {
            _logger.LogInformation("[BT-Pair] Znaleziono niesparowany klucz '{Name}' - probuje sparowac.", name);

            var d = dev;
            uint rc = BluetoothAuthenticateDeviceEx(IntPtr.Zero, IntPtr.Zero, ref d, IntPtr.Zero, MitmNotRequiredBonding);

            if (rc != ERROR_SUCCESS)
            {
                _logger.LogInformation("[BT-Pair] Parowanie SSP nie powiodlo sie (kod {Code}) - probuje PIN 0000.", rc);
                d = dev;
                rc = BluetoothAuthenticateDevice(IntPtr.Zero, IntPtr.Zero, ref d, "0000", 4);
            }

            if (rc != ERROR_SUCCESS)
            {
                _logger.LogWarning("[BT-Pair] Nie udalo sie sparowac '{Name}' (kod {Code}). Kolejna proba za ok. {Sec} s.", name, rc, RetryCooldown.TotalSeconds);
                return;
            }

            _logger.LogInformation("[BT-Pair] Sparowano '{Name}'. Wlaczam usluge portu szeregowego (SPP).", name);

            var guid = SerialPortServiceClass;
            uint rs = BluetoothSetServiceState(IntPtr.Zero, ref d, ref guid, BLUETOOTH_SERVICE_ENABLE);
            if (rs != ERROR_SUCCESS)
                _logger.LogWarning("[BT-Pair] BluetoothSetServiceState(SPP) zwrocil kod {Code} - port COM moze pojawic sie dopiero po chwili.", rs);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}
