using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Win32;
using System.IO.Ports;

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
        private readonly Dictionary<long, string> _lastState = new(); // diagnostyka: loguj tylko zmiany stanu
        private readonly Dictionary<long, (DateTime At, int Tries)> _sppTries = new();
        private readonly CancellationTokenSource _cts = new();

        // Mapowanie "port z konfiguracji AWS" -> "nazwa klucza Bluetooth" (appsettings: BluetoothKeyPorts).
        private static Dictionary<string, string> s_aliases = new(StringComparer.OrdinalIgnoreCase);
        private static ILogger? s_logger;
        private static readonly object s_lock = new();
        private static readonly Dictionary<string, (DateTime At, string Port, string? Error)> s_cache = new(StringComparer.OrdinalIgnoreCase);
        private static readonly Regex s_comName = new(@"^COM\d+$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        private static HashSet<string> s_configuredKeys = new(StringComparer.OrdinalIgnoreCase); // nazwy kluczy z konfiguracji z AWS
        private static readonly Dictionary<string, string> s_lastLogged = new(StringComparer.OrdinalIgnoreCase);

        public BluetoothAutoPairer(ILogger logger, IConfiguration config, Func<bool> shouldScan)
        {
            _logger = logger;
            _shouldScan = shouldScan;

            s_logger = logger;
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var child in config.GetSection("BluetoothKeyPorts").GetChildren())
            {
                if (!string.IsNullOrWhiteSpace(child.Value)) map[child.Key.Trim()] = child.Value.Trim();
            }
            s_aliases = map;
            if (map.Count > 0)
                logger.LogInformation("[BT-Port] Mapowanie port (z AWS) -> klucz: {Map}", string.Join(", ", map.Select(kv => $"{kv.Key}={kv.Value}")));

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
                _logger.LogInformation("[BT-Pair] NamePatterns jest puste - parowane beda tylko klucze wpisane w konfiguracji z AWS (pole adresu = nazwa klucza).");
            }

            _logger.LogInformation(
                "[BT-Pair] Automatyczne parowanie WLACZONE. Wzorce nazw: {Patterns}; skan co {Sec} s (tylko gdy jakis klucz jest rozlaczony); aktywny inquiry: {Inq}.",
                string.Join(", ", _patterns.Select(p => p.ToString())), _intervalSec, _issueInquiry);

            var token = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, stoppingToken).Token;
            _ = Task.Run(() => LoopAsync(token));
        }

        private async Task LoopAsync(CancellationToken ct)
        {
            bool startup = true; // pierwszy przebieg zaraz po starcie: sprawdz wszystkie klucze niezaleznie od stanu polaczen
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(TimeSpan.FromSeconds(startup ? 3 : _intervalSec), ct); }
                catch (OperationCanceledException) { break; }

                try
                {
                    // Po starcie: raz sprawdz wszystko. Potem skanuj tylko, gdy jakis klucz jest rozlaczony
                    // (utrata polaczenia / ponowne wlaczenie klucza) - zdrowe polaczenia zostaja nietkniete.
                    if (!startup && !_shouldScan()) continue;
                    startup = false;

                    foreach (var dev in Scan(_issueInquiry))
                    {
                        if (ct.IsCancellationRequested) break;
                        var name = (dev.szName ?? "").Trim();
                        bool matches = IsConfiguredKey(name) || _patterns.Any(r => SafeMatch(r, name));

                        // Diagnostyka: co Windows raportuje o kluczu pasujacym do wzorca (logowane tylko przy zmianie stanu).
                        if (matches)
                        {
                            long macDiag = dev.Address & 0xFFFFFFFFFFFF;
                            string state = $"paired={dev.fAuthenticated != 0} remembered={dev.fRemembered != 0} connected={dev.fConnected != 0} COM=[{DescribePorts(macDiag)}]";
                            if (!_lastState.TryGetValue(macDiag, out var prev) || prev != state)
                            {
                                _lastState[macDiag] = state;
                                _logger.LogInformation("[BT-Pair] Stan '{Name}' (MAC {Mac:X12}): {State}.", name, macDiag, state);
                            }
                        }

                        if (dev.fAuthenticated != 0)
                        {
                            if (matches) EnsureSpp(dev, name); // sparowany, ale bez portu COM -> wlacz usluge portu szeregowego
                            continue; // juz sparowane
                        }
                        if (!matches) continue;

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

        /// <summary>Wszystkie wpisy portow COM (wychodzacych SPP) zapisane w rejestrze dla adresu MAC - takze nieaktualne.</summary>
        private static List<string> FindRegistryPorts(long mac)
        {
            var ports = new List<string>();
            try
            {
                string macHex = mac.ToString("X12");
                using var root = Registry.LocalMachine.OpenSubKey(@"SYSTEM\CurrentControlSet\Enum\BTHENUM");
                if (root == null) return ports;

                foreach (var svcName in root.GetSubKeyNames())
                {
                    if (!svcName.StartsWith("{00001101-", StringComparison.OrdinalIgnoreCase)) continue; // SPP
                    using var svc = root.OpenSubKey(svcName);
                    if (svc == null) continue;

                    foreach (var inst in svc.GetSubKeyNames())
                    {
                        if (inst.IndexOf(macHex + "_C", StringComparison.OrdinalIgnoreCase) < 0) continue; // port wychodzacy
                        using var prm = svc.OpenSubKey(inst + @"\Device Parameters");
                        var port = prm?.GetValue("PortName") as string;
                        if (!string.IsNullOrEmpty(port)) ports.Add(port);
                    }
                }
            }
            catch { /* diagnostyka - ignorujemy */ }
            return ports.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        }

        /// <summary>Porty COM klucza, ktore faktycznie istnieja w systemie teraz (wpisy po usunietych urzadzeniach sa pomijane).</summary>
        private static List<string> FindPresentPorts(long mac)
        {
            HashSet<string> present;
            try { present = new HashSet<string>(SerialPort.GetPortNames(), StringComparer.OrdinalIgnoreCase); }
            catch { return new List<string>(); }
            return FindRegistryPorts(mac).Where(p => present.Contains(p)).ToList();
        }

        private static string DescribePorts(long mac)
        {
            var all = FindRegistryPorts(mac);
            var present = FindPresentPorts(mac);
            var stale = all.Where(p => !present.Contains(p, StringComparer.OrdinalIgnoreCase)).ToList();
            if (all.Count == 0) return "brak";
            string txt = present.Count > 0 ? "aktywne: " + string.Join(",", present) : "brak aktywnych";
            if (stale.Count > 0) txt += "; nieaktywne: " + string.Join(",", stale);
            return txt;
        }

        /// <summary>Nazwy kluczy Bluetooth wpisane w konfiguracji z AWS (pole adresu zawierajace nazwe zamiast COMx).
        /// Tylko takie klucze (oraz pasujace do NamePatterns) sa parowane przez ta bramke.</summary>
        internal static void SetConfiguredKeys(IEnumerable<string> addresses)
        {
            var keys = new HashSet<string>(
                addresses.Select(a => (a ?? "").Trim()).Where(a => a.Length > 0 && !s_comName.IsMatch(a)),
                StringComparer.OrdinalIgnoreCase);
            lock (s_lock) { s_configuredKeys = keys; }
        }

        /// <summary>Nazwa z AWS pasuje do nazwy w Windows, gdy jest identyczna albo jest jej poczatkiem przed "_"
        /// (np. "CEM3-BT_701407S" pasuje do "CEM3-BT_701407S_BC"). Krotszy prefix bez granicy "_" nie pasuje.</summary>
        private static bool NameMatches(string configured, string actual)
        {
            configured = configured.Trim();
            actual = actual.Trim();
            if (configured.Length == 0) return false;
            return string.Equals(configured, actual, StringComparison.OrdinalIgnoreCase)
                || (actual.Length > configured.Length && actual.StartsWith(configured + "_", StringComparison.OrdinalIgnoreCase));
        }

        private static bool IsConfiguredKey(string name)
        {
            lock (s_lock) { return s_configuredKeys.Any(k => NameMatches(k, name)); }
        }

        /// <summary>Zamienia adres z konfiguracji AWS na realny port COM.
        ///  * "COM8"                  -> bez zmian (chyba ze jest wpis w BluetoothKeyPorts),
        ///  * "CEM3-BT_701407S_BC"    -> aktualny port COM tego klucza w TYM komputerze (numer nadaje Windows).
        /// Dla nazwy klucza, ktorej nie da sie rozwiazac, rzuca wyjatek z czytelnym opisem (worker ponawia probe).</summary>
        internal static string ResolvePort(string configuredPort)
        {
            if (!OperatingSystem.IsWindows()) return configuredPort;

            configuredPort = configuredPort.Trim();
            bool isKeyName = !s_comName.IsMatch(configuredPort);
            string? keyName = isKeyName
                ? configuredPort
                : (s_aliases.TryGetValue(configuredPort, out var alias) ? alias : null);
            if (keyName == null) return configuredPort;

            lock (s_lock)
            {
                if (s_cache.TryGetValue(configuredPort, out var c) && DateTime.UtcNow - c.At < TimeSpan.FromSeconds(10))
                {
                    if (c.Error != null && isKeyName) throw new IOException(c.Error);
                    return c.Port;
                }

                string result = configuredPort;
                string? why = null;
                try
                {
                    var all = Scan(false);
                    var exact = all.Where(d => string.Equals((d.szName ?? "").Trim(), keyName, StringComparison.OrdinalIgnoreCase)).ToList();
                    var hits = exact.Count > 0 ? exact : all.Where(d => NameMatches(keyName, d.szName ?? "")).ToList();

                    if (hits.Count == 0)
                        why = $"klucz '{keyName}' nie jest znany Windows (niesparowany / poza zasiegiem / nie dodany)";
                    else if (hits.Select(h => h.Address & 0xFFFFFFFFFFFF).Distinct().Count() > 1)
                        why = $"nazwa '{keyName}' pasuje do kilku kluczy - wpisz pelna nazwe klucza";
                    else
                    {
                        var ports = FindPresentPorts(hits[0].Address & 0xFFFFFFFFFFFF);
                        if (ports.Count == 0) why = $"klucz '{keyName}' nie ma aktywnego portu COM (usluga SPP nie jest wlaczona)";
                        else result = ports[0];
                    }
                }
                catch (Exception ex) { why = "blad rozwiazywania klucza: " + ex.Message; }

                s_cache[configuredPort] = (DateTime.UtcNow, result, why);

                string msg = why == null
                    ? $"{configuredPort} -> {result}"
                    : $"{configuredPort}: {why}";
                if (!s_lastLogged.TryGetValue(configuredPort, out var prev) || prev != msg)
                {
                    s_lastLogged[configuredPort] = msg;
                    s_logger?.LogInformation("[BT-Port] {Msg}", msg);
                }

                if (why != null && isKeyName) throw new IOException(why);
                return result;
            }
        }

        /// <summary>Sparowany klucz bez zadnego aktywnego portu COM: wlacza usluge portu szeregowego (SPP).
        /// Dotyczy tylko kluczy, ktore nie maja ZADNEGO portu, wiec nie przestawia numerow dzialajacych portow.</summary>
        private void EnsureSpp(BLUETOOTH_DEVICE_INFO dev, string name)
        {
            long mac = dev.Address & 0xFFFFFFFFFFFF;
            if (FindPresentPorts(mac).Count > 0) { _sppTries.Remove(mac); return; }

            _sppTries.TryGetValue(mac, out var t);
            var cooldown = t.Tries >= 3 ? TimeSpan.FromMinutes(5) : RetryCooldown;
            if (t.Tries > 0 && DateTime.UtcNow - t.At < cooldown) return;
            _sppTries[mac] = (DateTime.UtcNow, t.Tries + 1);

            var d = dev;
            var guid = SerialPortServiceClass;
            if (t.Tries >= 1)
                BluetoothSetServiceState(IntPtr.Zero, ref d, ref guid, 0x00); // 2. i kolejne proby: wylacz, a potem wlacz

            uint rs = BluetoothSetServiceState(IntPtr.Zero, ref d, ref guid, BLUETOOTH_SERVICE_ENABLE);
            _logger.LogInformation("[BT-Pair] '{Name}' jest sparowany, ale nie ma portu COM - wlaczam usluge SPP (proba {N}, kod {Code}).", name, t.Tries + 1, rs);
        }

        private static bool SafeMatch(Regex r, string input)
        {
            try { return r.IsMatch(input); } catch (RegexMatchTimeoutException) { return false; }
        }

        private static List<BLUETOOTH_DEVICE_INFO> Scan(bool issueInquiry)
        {
            var found = new List<BLUETOOTH_DEVICE_INFO>();

            var search = new BLUETOOTH_DEVICE_SEARCH_PARAMS
            {
                dwSize = Marshal.SizeOf<BLUETOOTH_DEVICE_SEARCH_PARAMS>(),
                fReturnAuthenticated = 1,
                fReturnRemembered = 1,
                fReturnUnknown = 1,
                fReturnConnected = 1,
                fIssueInquiry = issueInquiry ? 1 : 0,
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
