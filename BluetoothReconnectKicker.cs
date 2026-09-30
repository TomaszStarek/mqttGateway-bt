using Microsoft.Extensions.Logging;
using System;
using System.Management;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace MqttModbusGateway
{
    /// <summary>
    /// Przyspiesza ponowne polaczenie Bluetooth po zerwaniu linku (np. blad
    /// "The semaphore timeout period has expired" przy otwieraniu portu COM),
    /// zamiast biernie czekac, az Windows sam odswiezy polaczenie (co potrafilo
    /// trwac od kilku do kilkunastu sekund).
    ///
    /// Dziala przez wylaczenie i natychmiastowe wlaczenie z powrotem uslugi
    /// Serial Port Profile (SPP) dla danego urzadzenia (BluetoothSetServiceState) -
    /// to zmusza stos Bluetooth do natychmiastowej proby ponownego zestawienia
    /// kanalu RFCOMM / portu COM, zamiast czekania na jego wewnetrzny timer.
    ///
    /// Adres MAC urzadzenia jest potrzebny do wywolania tego API, a DeviceWorker
    /// zna tylko nazwe portu (np. "COM10") - dlatego ta klasa sama, raz, ustala
    /// adres na podstawie WMI (Win32_PnPEntity) i trzyma go w pamieci.
    /// </summary>
    internal sealed class BluetoothReconnectKicker
    {
        private const int ERROR_SUCCESS = 0;
        private const uint BLUETOOTH_SERVICE_DISABLE = 0x00;
        private const uint BLUETOOTH_SERVICE_ENABLE = 0x01;
        private static readonly TimeSpan KickThrottle = TimeSpan.FromSeconds(3);

        // Standardowy (well-known) UUID profilu Serial Port Profile (SPP) w Bluetooth.
        private static readonly Guid SerialPortServiceClassUuid = new("00001101-0000-1000-8000-00805F9B34FB");

        [StructLayout(LayoutKind.Sequential)]
        private struct SYSTEMTIME
        {
            public ushort wYear, wMonth, wDayOfWeek, wDay, wHour, wMinute, wSecond, wMilliseconds;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct BLUETOOTH_DEVICE_INFO
        {
            public int dwSize;
            public long Address; // BLUETOOTH_ADDRESS - proste 6-bajtowe MAC jako liczba (np. 0xAABBCCDDEEFF)
            public uint ulClassofDevice;
            public int fConnected;
            public int fRemembered;
            public int fAuthenticated;
            public SYSTEMTIME stLastSeen;
            public SYSTEMTIME stLastUsed;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 248)]
            public string szName;
        }

        [DllImport("bthprops.cpl", SetLastError = true)]
        private static extern uint BluetoothSetServiceState(
            IntPtr hRadio,
            ref BLUETOOTH_DEVICE_INFO pbtdi,
            ref Guid pGuidService,
            uint dwServiceFlags);

        private readonly ILogger _logger;
        private readonly string _deviceLogId;

        private ulong? _btAddress;       // ustalane raz - adres MAC sie nie zmienia
        private bool _resolveAttempted;  // zeby nie odpytywac WMI w kolko, jesli sie raz nie udalo
        private DateTime _lastKickUtc = DateTime.MinValue;

        public BluetoothReconnectKicker(ILogger logger, string deviceLogId)
        {
            _logger = logger;
            _deviceLogId = deviceLogId;
        }

        /// <summary>
        /// Probuje wymusic natychmiastowe odswiezenie polaczenia Bluetooth dla danego portu COM.
        /// Bezpieczne w wywolywaniu czesto (throttlowane wewnetrznie) - jesli adresu MAC nie da sie
        /// ustalic, po prostu nic nie robi i worker wraca do zwyklego, biernego ponawiania.
        /// </summary>
        public void TryKick(string comPort)
        {
            if (DateTime.UtcNow - _lastKickUtc < KickThrottle) return;

            ulong? address = ResolveBluetoothAddress(comPort);
            if (address is null) return;

            _lastKickUtc = DateTime.UtcNow;

            var deviceInfo = new BLUETOOTH_DEVICE_INFO { Address = unchecked((long)address.Value) };
            deviceInfo.dwSize = Marshal.SizeOf<BLUETOOTH_DEVICE_INFO>();
            var guid = SerialPortServiceClassUuid;

            try
            {
                BluetoothSetServiceState(IntPtr.Zero, ref deviceInfo, ref guid, BLUETOOTH_SERVICE_DISABLE);
                uint result = BluetoothSetServiceState(IntPtr.Zero, ref deviceInfo, ref guid, BLUETOOTH_SERVICE_ENABLE);

                if (result == ERROR_SUCCESS)
                    _logger.LogInformation($"[{_deviceLogId}] Wymuszono natychmiastowe odswiezenie polaczenia Bluetooth dla {comPort}.");
                else
                    _logger.LogDebug($"[{_deviceLogId}] BluetoothSetServiceState (wymuszenie reconnectu) zwrocil kod {result} dla {comPort}.");
            }
            catch (Exception ex)
            {
                _logger.LogDebug($"[{_deviceLogId}] Blad przy wymuszaniu reconnectu Bluetooth dla {comPort}: {ex.Message}");
            }
        }

        private ulong? ResolveBluetoothAddress(string comPort)
        {
            if (_btAddress is not null) return _btAddress;
            if (_resolveAttempted) return null;
            _resolveAttempted = true;

            try
            {
                using var searcher = new ManagementObjectSearcher(
                    $"SELECT DeviceID, Name FROM Win32_PnPEntity WHERE Name LIKE '%({comPort})%'");

                foreach (ManagementBaseObject device in searcher.Get())
                {
                    string? deviceId = device["DeviceID"]?.ToString();
                    if (string.IsNullOrEmpty(deviceId)) continue;

                    // DeviceID dla wirtualnego portu COM przez Bluetooth SPP wyglada np. tak:
                    // BTHENUM\{00001101-...}_LOCALMFG&0000\7&2a80b1d2&0&001122334455_C00000000
                    // 12 znakow hex tuz przed "_C<cyfry>" (albo po "&0&") to adres MAC urzadzenia.
                    var match = Regex.Match(deviceId, "([0-9A-Fa-f]{12})_C[0-9]+$");
                    if (!match.Success)
                        match = Regex.Match(deviceId, "&0&([0-9A-Fa-f]{12})");

                    if (match.Success)
                    {
                        string macHex = match.Groups[1].Value;
                        ulong address = Convert.ToUInt64(macHex, 16);
                        _btAddress = address;
                        _logger.LogInformation($"[{_deviceLogId}] Rozpoznano adres Bluetooth dla {comPort}: {macHex} - szybkie wymuszanie reconnectu aktywne.");
                        return _btAddress;
                    }
                }

                _logger.LogWarning(
                    $"[{_deviceLogId}] Nie udalo sie automatycznie rozpoznac adresu Bluetooth dla portu {comPort}. " +
                    "Szybkie wymuszanie reconnectu niedostepne dla tego urzadzenia - worker nadal bedzie dzialal, " +
                    "tylko wracac po zerwaniu polaczenia biernie (tak jak dotychczas).");
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"[{_deviceLogId}] Blad przy rozpoznawaniu adresu Bluetooth dla {comPort}: {ex.Message}");
            }

            return null;
        }
    }
}
