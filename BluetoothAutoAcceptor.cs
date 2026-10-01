using Microsoft.Extensions.Logging;
using System;
using System.Runtime.InteropServices;

namespace MqttModbusGateway
{
    /// <summary>
    /// Rejestruje w systemowym stosie Bluetooth Windows handler, ktory automatycznie
    /// odpowiada "zgadzam sie" (Allow) na kazde zadanie parowania/potwierdzenia polaczenia,
    /// zamiast pokazywac uzytkownikowi okienko "Pair device? Allow/Cancel".
    ///
    /// KLUCZOWE: rejestracja dziala na poziomie calego systemu (BluetoothRegisterForAuthenticationEx
    /// z pbtdiIn = NULL rejestruje handler dla WSZYSTKICH urzadzen, nie jednego), a nie sesji
    /// konkretnego uzytkownika. Jesli ten kod dziala wewnatrz procesu USLUGI (LocalSystem, sesja 0),
    /// dziala on caly czas - takze wtedy, gdy ekran jest zablokowany, nikt nie jest zalogowany,
    /// albo zmienil sie operator. To rozwiazuje dwa problemy naraz:
    ///   1) brak koniecznosci recznego klikania "Allow" przy parowaniu kluczy dynamometrycznych,
    ///   2) utrate polaczenia po zablokowaniu/wylogowaniu - bo wczesniej ponowne polaczenie/parowanie
    ///      utykalo w oczekiwaniu na potwierdzenie, ktorego nikt (zaden zalogowany user) nie widzial.
    ///
    /// Uzycie: utworzyc jedna instancje na cale zycie uslugi, wywolac Start() raz przy starcie
    /// (np. w ServiceWorker.ExecuteAsync, przed Gateway.RunAsync), i Dispose() przy zatrzymaniu.
    /// </summary>
    internal sealed class BluetoothAutoAcceptor : IDisposable
    {
        private const int ERROR_SUCCESS = 0;

        // BLUETOOTH_AUTHENTICATION_METHOD (bluetoothapis.h)
        private enum AuthMethod
        {
            Legacy = 1,
            Oob = 2,
            NumericComparison = 3,
            PasskeyNotification = 4,
            Passkey = 5,
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct SYSTEMTIME
        {
            public ushort wYear, wMonth, wDayOfWeek, wDay, wHour, wMinute, wSecond, wMilliseconds;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct BLUETOOTH_DEVICE_INFO
        {
            public int dwSize;
            public long Address; // BLUETOOTH_ADDRESS - dolne 6 bajtow to adres MAC urzadzenia
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
        private struct BLUETOOTH_AUTHENTICATION_CALLBACK_PARAMS
        {
            public BLUETOOTH_DEVICE_INFO deviceInfo;
            public int authenticationMethod;
            public int ioCapability;
            public int authenticationRequirements;
            public uint NumericValueOrPasskey; // union { Numeric_Value; Passkey; }
        }

        // BLUETOOTH_AUTHENTICATE_RESPONSE - w C to struktura z unia; tutaj odwzorowana
        // jawnymi offsetami (LayoutKind.Explicit), bo C# nie ma prawdziwych unii.
        [StructLayout(LayoutKind.Explicit, Size = 48)]
        private struct BLUETOOTH_AUTHENTICATE_RESPONSE
        {
            [FieldOffset(0)] public long bthAddressRemote;
            [FieldOffset(8)] public int authMethod;

            // -- poczatek unii (offset 12, max 32 bajty) --
            // Wszystkie pola unii sa typami blittable (bez byte[]) - CLR zabrania nakladania
            // pola referencyjnego na wartosciowe (TypeLoadException).
            // Legacy PIN (pin[16]) zaczyna sie od offsetu 12, wiec 4-znakowy PIN mieści sie w pierwszym uint.
            [FieldOffset(12)] public uint numericValueOrPasskey; // NumericComparison / PasskeyNotification / Passkey / pierwsze 4 bajty PIN-u
            [FieldOffset(28)] public byte legacyPinLength;
            // -- koniec unii --

            [FieldOffset(44)] public byte negativeResponse; // 0 = akceptuj, 1 = odrzuc
        }

        private delegate void AuthCallbackEx(IntPtr pvParam, ref BLUETOOTH_AUTHENTICATION_CALLBACK_PARAMS pParams);

        [DllImport("bthprops.cpl", SetLastError = true)]
        private static extern uint BluetoothRegisterForAuthenticationEx(
            IntPtr pbtdiIn,
            out IntPtr phRegHandle,
            AuthCallbackEx pfnCallbackIn,
            IntPtr pvParam);

        [DllImport("bthprops.cpl", SetLastError = true)]
        private static extern uint BluetoothSendAuthenticationResponseEx(
            IntPtr hRadio,
            ref BLUETOOTH_AUTHENTICATE_RESPONSE pauthResponse);

        [DllImport("bthprops.cpl")]
        private static extern uint BluetoothUnregisterAuthentication(IntPtr hRegHandle);

        private readonly ILogger _logger;
        private IntPtr _regHandle = IntPtr.Zero;
        private AuthCallbackEx? _callback; // trzymamy zywa referencje - inaczej GC sprzatnie delegat i callback z natywnego kodu bedzie crashowac

        public BluetoothAutoAcceptor(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>Rejestruje globalny handler autoryzacji. Zwraca false, jesli rejestracja sie nie powiodla
        /// (np. brak radia Bluetooth na komputerze) - w takim razie parowanie nadal bedzie wymagac reki operatora.</summary>
        public bool Start()
        {
            _callback = OnAuthenticationRequested;

            uint result = BluetoothRegisterForAuthenticationEx(IntPtr.Zero, out _regHandle, _callback, IntPtr.Zero);

            if (result != ERROR_SUCCESS)
            {
                _logger.LogWarning(
                    "[BT-Auth] BluetoothRegisterForAuthenticationEx nie powiodlo sie (kod {Code}). " +
                    "Automatyczne akceptowanie parowania NIE jest aktywne - parowanie nadal bedzie wymagac recznego potwierdzenia.",
                    result);
                return false;
            }

            _logger.LogInformation("[BT-Auth] Automatyczne akceptowanie parowania Bluetooth aktywne (system-wide, niezalezne od sesji uzytkownika).");
            return true;
        }

        private void OnAuthenticationRequested(IntPtr pvParam, ref BLUETOOTH_AUTHENTICATION_CALLBACK_PARAMS p)
        {
            try
            {
                var method = (AuthMethod)p.authenticationMethod;
                _logger.LogInformation(
                    "[BT-Auth] Zadanie parowania od '{Name}' (metoda={Method}) - akceptuje automatycznie.",
                    p.deviceInfo.szName, method);

                var response = new BLUETOOTH_AUTHENTICATE_RESPONSE
                {
                    bthAddressRemote = p.deviceInfo.Address,
                    authMethod = p.authenticationMethod,
                    negativeResponse = 0, // 0 = akceptuj
                };

                switch (method)
                {
                    case AuthMethod.NumericComparison:
                    case AuthMethod.PasskeyNotification:
                    case AuthMethod.Passkey:
                        // "Just Works" (brak wyswietlacza/klawiatury po stronie urzadzenia) trafia tutaj -
                        // to jest dokladnie ten przypadek okienka "Pair device? Allow/Cancel" bez PIN-u.
                        response.numericValueOrPasskey = p.NumericValueOrPasskey;
                        break;

                    case AuthMethod.Legacy:
                        // Proste modulu BT-SPP (np. w kluczach dynamometrycznych) czesto uzywaja
                        // starego parowania z domyslnym PIN-em "0000".
                        // PIN "0000" = bajty 0x30 0x30 0x30 0x30 (little-endian, offset 12); reszta pin[16] to zera.
                        response.numericValueOrPasskey = 0x30303030;
                        response.legacyPinLength = 4;
                        break;

                    default:
                        _logger.LogWarning("[BT-Auth] Nieobslugiwana metoda uwierzytelniania ({Method}) - parowanie odrzucone.", method);
                        response.negativeResponse = 1;
                        break;
                }

                uint sendResult = BluetoothSendAuthenticationResponseEx(IntPtr.Zero, ref response);
                if (sendResult != ERROR_SUCCESS)
                {
                    _logger.LogWarning("[BT-Auth] BluetoothSendAuthenticationResponseEx zwrocil kod bledu {Code}.", sendResult);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BT-Auth] Blad w obsludze zadania parowania Bluetooth.");
            }
        }

        public void Dispose()
        {
            if (_regHandle != IntPtr.Zero)
            {
                BluetoothUnregisterAuthentication(_regHandle);
                _regHandle = IntPtr.Zero;
            }
        }
    }
}
