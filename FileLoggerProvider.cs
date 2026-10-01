using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;

namespace MqttModbusService;

/// <summary>
/// Prosty, bezpakietowy logger do pliku (dzienne pliki gateway-yyyyMMdd.log).
/// Dziala rownolegle z pozostalymi loggerami (Event Log, konsola). Format linii:
///   yyyy-MM-dd HH:mm:ss.fff|POZIOM|PelnaKategoria[id]|tresc   (kazdy wpis w jednej linii)
/// Plik jest otwierany z FileShare.ReadWrite, wiec mozna go czytac "na zywo"
/// (Deployer -> "Log na zywo"). Zapis idzie przez kolejke w osobnym watku, wiec
/// logowanie nie spowalnia petli komunikacji z kluczami.
/// </summary>
[ProviderAlias("File")]
public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly string _dir;
    private readonly int _retainDays;
    private readonly BlockingCollection<string> _queue = new(new ConcurrentQueue<string>(), 20_000);
    private readonly Thread _thread;

    private static string? _staticDir;

    private FileStream? _stream;
    private string? _streamDate;

    public FileLoggerProvider(string directory, int retainDays = 14)
    {
        _dir = directory;
        _staticDir = directory;
        _retainDays = retainDays;

        try { Directory.CreateDirectory(_dir); } catch { /* logowanie nie moze wywrocic uslugi */ }
        CleanupOldFiles();

        _thread = new Thread(WriteLoop) { IsBackground = true, Name = "FileLogger" };
        _thread.Start();
    }

    /// <summary>
    /// Synchroniczny zapis bezposrednio do pliku - dla sytuacji awaryjnych (nieobsluzony wyjatek,
    /// zamykanie procesu), kiedy kolejka w tle moglaby nie zdazyc sie oproznic.
    /// </summary>
    public static void WriteDirect(string level, string category, string message)
    {
        try
        {
            var dir = _staticDir;
            if (dir is null) return;
            Directory.CreateDirectory(dir);
            var path = Path.Combine(dir, $"gateway-{DateTime.Now:yyyyMMdd}.log");
            var msg = message.Replace("\r\n", " | ").Replace('\n', ' ').Replace('\r', ' ');
            var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}|{level}|{category}|{msg}\r\n";
            using var fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            var bytes = Encoding.UTF8.GetBytes(line);
            fs.Write(bytes, 0, bytes.Length);
        }
        catch { }
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(categoryName, this);

    internal void Enqueue(string line)
    {
        // Przy przepelnieniu kolejki (np. brak miejsca na dysku) wpis jest pomijany.
        _queue.TryAdd(line);
    }

    private void WriteLoop()
    {
        try
        {
            foreach (var line in _queue.GetConsumingEnumerable())
            {
                try
                {
                    var date = DateTime.Now.ToString("yyyyMMdd");
                    if (_stream is null || _streamDate != date)
                    {
                        _stream?.Dispose();
                        var path = Path.Combine(_dir, $"gateway-{date}.log");
                        _stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
                        _streamDate = date;
                        CleanupOldFiles();
                    }

                    var bytes = Encoding.UTF8.GetBytes(line + "\r\n");
                    _stream.Write(bytes, 0, bytes.Length);
                    _stream.Flush();
                }
                catch
                {
                    // Nie udalo sie zapisac - zamykamy strumien, przy nastepnym wpisie sprobujemy ponownie.
                    try { _stream?.Dispose(); } catch { }
                    _stream = null;
                    _streamDate = null;
                }
            }
        }
        catch { /* zamykanie aplikacji */ }
    }

    private void CleanupOldFiles()
    {
        try
        {
            var limit = DateTime.Now.AddDays(-_retainDays);
            foreach (var f in Directory.EnumerateFiles(_dir, "gateway-*.log"))
                if (File.GetLastWriteTime(f) < limit)
                    File.Delete(f);
        }
        catch { }
    }

    public void Dispose()
    {
        _queue.CompleteAdding();
        try { _thread.Join(2000); } catch { }
        try { _stream?.Dispose(); } catch { }
        _queue.Dispose();
    }

    private sealed class FileLogger : ILogger
    {
        private readonly string _category;
        private readonly FileLoggerProvider _provider;

        public FileLogger(string category, FileLoggerProvider provider)
        {
            _category = category;
            _provider = provider;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;

            var level = logLevel switch
            {
                LogLevel.Trace => "TRACE",
                LogLevel.Debug => "DEBUG",
                LogLevel.Information => "INFO",
                LogLevel.Warning => "WARN",
                LogLevel.Error => "ERROR",
                LogLevel.Critical => "CRIT",
                _ => "INFO",
            };

            var msg = formatter(state, exception);
            if (exception is not null) msg += " | " + exception;

            // jeden wpis = jedna linia (znaki | w tresci sa dozwolone - parser dzieli tylko na 4 pola)
            msg = msg.Replace("\r\n", " | ").Replace('\n', ' ').Replace('\r', ' ');

            _provider.Enqueue($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}|{level}|{_category}[{eventId.Id}]|{msg}");
        }
    }
}
