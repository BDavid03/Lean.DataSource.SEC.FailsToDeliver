/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataSource;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Downloader/converter for the SEC Fails-to-Deliver dataset
    /// </summary>
    public class FailsToDeliverUniverseDataDownloader : IDisposable
    {
        public const string VendorName = "U.S. Securities and Exchange Commission";
        public const string VendorDataName = FailsToDeliver.SourceDirectory;

        private const string CatalogUrl = "https://www.sec.gov/data.json";
        private const string CatalogHtmlUrl = "https://catalog.data.gov/dataset/fails-to-deliver-data";

        private readonly string _destinationFolder;
        private readonly string _universeFolder;
        private readonly string _dataFolder = Globals.DataFolder;
        private readonly bool _canCreateUniverseFiles;
        private readonly int _maxRetries = 5;
        private readonly string _userAgent;
        private readonly RateGate _indexGate;
        private readonly HttpClient _httpClient;
        private readonly bool _skipProcessedDistributions;
        private readonly string _temporaryFolder;
        private ConcurrentDictionary<string, ConcurrentQueue<string>> _tempData = new();

        /// <summary>
        /// Creates a new instance of the downloader
        /// </summary>
        public FailsToDeliverUniverseDataDownloader(string destinationFolder, string apiKey = null)
        {
            _destinationFolder = Path.Combine(destinationFolder, VendorDataName);
            _universeFolder = Path.Combine(_destinationFolder, "universe");
            _userAgent = string.IsNullOrWhiteSpace(apiKey)
                ? Config.Get("sec-user-agent", "QuantConnectLeanDataDownloader/1.0 (support@quantconnect.com)")
                : apiKey;
            _canCreateUniverseFiles = Directory.Exists(Path.Combine(_dataFolder, "equity", "usa", "map_files"));

            // SEC Fair Access guidelines recommend no more than 10 requests per second.
            _indexGate = new RateGate(5, TimeSpan.FromSeconds(1));

            _httpClient = new HttpClient
            {
                Timeout = TimeSpan.FromMinutes(5)
            };
            _httpClient.DefaultRequestHeaders.Clear();
            _httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(_userAgent);
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));

            _skipProcessedDistributions = Config.GetBool("sec-skip-processed-distributions", true);
            _temporaryFolder = Path.Combine(_destinationFolder, "tmp");

            Directory.CreateDirectory(_destinationFolder);
            Directory.CreateDirectory(_universeFolder);
            Directory.CreateDirectory(_temporaryFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var stopwatch = Stopwatch.StartNew();
            if (!_canCreateUniverseFiles)
            {
                Log.Error($"FailsToDeliverUniverseDataDownloader.Run(): Missing Lean map files under {_dataFolder}. Please run the Lean data downloader before executing this process.");
                return false;
            }

            List<DistributionMetadata> distributions;
            try
            {
                distributions = GetDistributionMetadata();
            }
            catch (Exception err)
            {
                Log.Error(err, "FailsToDeliverUniverseDataDownloader.Run(): Failed to parse the SEC catalog.");
                return false;
            }

            if (distributions.Count == 0)
            {
                Log.Error("FailsToDeliverUniverseDataDownloader.Run(): No distribution metadata was discovered.");
                return false;
            }

            var today = DateTime.UtcNow.Date;
            var dataProvider = new DefaultDataProvider();
            var mapFileProvider = new LocalZipMapFileProvider();
            mapFileProvider.Initialize(dataProvider);

            var processedAny = false;
            var downloadTasks = new List<Task>();

            foreach (var distribution in distributions)
            {
                if (distribution.ProcessDate.Date > today)
                {
                    continue;
                }

                if (_skipProcessedDistributions && AlreadyProcessed(distribution.ProcessDate))
                {
                    Log.Trace($"FailsToDeliverUniverseDataDownloader.Run(): Skipping {distribution.Title} because data already exists.");
                    continue;
                }

                downloadTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        Log.Trace($"FailsToDeliverUniverseDataDownloader.Run(): Downloading {distribution.Title} ({distribution.DownloadUrl})");
                        var bytes = await DownloadBinary(distribution.DownloadUrl);
                        if (bytes == null || bytes.Length == 0)
                        {
                            return;
                        }

                        using var archive = new ZipArchive(new MemoryStream(bytes));
                        ProcessDistributionArchive(distribution, archive, mapFileProvider, ref processedAny);
                    }
                    catch (Exception err)
                    {
                        Log.Error(err, $"FailsToDeliverUniverseDataDownloader.Run(): Failed processing {distribution.Title} ({distribution.DownloadUrl}).");
                    }
                }));
            }

            Task.WhenAll(downloadTasks).GetAwaiter().GetResult();
            FlushSymbolData();

            FlushSymbolData();

            Log.Trace($"FailsToDeliverUniverseDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return processedAny;
        }

        private void ProcessDistributionArchive(DistributionMetadata distribution, ZipArchive archive, LocalZipMapFileProvider mapFileProvider, ref bool processedAny)
        {
            var processDate = distribution.ProcessDate.Date;
            var entry = archive.Entries.FirstOrDefault();
            if (entry == null)
            {
                Log.Error($"FailsToDeliverUniverseDataDownloader.ProcessDistribution(): Unable to locate payload inside archive for {distribution.Title}.");
                return;
            }

            var universeLines = new List<string>();
            var processedLines = 0;

            using var entryStream = entry.Open();
            using var reader = new StreamReader(entryStream);

            reader.ReadLine();

            string rawLine;
            while ((rawLine = reader.ReadLine()) != null)
            {
                if (!TryParseRawLine(rawLine, out var record))
                {
                    continue;
                }

                if (!TryCreateSymbol(record.Symbol, record.SettlementDate, mapFileProvider, out var symbol))
                {
                    continue;
                }

                var dataLine = string.Join(",",
                    processDate.ToStringInvariant(DateFormat.EightCharacter),
                    record.SettlementDate.ToStringInvariant(DateFormat.EightCharacter),
                    record.Cusip,
                    record.Quantity.ToStringInvariant(),
                    record.ReferencePrice.ToStringInvariant());

                AppendSymbolLine(symbol.Value, dataLine);

                if (_canCreateUniverseFiles)
                {
                    universeLines.Add(string.Join(",",
                        symbol.ID,
                        symbol.Value,
                        record.Cusip,
                        record.Quantity.ToStringInvariant(),
                        record.SettlementDate.ToStringInvariant(DateFormat.EightCharacter),
                        record.ReferencePrice.ToStringInvariant()));
                }

                processedLines++;
            }

            if (_canCreateUniverseFiles && universeLines.Count > 0)
            {
                SaveContentToFile(_universeFolder, processDate.ToStringInvariant(DateFormat.EightCharacter), universeLines);
            }

            if (processedLines > 0)
            {
                processedAny = true;
            }

            var skipped = universeLines.Count - processedLines;
            Log.Trace($"FailsToDeliverUniverseDataDownloader.ProcessDistribution(): Processed {processedLines:N0} rows for {distribution.Title}. Kept {processedLines:N0}, skipped {Math.Max(0, skipped):N0}.");
        }

        private List<DistributionMetadata> GetDistributionMetadata()
        {
            var html = HttpRequester(CatalogHtmlUrl).Result;
            if (string.IsNullOrWhiteSpace(html))
            {
                return new List<DistributionMetadata>();
            }

            var results = new List<DistributionMetadata>();
            foreach (var link in ScrapeDownloadLinks(html))
            {
                var normalizedUrl = NormalizeDownloadUrl(link);
                var fileName = TryGetFileName(normalizedUrl);
                if (string.IsNullOrEmpty(fileName))
                {
                    continue;
                }

                if (!TryParseDistributionFileName(fileName, out var year, out var month, out var half))
                {
                    continue;
                }

                results.Add(new DistributionMetadata
                {
                    Title = $"{CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(month)} {year}, {(half == 'a' ? "first" : "second")} half",
                    DownloadUrl = normalizedUrl,
                    ProcessDate = GetProcessingDate(year, month, half)
                });
            }

            return results
                .OrderBy(r => r.ProcessDate)
                .ToList();
        }

        /// <summary>
        /// Sends a GET request for the provided URL and returns the payload as string
        /// </summary>
        private async Task<string> HttpRequester(string url)
        {
            for (var retries = 1; retries <= _maxRetries; retries++)
            {
                try
                {
                    _indexGate.WaitToProceed();
                    var response = await _httpClient.GetAsync(url);
                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        Log.Error($"FailsToDeliverUniverseDataDownloader.HttpRequester(): Files not found at url: {url}");
                        response.DisposeSafely();
                        return string.Empty;
                    }

                    response.EnsureSuccessStatusCode();
                    var result = await response.Content.ReadAsStringAsync();
                    response.DisposeSafely();
                    return result;
                }
                catch (Exception e)
                {
                    Log.Error(e, $"FailsToDeliverUniverseDataDownloader.HttpRequester(): Error retrieving {url}. (retry {retries}/{_maxRetries})");
                    Thread.Sleep(1000 * retries);
                }
            }

            throw new Exception($"Request failed with no more retries remaining for {url} (retry {_maxRetries}/{_maxRetries})");
        }

        /// <summary>
        /// Downloads binary payloads
        /// </summary>
        private async Task<byte[]> DownloadBinary(string url)
        {
            for (var retries = 1; retries <= _maxRetries; retries++)
            {
                try
                {
                    _indexGate.WaitToProceed();
                    var response = await _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        Log.Error($"FailsToDeliverUniverseDataDownloader.DownloadBinary(): Files not found at url: {url}");
                        response.DisposeSafely();
                        return Array.Empty<byte>();
                    }

                    response.EnsureSuccessStatusCode();
                    var bytes = await response.Content.ReadAsByteArrayAsync();
                    response.DisposeSafely();
                    return bytes;
                }
                catch (Exception e)
                {
                    Log.Error(e, $"FailsToDeliverUniverseDataDownloader.DownloadBinary(): Error downloading {url}. (retry {retries}/{_maxRetries})");
                    Thread.Sleep(1000 * retries);
                }
            }

            throw new Exception($"Binary request failed with no more retries remaining for {url} (retry {_maxRetries}/{_maxRetries})");
        }

        private static DateTime GetProcessingDate(int year, int month, char halfIndicator)
        {
            var start = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc);
            if (char.ToLowerInvariant(halfIndicator) == 'a')
            {
                // The first half becomes public on the final calendar day of the same month
                return start.AddMonths(1).AddDays(-1);
            }

            // The second half becomes public around the 15th of the following month
            var nextMonth = start.AddMonths(1);
            return new DateTime(nextMonth.Year, nextMonth.Month, 15, 0, 0, 0, DateTimeKind.Utc);
        }

        private static bool TryParseRawLine(string line, out FailRecord record)
        {
            record = default;
            if (string.IsNullOrWhiteSpace(line))
            {
                return false;
            }

            var csv = line.Split('|');
            if (csv.Length < 6)
            {
                return false;
            }

            if (!DateTime.TryParseExact(csv[0], DateFormat.EightCharacter, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var settlementDate))
            {
                return false;
            }

            var quantityString = csv[3].Replace(",", string.Empty).Trim();
            if (!long.TryParse(quantityString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var quantity) || quantity <= 0)
            {
                return false;
            }

            var symbol = csv[2].Trim();
            if (string.IsNullOrWhiteSpace(symbol))
            {
                return false;
            }

            var priceString = csv.Length > 5 ? csv[5].Replace(",", string.Empty).Trim() : string.Empty;
            decimal price = 0m;
            if (!string.IsNullOrWhiteSpace(priceString))
            {
                decimal.TryParse(priceString, NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out price);
            }

            record = new FailRecord
            {
                SettlementDate = DateTime.SpecifyKind(settlementDate, DateTimeKind.Utc),
                Cusip = csv[1].Trim(),
                Symbol = symbol,
                Quantity = quantity,
                ReferencePrice = price
            };

            return true;
        }

        private bool TryCreateSymbol(string ticker, DateTime date, LocalZipMapFileProvider mapFileProvider, out Symbol symbol)
        {
            symbol = null;
            if (!TryNormalizeDefunctTicker(ticker, out var normalizedTicker))
            {
                return false;
            }

            var cleanedTicker = CleanTicker(normalizedTicker);
            if (string.IsNullOrWhiteSpace(cleanedTicker))
            {
                return false;
            }

            try
            {
                var securityIdentifier = SecurityIdentifier.GenerateEquity(cleanedTicker, Market.USA, true, mapFileProvider, date);
                symbol = new Symbol(securityIdentifier, cleanedTicker);
                return true;
            }
            catch (Exception err)
            {
                Log.Debug($"FailsToDeliverUniverseDataDownloader.TryCreateSymbol(): {err.Message} for {cleanedTicker} on {date:yyyy-MM-dd}");
                return false;
            }
        }

        private static string CleanTicker(string ticker)
        {
            if (string.IsNullOrWhiteSpace(ticker))
            {
                return string.Empty;
            }

            var builder = new List<char>(ticker.Length);
            foreach (var character in ticker.Trim())
            {
                if (char.IsLetterOrDigit(character))
                {
                    builder.Add(char.ToUpperInvariant(character));
                    continue;
                }

                if (character == '.' || character == '/' || character == '-' || character == '_')
                {
                    builder.Add('.');
                }
            }

            var cleaned = new string(builder.ToArray()).Trim('.');
            return cleaned;
        }

        private static string NormalizeDownloadUrl(string url)
        {
            if (url.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                return url;
            }

            return $"https://www.sec.gov{(url.StartsWith("/") ? url : "/" + url)}";
        }

        private static string TryGetFileName(string url)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out var uri))
            {
                return Path.GetFileNameWithoutExtension(uri.AbsolutePath);
            }

            return Path.GetFileNameWithoutExtension(url);
        }

        private static IEnumerable<string> ScrapeDownloadLinks(string html)
        {
            const string marker = "https://www.sec.gov/files/data/fails-deliver-data/";
            var links = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var span = html.AsSpan();
            while (true)
            {
                var idx = span.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
                if (idx == -1)
                {
                    break;
                }

                span = span[idx..];
                var end = span.IndexOf('"');
                if (end <= 0)
                {
                    break;
                }

                links.Add(span[..end].ToString());
                span = span[end..];
            }

            return links;
        }

        private bool AlreadyProcessed(DateTime processDate)
        {
            var universePath = Path.Combine(_universeFolder, $"{processDate.ToStringInvariant(DateFormat.EightCharacter)}.csv");
            return File.Exists(universePath);
        }

        private static bool TryParseDistributionFileName(string fileName, out int year, out int month, out char half)
        {
            year = 0;
            month = 0;
            half = '\0';

            if (string.IsNullOrWhiteSpace(fileName))
            {
                return false;
            }

            fileName = fileName.Trim();
            if (!fileName.StartsWith("cnsfails", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var token = fileName.Substring("cnsfails".Length);
            if (token.Length < 7)
            {
                return false;
            }

            if (!int.TryParse(token.AsSpan(0, 4), NumberStyles.None, CultureInfo.InvariantCulture, out year))
            {
                return false;
            }

            if (!int.TryParse(token.AsSpan(4, 2), NumberStyles.None, CultureInfo.InvariantCulture, out month))
            {
                return false;
            }

            half = char.ToLowerInvariant(token[6]);
            if (half != 'a' && half != 'b')
            {
                return false;
            }

            return true;
        }

        private void AppendSymbolLine(string symbol, string content)
        {
            var queue = _tempData.GetOrAdd(symbol, _ => new ConcurrentQueue<string>());
            queue.Enqueue(content);
        }

        private void FlushSymbolData()
        {
            foreach (var (symbol, lines) in _tempData)
            {
                SaveContentToFile(_destinationFolder, symbol, lines);
            }

            _tempData = new ConcurrentDictionary<string, ConcurrentQueue<string>>();
        }

        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        private void SaveContentToFile(string destinationFolder, string name, IEnumerable<string> contents)
        {
            name = name.ToLowerInvariant();
            var finalPath = Path.Combine(destinationFolder, $"{name}.csv");
            var finalFileExists = File.Exists(finalPath);

            if (_skipProcessedDistributions && finalFileExists)
            {
                File.AppendAllLines(finalPath, contents);
                return;
            }

            var lines = new HashSet<string>(contents);
            if (finalFileExists)
            {
                foreach (var line in File.ReadAllLines(finalPath))
                {
                    lines.Add(line);
                }
            }

            var finalLines = destinationFolder.Contains("universe") ?
                lines.OrderBy(x => x.Split(',').First()).ToList() :
                lines
                    .OrderBy(x => DateTime.ParseExact(x.Split(',').First(), DateFormat.EightCharacter, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal))
                    .ToList();

            var tempPath = Path.Combine(_temporaryFolder, $"{Guid.NewGuid()}.tmp");
            File.WriteAllLines(tempPath, finalLines);
            var tempFilePath = new FileInfo(tempPath);
            tempFilePath.MoveTo(finalPath, true);
        }

        /// <summary>
        /// Tries to normalize a potentially defunct ticker into a normal ticker.
        /// </summary>
        private static bool TryNormalizeDefunctTicker(string ticker, out string nonDefunctTicker)
        {
            if (ticker.IndexOf("defunct", StringComparison.OrdinalIgnoreCase) > 0)
            {
                foreach (var delimChar in _defunctDelimiters)
                {
                    var length = ticker.IndexOf(delimChar, StringComparison.Ordinal);
                    if (length == -1)
                    {
                        continue;
                    }

                    nonDefunctTicker = ticker[..length].Trim();
                    return true;
                }

                nonDefunctTicker = string.Empty;
                return false;
            }

            nonDefunctTicker = ticker;
            return true;
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (Directory.Exists(_temporaryFolder))
                {
                    Directory.Delete(_temporaryFolder, true);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, $"FailsToDeliverUniverseDataDownloader.Dispose(): Failed to delete temporary folder {_temporaryFolder}");
            }

            _indexGate?.Dispose();
            _httpClient?.Dispose();
        }

        private sealed class DistributionMetadata
        {
            public string Title { get; set; }
            public string DownloadUrl { get; set; }
            public DateTime ProcessDate { get; set; }
        }

        private sealed class FailRecord
        {
            public DateTime SettlementDate { get; set; }
            public string Cusip { get; set; }
            public string Symbol { get; set; }
            public long Quantity { get; set; }
            public decimal ReferencePrice { get; set; }
        }

        private static readonly List<char> _defunctDelimiters = new()
        {
            '-',
            '_'
        };
    }
}
