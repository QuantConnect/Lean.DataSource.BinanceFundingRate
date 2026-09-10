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
using System.IO;
using System.Net;
using System.Linq;
using System.Net.Http;
using Newtonsoft.Json;
using QuantConnect.Util;
using System.Threading;
using QuantConnect.Logging;
using System.Globalization;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// BinanceFundingRateDownloader implementation.
    /// </summary>
    public class BinanceFundingRateDownloader : IDisposable
    {
        private const string _binanceFutureCryptoApiEndpoint = "https://fapi.binance.com/fapi/v1";
        private const string _binanceFutureCoinApiEndpoint = "https://dapi.binance.com/dapi/v1";

        private static readonly string[] _apiEndpoints = [ _binanceFutureCoinApiEndpoint, _binanceFutureCryptoApiEndpoint ];

        /// <summary>
        /// Records per fundingRate response. Binance defaults this to 100 and truncates silently,
        /// which drops most symbols on any query wide enough to cover the whole exchange.
        /// </summary>
        private const int _fundingRatePageSize = 1000;

        /// <summary>
        /// Kept well below the rate gate: the download helper blocks its thread on the response,
        /// so an unbounded fan out over hundreds of symbols starves the thread pool and the
        /// in flight requests time out waiting for a thread to resume on.
        /// </summary>
        private const int _maxParallelDownloads = 8;

        private const int _maxDownloadAttempts = 3;

        /// <summary>
        /// Binance answers 429 once the IP exceeds its budget and 418 while it bans the IP, which
        /// lasts minutes; a back off of a few seconds would only burn the attempts inside the ban.
        /// </summary>
        private static readonly TimeSpan _rateLimitBackOff = TimeSpan.FromMinutes(1);

        private readonly DateTime? _deploymentDate;
        private readonly string _destinationFolder;
        private readonly string _existingInDataFolder;
        private readonly HttpClient _client = new();

        /// <summary>
        /// Control the rate of download per unit of time.
        /// </summary>
        private readonly RateGate _indexGate;

        /// <summary>
        /// Creates a new instance of <see cref="MyCustomData"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        public BinanceFundingRateDownloader(string destinationFolder, DateTime? deploymentDate)
        {
            _deploymentDate = deploymentDate;
            _destinationFolder = Path.Combine(destinationFolder, "cryptofuture", "binance", "margin_interest");
            _existingInDataFolder = Path.Combine(Globals.DataFolder, "cryptofuture", "binance", "margin_interest");

            // Represents rate limits of X requests per Y second
            _indexGate = new RateGate(25, TimeSpan.FromSeconds(1));

            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var success = true;
            var (start, end) = GetProcessingWindow();

            foreach (var baseApi in _apiEndpoints)
            {
                var symbols = GetPerpetualSymbols(baseApi);
                if (symbols == null)
                {
                    success = false;
                    continue;
                }

                var ratePerSymbol = new ConcurrentDictionary<string, Dictionary<DateTime, decimal>>();
                var failed = new ConcurrentBag<string>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = _maxParallelDownloads };

                Parallel.ForEach(symbols, options, symbol =>
                {
                    var rates = GetData(baseApi, symbol.Name, start, end);
                    if (rates == null)
                    {
                        // a symbol the exchange no longer lists may be rejected outright, and the file
                        // we already hold is left untouched either way, so it must not fail the run
                        if (!symbol.Delisted)
                        {
                            failed.Add(symbol.Name);
                        }
                    }
                    else if (rates.Count > 0)
                    {
                        ratePerSymbol[symbol.Name] = rates;
                    }
                });

                // a contract trading since before the window settles at least every 8 hours, so it
                // must have produced rows. Anything else is listed later or no longer trading
                var missing = symbols
                    .Where(x => x.IsTrading && x.OnboardTime < start && !ratePerSymbol.ContainsKey(x.Name) && !failed.Contains(x.Name))
                    .Select(x => x.Name)
                    .ToList();

                if (!failed.IsEmpty)
                {
                    Log.Error($"Run(): {baseApi} download failed for {failed.Count} perpetuals: {string.Join(", ", failed)}");
                    success = false;
                }
                if (missing.Count > 0)
                {
                    Log.Error($"Run(): {baseApi} returned no funding rate for {missing.Count} trading perpetuals: {string.Join(", ", missing)}");
                    success = false;
                }

                var recovered = symbols.Count(x => x.Delisted && ratePerSymbol.ContainsKey(x.Name));
                Log.Trace($"Run(): {baseApi} returned funding rates for {ratePerSymbol.Count - recovered} of " +
                    $"{symbols.Count(x => !x.Delisted)} perpetuals and {recovered} delisted between {start:yyyyMMdd} and {end:yyyyMMdd}");

                // what did download is still good data, so it is written even when the run fails
                foreach (var kvp in ratePerSymbol)
                {
                    SaveContentToFile(_destinationFolder, kvp.Key.RemoveFromEnd("_PERP"), kvp.Value);
                }
            }

            return success;
        }

        /// <summary>
        /// The perpetual contracts an endpoint settles funding for. The USDT endpoint also lists
        /// tokenized equities as TRADIFI_PERPETUAL, which pay funding as well.
        /// </summary>
        private ApiSymbol[] GetPerpetualSymbols(string baseApi)
        {
            var exchangeInfo = Download<ExchangeInfo>($"{baseApi}/exchangeInfo");
            if (exchangeInfo?.Symbols == null)
            {
                return null;
            }

            var listed = exchangeInfo.Symbols
                .Where(x => x.ContractType != null && x.ContractType.EndsWith("PERPETUAL", StringComparison.InvariantCultureIgnoreCase))
                .ToList();

            return [.. listed, .. GetDelistedSymbols(baseApi, listed)];
        }

        /// <summary>
        /// Binance drops a delisted contract from exchangeInfo but keeps serving its funding history,
        /// so a ticker we already store and can no longer see is still worth asking for; without this
        /// a full rebuild loses its file. Which endpoint settled it is not recorded anywhere, so both
        /// are asked: the one that does not know the symbol answers with an empty page.
        /// </summary>
        private IEnumerable<ApiSymbol> GetDelistedSymbols(string baseApi, IEnumerable<ApiSymbol> listed)
        {
            if (!Directory.Exists(_existingInDataFolder))
            {
                return [];
            }

            var known = listed
                .Select(x => x.Name.RemoveFromEnd("_PERP"))
                .ToHashSet(StringComparer.InvariantCultureIgnoreCase);

            // coin margined contracts are named <TICKER>_PERP, the file keeps the ticker
            var suffix = baseApi == _binanceFutureCoinApiEndpoint ? "_PERP" : string.Empty;

            return Directory.EnumerateFiles(_existingInDataFolder, "*.csv")
                .Select(x => Path.GetFileNameWithoutExtension(x).ToUpperInvariant())
                .Where(x => !known.Contains(x))
                .Select(x => new ApiSymbol { Name = $"{x}{suffix}", Delisted = true });
        }

        private (DateTime Start, DateTime End) GetProcessingWindow()
        {
            if (_deploymentDate.HasValue)
            {
                return (_deploymentDate.Value.Date, _deploymentDate.Value.Date.AddDays(1));
            }

            // everything
            return (new DateTime(2019, 9, 13), DateTime.UtcNow.Date.AddDays(1));
        }

        /// <summary>
        /// Pages through the funding rates of a symbol. Returns null when a page could not be
        /// downloaded, so the caller does not mistake a lost symbol for one without funding.
        /// </summary>
        private Dictionary<DateTime, decimal> GetData(string baseApi, string symbol, DateTime start, DateTime end)
        {
            var result = new Dictionary<DateTime, decimal>();
            var from = (long)Time.DateTimeToUnixTimeStampMilliseconds(start);
            var to = (long)Time.DateTimeToUnixTimeStampMilliseconds(end);

            while (from < to)
            {
                var page = Download<ApiFundingRate[]>($"{baseApi}/fundingRate?symbol={symbol}&startTime={from}&endTime={to}&limit={_fundingRatePageSize}");
                if (page == null)
                {
                    return null;
                }

                foreach (var apiFundingRate in page)
                {
                    // funding times carry a few milliseconds of jitter
                    var fundingTime = Time.UnixMillisecondTimeStampToDateTime(apiFundingRate.FundingTime).RoundDown(Time.OneSecond);
                    result[fundingTime] = apiFundingRate.FundingRate;
                }

                if (page.Length < _fundingRatePageSize)
                {
                    break;
                }

                // a symbol settles at most once per funding time, so resuming right after the last
                // one we saw can neither skip nor repeat a record
                from = page.Max(x => x.FundingTime) + 1;
            }

            return result;
        }

        /// <summary>
        /// Downloads and deserializes a url, retrying the transient failures Binance throws at us
        /// over a few hundred requests. Returns null once the attempts are spent or the failure
        /// is permanent, so the caller decides what one lost request costs.
        /// </summary>
        private T Download<T>(string url) where T : class
        {
            for (var attempt = 1; attempt <= _maxDownloadAttempts; attempt++)
            {
                _indexGate.WaitToProceed();

                HttpStatusCode? statusCode = null;
                try
                {
                    // logs and returns false on a non success status code or connection error
                    if (_client.TryDownloadData<T>(url, out var result, out statusCode))
                    {
                        return result;
                    }
                }
                catch (Exception exception)
                {
                    // timeouts and malformed bodies escape the helper
                    Log.Error($"Download(): attempt {attempt} failed for {url}: {exception.Message}");
                }

                var rateLimited = statusCode is HttpStatusCode.TooManyRequests or (HttpStatusCode)418;
                if (!rateLimited && statusCode is >= HttpStatusCode.BadRequest and < HttpStatusCode.InternalServerError)
                {
                    // Binance will keep rejecting the same request
                    break;
                }

                if (attempt < _maxDownloadAttempts)
                {
                    Thread.Sleep(rateLimited ? _rateLimitBackOff : TimeSpan.FromSeconds(attempt));
                }
            }

            Log.Error($"Download(): giving up on {url}");
            return null;
        }

        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        /// <param name="destinationFolder">Final destination of the data</param>
        /// <param name="name">file name</param>
        /// <param name="contents">Contents to write</param>
        private void SaveContentToFile(string destinationFolder, string name, Dictionary<DateTime, decimal> contents)
        {
            name = name.ToLowerInvariant();
            var finalPath = Path.Combine(destinationFolder, $"{name}.csv");
            var existingPath = Path.Combine(_existingInDataFolder, $"{name}.csv");

            if (File.Exists(existingPath))
            {
                foreach (var line in File.ReadAllLines(existingPath))
                {
                    if (string.IsNullOrEmpty(line))
                    {
                        continue;
                    }
                    var parts = line.Split(',');
                    if (parts.Length == 1)
                    {
                        continue;
                    }
                    var time = DateTime.ParseExact(parts[0], "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None);
                    var rate = decimal.Parse(parts[1], NumberStyles.Any, CultureInfo.InvariantCulture);
                    if (!contents.ContainsKey(time))
                    {
                        // use existing unless we have a new value
                        contents[time] = rate;
                    }
                }
            }

            var finalLines = contents.OrderBy(x => x.Key).Select(x => $"{x.Key:yyyyMMdd HH:mm:ss},{x.Value.ToStringInvariant()}").ToList();

            var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.tmp");
            // the fleet writes these on linux and the store keeps unix line endings, so a run on
            // windows must not turn them into CRLF
            File.WriteAllText(tempPath, string.Join("\n", finalLines) + "\n");
            var tempFilePath = new FileInfo(tempPath);
            tempFilePath.MoveTo(finalPath, true);
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            _client.Dispose();
            _indexGate?.Dispose();
        }

        private class ApiFundingRate
        {
            public long FundingTime { get; set; }
            public decimal FundingRate { get; set; }
        }

        private class ExchangeInfo
        {
            public ApiSymbol[] Symbols { get; set; }
        }

        private class ApiSymbol
        {
            [JsonProperty(PropertyName = "symbol")]
            public string Name { get; set; }
            public string ContractType { get; set; }
            public long OnboardDate { get; set; }

            // the USDT endpoint calls it status, the coin endpoint contractStatus
            public string Status { get; set; }
            public string ContractStatus { get; set; }

            /// <summary>
            /// Set for a symbol exchangeInfo no longer lists, recovered from the data we already hold
            /// </summary>
            [JsonIgnore]
            public bool Delisted { get; set; }

            public bool IsTrading => (Status ?? ContractStatus) == "TRADING";
            public DateTime OnboardTime => Time.UnixMillisecondTimeStampToDateTime(OnboardDate);
        }
    }
}
