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
using System.Linq;
using Newtonsoft.Json;
using QuantConnect.Util;
using System.Threading;
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

        /// <summary>
        /// Records per fundingRate response. Binance defaults this to 100 and truncates silently,
        /// which drops most symbols on any query wide enough to cover the whole exchange.
        /// </summary>
        private const int _fundingRatePageSize = 1000;

        /// <summary>
        /// Kept well below the rate gate: <see cref="Extensions.DownloadData"/> blocks its thread,
        /// so an unbounded fan out over hundreds of symbols starves the thread pool and the
        /// in flight requests time out waiting for a thread to resume on.
        /// </summary>
        private const int _maxParallelDownloads = 8;

        private const int _maxDownloadAttempts = 3;

        private static readonly string[] _apiEndpoints = [_binanceFutureCoinApiEndpoint, _binanceFutureCryptoApiEndpoint];

        private readonly DateTime? _deploymentDate;
        private readonly string _destinationFolder;
        private readonly string _existingInDataFolder;
        private readonly Dictionary<string, string[]> _perpetualSymbolsPerApi;

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

            _perpetualSymbolsPerApi = _apiEndpoints.ToDictionary(baseApi => baseApi, GetPerpetualSymbols);
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
                var symbols = _perpetualSymbolsPerApi[baseApi];
                var ratePerSymbol = new ConcurrentDictionary<string, Dictionary<DateTime, decimal>>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = _maxParallelDownloads };

                Parallel.ForEach(symbols, options, symbol =>
                {
                    var rates = GetData(baseApi, symbol, start, end);
                    if (rates.Count > 0)
                    {
                        ratePerSymbol[symbol] = rates;
                    }
                    // an empty result is expected for a contract listed after the window we process
                });

                if (ratePerSymbol.IsEmpty)
                {
                    Logging.Log.Error($"Run(): {baseApi} returned no funding rate for any of its {symbols.Length} perpetuals between {start:yyyyMMdd} and {end:yyyyMMdd}");
                    success = false;
                    continue;
                }

                Logging.Log.Trace($"Run(): {baseApi} returned funding rates for {ratePerSymbol.Count} of {symbols.Length} perpetuals");

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
        private static string[] GetPerpetualSymbols(string baseApi)
        {
            var exchangeInfo = JsonConvert.DeserializeObject<ExchangeInfo>(Extensions.DownloadData($"{baseApi}/exchangeInfo"));

            return exchangeInfo.Symbols
                .Where(x => x.ContractType != null && x.ContractType.EndsWith("PERPETUAL", StringComparison.InvariantCultureIgnoreCase))
                .Select(x => x.Name)
                .ToArray();
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

        private Dictionary<DateTime, decimal> GetData(string baseApi, string symbol, DateTime start, DateTime end)
        {
            var result = new Dictionary<DateTime, decimal>();
            var from = (long)Time.DateTimeToUnixTimeStampMilliseconds(start);
            var to = (long)Time.DateTimeToUnixTimeStampMilliseconds(end);

            while (from < to)
            {
                var page = DownloadPage(symbol, $"{baseApi}/fundingRate?symbol={symbol}&startTime={from}&endTime={to}&limit={_fundingRatePageSize}");
                if (page is null or { Length: 0 })
                {
                    break;
                }

                foreach (var apiFundingRate in page)
                {
                    var fundingTime = Time.UnixMillisecondTimeStampToDateTime(apiFundingRate.FundingTime);
                    var key = new DateTime(fundingTime.Year, fundingTime.Month, fundingTime.Day, fundingTime.Hour, fundingTime.Minute, fundingTime.Second);
                    result[key] = apiFundingRate.FundingRate;
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
        /// Downloads a single page of funding rates, retrying the transient failures Binance throws
        /// at us over a few hundred requests instead of losing the whole run to one of them.
        /// </summary>
        private ApiFundingRate[] DownloadPage(string symbol, string url)
        {
            for (var attempt = 1; attempt <= _maxDownloadAttempts; attempt++)
            {
                _indexGate.WaitToProceed();

                try
                {
                    // returns null on a non success status code
                    var data = Extensions.DownloadData(url);
                    if (data != null)
                    {
                        return JsonConvert.DeserializeObject<ApiFundingRate[]>(data);
                    }

                    Logging.Log.Trace($"DownloadPage(): {symbol} attempt {attempt} returned no data");
                }
                catch (Exception exception)
                {
                    Logging.Log.Trace($"DownloadPage(): {symbol} attempt {attempt} failed: {exception.Message}");
                }

                Thread.Sleep(TimeSpan.FromSeconds(attempt));
            }

            throw new Exception($"DownloadPage(): giving up on {symbol} after {_maxDownloadAttempts} attempts: {url}");
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
            File.WriteAllLines(tempPath, finalLines);
            var tempFilePath = new FileInfo(tempPath);
            tempFilePath.MoveTo(finalPath, true);
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            _indexGate?.Dispose();
        }

        private class ApiFundingRate
        {
            public string Symbol { get; set; }
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
        }
    }
}
