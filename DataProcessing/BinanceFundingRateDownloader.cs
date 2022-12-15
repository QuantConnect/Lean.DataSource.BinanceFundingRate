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
using System.Globalization;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// BinanceFundingRateDownloader implementation.
    /// </summary>
    public class BinanceFundingRateDownloader : IDisposable
    {
        private const string _binanceFutureCryptoApiEndpoint = "https://fapi.binance.com/fapi/v1";
        private const string _binanceFutureCoinApiEndpoint = "https://dapi.binance.com/dapi/v1";

        private readonly DateTime? _deploymentDate;
        private readonly string _destinationFolder;
        private readonly string _existingInDataFolder;
        private readonly ExchangeInfo _exchangeInfo;

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

            _exchangeInfo = JsonConvert.DeserializeObject<ExchangeInfo>(Extensions.DownloadData($"{_binanceFutureCoinApiEndpoint}/exchangeInfo"));
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            foreach (var baseApi in new[] { _binanceFutureCoinApiEndpoint, _binanceFutureCryptoApiEndpoint })
            {
                var ratePerSymbol = new Dictionary<string, Dictionary<DateTime, decimal>>();
                foreach (var date in GetProcessingDates())
                {
                    foreach (var apiFundingRate in GetData(baseApi, date))
                    {
                        var fundingTime = Time.UnixMillisecondTimeStampToDateTime(apiFundingRate.FundingTime);
                        if (!ratePerSymbol.TryGetValue(apiFundingRate.Symbol, out var dictionary))
                        {
                            ratePerSymbol[apiFundingRate.Symbol] = dictionary = new();
                        }

                        var key = new DateTime(fundingTime.Year, fundingTime.Month, fundingTime.Day, fundingTime.Hour, fundingTime.Minute, fundingTime.Second);
                        dictionary[key] = apiFundingRate.FundingRate;
                    }
                }

                foreach (var kvp in ratePerSymbol)
                {
                    SaveContentToFile(_destinationFolder, kvp.Key.RemoveFromEnd("_PERP"), kvp.Value);
                }
            }

            return true;
        }

        private IEnumerable<DateTime> GetProcessingDates()
        {
            if (_deploymentDate.HasValue)
            {
                return new[] { _deploymentDate.Value };
            }
            else
            {
                // everything
                return Time.EachDay(new DateTime(2019, 9, 13), DateTime.UtcNow.Date);
            }
        }

        private IEnumerable<ApiFundingRate> GetData(string baseApi, DateTime date)
        {
            var start = (long)Time.DateTimeToUnixTimeStampMilliseconds(date.Date);
            var end = (long)Time.DateTimeToUnixTimeStampMilliseconds(date.AddDays(1).Date);

            if (baseApi == _binanceFutureCryptoApiEndpoint)
            {
                _indexGate.WaitToProceed();

                // symbol not mandatory
                var data = Extensions.DownloadData($"{baseApi}/fundingRate?startTime={start}&endTime={end}");

                try
                {
                    return JsonConvert.DeserializeObject<ApiFundingRate[]>(data);
                }
                catch (Exception)
                {
                    Logging.Log.Error($"GetData(): deserialization failed {data}");
                    throw;
                }
            }
            else
            {
                var result = new List<ApiFundingRate>();

                if(date < new DateTime(2020, 10, 1))
                {
                    // nothing before this date
                    return result;
                }
                Parallel.ForEach(_exchangeInfo.Symbols.Where(x => x.ContractType.Equals("PERPETUAL", StringComparison.InvariantCultureIgnoreCase)), symbol =>
                {
                    _indexGate.WaitToProceed();

                    var data = Extensions.DownloadData($"{baseApi}/fundingRate?startTime={start}&endTime={end}&symbol={symbol.Name}");

                    lock (result)
                    {
                        try
                        {
                            result.AddRange(JsonConvert.DeserializeObject<ApiFundingRate[]>(data));
                        }
                        catch (Exception)
                        {
                            Logging.Log.Error($"GetData(): deserialization failed {data}");
                            throw;
                        }
                    }
                });
                return result;
            }
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