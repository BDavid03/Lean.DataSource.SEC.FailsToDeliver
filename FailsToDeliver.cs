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
 *
*/

using System;
using NodaTime;
using System.IO;
using QuantConnect.Data;
using System.Collections.Generic;
using System.Globalization;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// U.S. SEC Fails-to-Deliver data point
    /// </summary>
    public class FailsToDeliver : BaseData
    {
        /// <summary>
        /// Name of the directory where the dataset is stored
        /// </summary>
        public const string SourceDirectory = "sec-fails-to-deliver";

        public string Cusip { get; set; }

        public long Quantity { get; set; }

        public decimal ReferencePrice { get; set; }

        public DateTime SettlementDate { get; set; }

        public DateTime ProcessingDate { get; set; }

        /// <summary>
        /// Time passed between the date of the data and the time the data became available to us
        /// </summary>
        public TimeSpan Period { get; set; } = TimeSpan.Zero;

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + Period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    SourceDirectory,
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');

            var processingDate = Parse.DateTimeExact(csv[0], DateFormat.EightCharacter);
            var settlementDate = Parse.DateTimeExact(csv[1], DateFormat.EightCharacter);
            var price = decimal.Parse(csv[4], CultureInfo.InvariantCulture);
            var quantity = long.Parse(csv[3], CultureInfo.InvariantCulture);

            var notional = price * quantity;
            return new FailsToDeliver
            {
                Symbol = config.Symbol,
                Cusip = csv[2],
                Quantity = quantity,
                ReferencePrice = price,
                SettlementDate = settlementDate,
                ProcessingDate = processingDate,
                Time = processingDate,
                Value = notional
            };
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        public override BaseData Clone()
        {
            return new FailsToDeliver
            {
                Symbol = Symbol,
                Time = Time,
                EndTime = EndTime,
                Cusip = Cusip,
                Quantity = Quantity,
                ReferencePrice = ReferencePrice,
                SettlementDate = SettlementDate,
                ProcessingDate = ProcessingDate,
                Value = Value
            };
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol and requires that corporate events be applied to it as well, such as renames and delistings
        /// </summary>
        /// <returns>false</returns>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - Quantity: {Quantity} shares, Settlement: {SettlementDate:yyyy-MM-dd}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }
    }
}
