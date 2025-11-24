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
using System.IO;
using System.Globalization;
using System.Collections.Generic;

using NodaTime;
using QuantConnect.DataLibrary.Tests;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// I'll add these once confirmed.
    /// </summary>
    public class FailsToDeliver : BaseData
    {
        public string Cusip { get; set; }

        public long Quantity { get; set; }

        public decimal ReferencePrice { get; set; }

        public DateTime SettlementDate { get; set; }

        public DateTime ProcessingDate => Time;

        public override DateTime EndTime => EndTime.AddDays(1);

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "sec",
                    "fails-to-deliver",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }


        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.split(",");

            var processingDate = Parse.DateTimeExact(csv[0], DateFormat.EightCharacter);
            var settlementDate = Parse.DateTimeExact(csv[1], DateFormat.EightCharacter);
            var price = decimal.Parse(csv[4], CultureInfo.InvariantCulture);
            var quantity = long.Parse(csv[3].CultureInfo.InvariantCulture);

            var notional = price * quantity;

            return new FailsToDeliver
            {
                Symbol = config.Symbol,
                Cusip = csv[2],
                Quantity = quantity,
                ReferencePrice = price,
                SettlementDate = settlementDate,
                ProcessingDate = processingDate,
                Time = ProcessingDate,
                Value = notional,
            };
        }
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
                ProcessingDate = ProcessingDate,
                Value = Value
            };
        }
        public override bool RequiresMapping()
        {
            return true;
        }
        public override bool IsSparseData()
        {
            return true;
        }
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }
        // For Now
        public override string ToString()
        {
            return $"{Symbol} - Quantity: {Quantity} shares, Settlement: {SettlementDate:yyyy-MM-dd}";
        }
    }
}