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
using QuantConnect.Data;
using QuantConnect.UniverseSelection;
using QuantConnect.DataSource;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// I'll add these once confirmed.
    /// </summary>
    public class FailsToDeliverUniverse : BaseDataCollection
    {
        public override DateTime EndTime => EndTime.AddDays(1);

        public string Cusip { get; set; }

        public long Quantity { get; set; }

        public DateTime SettlementDate { get; set; }

        public decimal ReferencePrice { get; set; }

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "sec",
                    "fails-to-deliver",
                    "universe",
                    $"{date.ToStringInvariant(DateFormat.EightCharacter)}.csv"
                ),
                SubscriptionTransportMedium.LocalFile,
                FileFormat.FoldingCollection
            );
        }
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');
            var quantity = long.Parse(csv[3], CultureInfo.InvariantCulture);
            var settlementDate = Parse.DateTimeExact(csv[4], DateFormat.EightCharacter);
            var RefPrice = decimal.Parse(csv[5], CultureInfo.InvariantCulture);
            var notional = RefPrice * quantity;
            return new FailsToDeliverUniverse
            {
                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Cusip = csv[2],
                Quantity = quantity,
                SettlementDate = settlementDate,
                ReferencePrice = RefPrice,
                Time = date.AddDays(-1),
                Value = notional
            };
        }
        public override bool IsSparseData()
        {
            return true;
        }
        public override Resolution DefaultResolution()
        {
            return DefaultResolution.Daily;
        }
        public override List<Resolution> SupportResolutions()
        {
            return DailyResolution;
        }
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }
        public override string ToString()
        {
            return $"{Symbol} - Quantity: {quantity} {SettlementDate}";
        }
        public override BaseData Clone()
        {
            return new FailsToDeliverUniverse
            {
                Symbol = Symbol,
                Time = Time,
                Data = Data,
                value = Value
            };
        }
    }
}