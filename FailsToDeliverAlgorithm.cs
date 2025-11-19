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

using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Orders;
using QuantConnect.Algorithm;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    /// <summary>
    /// Example algorithm using the custom data type as a source of alpha
    /// </summary>
    public class FailsToDeliverAlgorithm : QCAlgorithm
    {
        private Symbol _customDataSymbol;
        private Symbol _equitySymbol;

        /// <summary>
        /// Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.
        /// </summary>
        public override void Initialize()
        {
            SetStartDate(2024, 1, 1);
            SetEndDate(2024, 12, 31);
            SetCash(100000);

            // Trade GME using the SEC Fails-to-Deliver data as a sentiment overlay
            _equitySymbol = AddEquity("GME").Symbol;
            _customDataSymbol = AddData<FailsToDeliver>("GME").Symbol;
        }

        /// <summary>
        /// OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.
        /// </summary>
        /// <param name="slice">Slice object keyed by symbol containing the stock data</param>
        public override void OnData(Slice slice)
        {
            if (!slice.ContainsKey(_customDataSymbol))
            {
                return;
            }

            var data = slice[_customDataSymbol];
            // Simple heuristic: short the stock when the reported fails-to-deliver share quantity spikes,
            // and cover when activity cools off.
            var threshold = 1_000_000;
            if (data.Quantity > threshold && !Portfolio[_equitySymbol].IsShort)
            {
                SetHoldings(_equitySymbol, -0.5m);
                Debug($"FailsToDeliver spike: {data.Quantity:N0} shares for {_equitySymbol} (settlement {data.SettlementDate:yyyy-MM-dd})");
            }
            else if (data.Quantity < threshold * 0.25m && Portfolio[_equitySymbol].IsShort)
            {
                Liquidate(_equitySymbol);
            }
        }

        /// <summary>
        /// Order fill event handler. On an order fill update the resulting information is passed to this method.
        /// </summary>
        /// <param name="orderEvent">Order event details containing details of the events</param>
        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            if (orderEvent.Status.IsFill())
            {
                Debug($"Purchased Stock: {orderEvent.Symbol}");
            }
        }
    }
}
