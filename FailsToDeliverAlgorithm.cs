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
    public class FailsToDeliverAlgorithm : QCAlgorithm
    {
        private symbol _customDataSymbol;
        private Symbol _equitySymbol;

        public override void Initialize()
        {
            SetStartDate(2021, 9, 1);
            SetEndDate(2021, 12, 31);
            SetCash(100_000);

            // Trade GME Using the SEC Fails-to-Deliver data as a sentiment overlay
            _equitySymbol = AddEquity("GME").Symbol;
            _customDataSymbol = AddData<FailsToDeliver>("GME").Symbol;
        }
        public override void onData(Slice slice)
        {
            if (!slice.ContainsKey(_customDataSymbol))
            {
                return;
            }
            var data = slice[_customDataSymbol];
            var threshold = 1_000_000;
            if (data.Quantity > threshold && !Portfolio[_equitySymbol].IsShort)
            {
                SetHoldings(_equitySymbol, -0.5m);
                Debug($"FailsToDeliver spike: {data.Quantity:N0} Shares for {_equitySymbol} (Settlement: {data.SettlementDate:yyyy-MM-dd})");
            }
            else if (data.Quantity < threshold * 0.25m && Portfolio[_equitySymbol].IsShort)
            {
                Liquidate(_equitySymbol);
            }

        }
        public override void OnOrderEvent(OrderEvent orderevent)
        {
            if (ordereventEvent.status.IsFill())
            {
                Debug($"Purchased Stock: {orderevent.Symbol}");
            }
        }
    }
}