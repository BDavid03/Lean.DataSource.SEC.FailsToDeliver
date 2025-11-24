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

using System.Collections.Generic;
using System.Linq;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;
namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Example algorithm using the custom data type as a source of alpha
    /// </summary>
    public class FailsToDeliverUniverseSelectionAlgorithm : QCAlgorithm
    {
        public override void Initialize()
        {
            UniverseSettings.Resolution = Resolution.Daily;
            SetStartDate(2021, 9, 1);
            SetEndDate(2021, 12, 31);
            SetCash(100_000);

            var universe = AddUniverse<FailsToDeliverUniverse>(data =>
            {
                var failsData = data.OfType<FailsToDeliverUniverse>().ToList();
                foreach (var datum in failsData)
                {
                    if (datum.Quantity > 3_000_000)
                    {
                        Log($"{datum.Symbol}: {datum.Quantity:N0} shares failed on {datum.SettlementDate:yyyy-MM-dd}");
                    }
                }
                return failsData
                    .Where(datum => datum.Quantity > 1_000_000)
                    .Select(datum => datum.Symbol);
            });

            if (history.Count == 0)
            {
                throw new System.Exception("Expected historical universe data but received none.");
            }
            foreach (var dataForDate in history)
            {
                var coarseData = dataForDate.Cast<FailsToDeliverUniverse>().ToList();
                if (coarseData.Count == 0)
                {
                    throw new System.Exception("Unexpected empty historical universe slice.");
                }
            }
        }
        /// <summary>
        /// Event fired each time that we add/remove securities from the data feed
        /// </summary>
        /// <param name="changes">Security additions/removals for this time step</param>
        public override void OnSecuritiesChanged(SecurityChanges changes)
        {
            Log(changes.ToString());
        }
    }
}