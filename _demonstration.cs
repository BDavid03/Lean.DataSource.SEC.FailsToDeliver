using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.DataSource;
using QuantConnect.Data;

namespace QuantConnect.Algorithm.CSharp
{
    public class FailsToDeliverExampleAlgorithm : QCAlgorithm
    {
        private Symbol _ftdSymbol;

        public override void Initialize()
        {
            SetStartDate(2024, 1, 1);
            SetEndDate(2024, 12, 31);
            SetCash(100000);

            // Register the custom dataset
            _ftdSymbol = AddData<FailsToDeliver>("FTD").Symbol;
        }

        public override void OnData(Slice slice)
        {
            if (slice.ContainsKey(_ftdSymbol))
            {
                var ftd = slice.Get<FailsToDeliver>(_ftdSymbol);
                Debug($"FTD {ftd.Time:yyyy-MM-dd} | {ftd.Symbol.Value} | Qty {ftd.Quantity} | ${ftd.Price}");
            }
        }
    }
}
