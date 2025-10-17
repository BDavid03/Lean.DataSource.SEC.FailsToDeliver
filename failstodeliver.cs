using System;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using System.Globalization;
using QuantConnect;

namespace QuantConnect.DataSource
{
    public class FailsToDeliver : BaseData
    {
        public int Quantity { get; set; }
        public decimal Price { get; set; }
        public string Description { get; set; }

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLive)
        {
            var path = Path.Combine(
                Globals.DataFolder, "alternative", "sec_ftd",
                $"{date:yyyy-MM-dd}.csv"
            );
            return new SubscriptionDataSource(path, SubscriptionTransportMedium.LocalFile, FileFormat.Csv);
        }

        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLive)
        {
            var parts = line.Split(',');
            if (parts.Length < 5) return null;

            return new FailsToDeliver
            {
                Symbol = new Symbol(SecurityIdentifier.GenerateBase(parts[1], SecurityType.Base, Market.USA), parts[1]),
                Time = DateTime.ParseExact(parts[0], "yyyy-MM-dd", CultureInfo.InvariantCulture),
                Quantity = int.Parse(parts[2]),
                Price = decimal.Parse(parts[3], CultureInfo.InvariantCulture),
                Description = parts[4],
                Value = decimal.Parse(parts[3], CultureInfo.InvariantCulture)  // used for charting
            };
        }
    }
}
