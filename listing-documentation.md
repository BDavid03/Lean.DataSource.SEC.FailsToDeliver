### Requesting Data
To add the SEC Fails-to-Deliver dataset to your algorithm, call `AddData<FailsToDeliver>()` for each symbol you need. As with all custom data, keep the returned `Symbol` for fast lookups. For more information, see [Importing Custom Data](https://www.quantconnect.com/docs/algorithm-reference/importing-custom-data).

Python:
```
self.equity = self.AddEquity("GME").Symbol
self.fails_symbol = self.AddData(FailsToDeliver, "GME").Symbol
```

C#:
```
_equity = AddEquity("GME").Symbol;
_fails = AddData<FailsToDeliver>("GME").Symbol;
```

### Accessing Data
Fails-to-Deliver data arrives once per publication date inside each `Slice`. The data contains the original settlement date, CUSIP, the number of shares that failed to settle, price, and description. Publication dates occur twice per month: the first-half file arrives at the end of the month and the second-half file arrives around the 15th of the next month.

Python:
```
def OnData(self, slice: Slice):
    if self.fails_symbol not in slice:
        return

    datum = slice[self.fails_symbol]
    if datum.Quantity > 1_000_000:
        self.SetHoldings(self.equity, -0.5)
```

C#:
```
public override void OnData(Slice slice)
{
    if (!slice.ContainsKey(_fails))
    {
        return;
    }

    var datum = slice[_fails];
    if (datum.Quantity > 1_000_000)
    {
        SetHoldings(_equity, -0.5m);
    }
}
```

### Historical Data
Use the `History` API with the custom data symbol to gather previously published fails-to-deliver quantities. The history result respects the publication date, so a request prior to the official release returns empty.

Python:
```
history = self.History(self.fails_symbol, 30, Resolution.Daily)
for time, row in history.loc[self.fails_symbol].iterrows():
    self.Debug(f"{time.date()} -> {row['quantity']} shares failed on {row['settlementdate'].date()}")
```

C#:
```
var history = History<FailsToDeliver>(_fails, 30, Resolution.Daily);
foreach (var datum in history)
{
    Debug($"{datum.EndTime:yyyy-MM-dd}: {datum.Quantity:N0} fails (settlement {datum.SettlementDate:yyyy-MM-dd})");
}
```
