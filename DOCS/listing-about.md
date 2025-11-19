### Meta
- **Dataset name**: SEC Fails-to-Deliver
- **Vendor name**: U.S. Securities and Exchange Commission
- **Vendor Website**: https://www.sec.gov/

### Introduction

The SEC Fails-to-Deliver dataset contains every equity security that appeared on the U.S. National Securities Clearing Corporation (NSCC) fails-to-deliver reports. The SEC publishes these reports twice each month and each entry includes the CUSIP, ticker, settlement date, number of shares that failed to settle, and reference pricing. Coverage starts in February 2004, spans all U.S. equities, and is updated at a daily resolution keyed to the publication date so algorithms can respect when the information became public.

### About the Provider
The U.S. Securities and Exchange Commission (SEC) is the independent U.S. Government agency that oversees securities markets. The agency gathers raw data from clearing firms, standardizes it, and releases multiple transparency datasets including fails-to-deliver, short sale metrics, and insider transactions.

### Getting Started
Python:
```
self.symbol = self.AddEquity("GME").Symbol
self.ftd_symbol = self.AddData(FailsToDeliver, "GME").Symbol
```

C#:
```
_equity = AddEquity("GME").Symbol;
_fails = AddData<FailsToDeliver>("GME").Symbol;
```

### Data Summary
- **Start Date**: 2004-02-01
- **Asset Coverage**: All U.S. listed equities with reported settlement issues
- **Resolution**: Daily (publication date)
- **Data Density**: Sparse
- **Timezone**: UTC

### Example Applications

The SEC Fails-to-Deliver dataset enables researchers to accurately monitor settlement pressure, short sale constraints, and borrow availability stress. Examples include:

- Tilt long/short books based on abnormal quantities of failed shares for individual symbols.
- Build universe filters that focus on names that repeatedly appear on the list while liquidity remains high.
- Detect crowding risk by combining FTD data with short-interest and options positioning metrics.

### Data Point Attributes

- `FailsToDeliver` (symbol-level history)
- `FailsToDeliverUniverse` (daily publication snapshots for universe selection)
