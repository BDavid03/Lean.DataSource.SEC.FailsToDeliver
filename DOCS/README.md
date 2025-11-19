![LEAN Data Source SDK](http://cdn.quantconnect.com.s3.us-east-1.amazonaws.com/datasources/Github_LeanDataSourceSDK.png)

# SEC Fails-to-Deliver Data Source

This repository implements the QuantConnect LEAN integration for the U.S. Securities and Exchange Commission (SEC) Fails‑to‑Deliver (FTD) reports. The processing pipeline downloads the twice‑monthly ZIP files from sec.gov, normalizes tickers into Lean symbols using the equity map files, and emits Lean‑ready history/universe CSVs under `alternative/sec-fails-to-deliver`.

---

## Repository Layout

| Path | Description |
| --- | --- |
| `FailsToDeliver*.cs` | Data models + sample algorithms that demonstrate how to request and consume the dataset inside Lean. |
| `DataProcessing/` | C# processing console that downloads/raw‑processes the SEC files. Includes `config.json` for the downloader and helper scripts. |
| `tests/` | Unit tests for serialization plus smoke tests that run the sample algorithms against Lean. |
| `output/` | Sample data (same structure as the final Lean `Data/` directory). |
| `listing-*.md`, `mycustomdata.json` | Documentation stubs used when the dataset is listed on the QuantConnect Data Market. |

Two `config.json` files exist on purpose:

- Root `config.json` is used when running Lean (`dotnet run --project Launcher/QuantConnect.Lean.Launcher.csproj`, `lean backtest`, etc.).
- `DataProcessing/config.json` is copied next to the downloader executable (so `dotnet run --project DataProcessing/DataProcessing.csproj` can resolve settings even outside this repo).

Keep the values in sync so the downloader and Lean read the same folders.

---

## Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download) (the repo targets `net9.0`).
- Lean cloned next to this repository (`../Lean`) so the tests can reference `QuantConnect.Tests`.
- Lean equity map files under `<Lean repo>/Data/equity/usa/map_files`. Without these, the downloader will skip every row because it cannot map tickers to Lean symbols.

Optional but recommended:

- QuantConnect Lean CLI for running local backtests.
- Python 3.8+ if you plan to run the QuantBook helper (`DataProcessing/process.sample.py`).

---

## Building and Testing

```powershell
dotnet build QuantConnect.DataSource.csproj -c Release
dotnet test tests/Tests.csproj /p:Configuration=Release
```

The GitHub workflow (`.github/workflows/build.yml`) mirrors these steps inside the `quantconnect/lean:foundation` container.

---

## Generating the Dataset

1. Update both `config.json` files:
   ```json
   {
       "data-folder": "C:\\Users\\User\\source\\repos\\Lean\\Data\\",
       "temp-output-directory": "C:\\Users\\User\\source\\repos\\Lean\\Data\\",
       "sec-user-agent": "YourCompanyNameYourEmail@example.com"
   }
   ```
   Use absolute paths so the downloader can always locate Lean’s data directory.

2. Run the downloader (from the repo root):
   ```powershell
   dotnet run --project DataProcessing/DataProcessing.csproj -c Release
   ```

   The console logs one line per SEC ZIP, e.g. `Processed 12345 rows for October 2024, first half.`. Files are written to:
   - `Lean/Data/alternative/sec-fails-to-deliver/<ticker>.csv`
   - `Lean/Data/alternative/sec-fails-to-deliver/universe/YYYYMMDD.csv`
   - `output/alternative/sec-fails-to-deliver/...` (sample copy for reviewers)

3. When re-running, the downloader merges new rows into the existing CSVs, dedupes, and keeps them chronologically sorted.

> **Tip:** If you only need to verify a small slice locally, copy the relevant map files into your Lean data folder (e.g., SPY → `equity/usa/map_files/spy.zip`). Rows for all other tickers are skipped until their map files exist.

---

## Validating the Data Source

1. **Sample Algorithm**  
   ```powershell
   cd ..\Lean
   dotnet run --project Launcher/QuantConnect.Lean.Launcher.csproj -- `
       --algorithm-type-name QuantConnect.DataLibrary.Tests.FailsToDeliverAlgorithm `
       --data-folder "..\Lean.DataSource.SEC.FailsToDeliver\Lean\Data"
   ```
   The algorithm logs “FailsToDeliver spike…” whenever the custom data arrives, so you know slices are populated.

2. **Universe Selection Algorithm**  
   `QuantConnect.Algorithm.CSharp.FailsToDeliverUniverseSelectionAlgorithm` reads the `universe/YYYYMMDD.csv` files. If the run throws, your universe snapshots are missing or malformed.

3. **Unit Tests**  
   `dotnet test` ensures the data model serializes/clones correctly and that the sample algorithms compile against Lean.

4. **QuantBook (optional)**  
   Build `DataProcessing`, copy `process.sample.py` into `DataProcessing/bin/Debug/net9.0`, set `Config.Set("data-folder", "<path>")`, and run the script to inspect history via the notebook API.

---

## Contributing / Next Steps

- Keep `README.md`, `listing-about.md`, and `listing-documentation.md` up to date with any schema or workflow changes.
- Submit sample CSVs under `output/alternative/...` whenever you open a PR so reviewers can reproduce the algorithms without running the downloader.
- If you implement the live provider (`MyCustomDataProvider.cs`) and downloader (`MyCustomDataDownloader.cs`), rename them to match this dataset (`FailsToDeliver*`) and wire them into Lean per the SDK guide.

Questions? Reach out to `support@quantconnect.com` or open an issue/PR. Happy data processing!
