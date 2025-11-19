# Usage Examples

## Backtesting the Sample Algorithm (C#)
```powershell
cd ..\Lean
dotnet run --project Launcher/QuantConnect.Lean.Launcher.csproj -- `
    --algorithm-type-name QuantConnect.DataLibrary.Tests.FailsToDeliverAlgorithm `
    --data-folder "..\Lean.DataSource.SEC.FailsToDeliver\Lean\Data"
```

## Backtesting the Sample Algorithm (Python)
```powershell
cd ..\Lean
dotnet run --project Launcher/QuantConnect.Lean.Launcher.csproj -- `
    --algorithm-type-name QuantConnect.DataLibrary.Tests.FailsToDeliverAlgorithmPy `
    --data-folder "..\Lean.DataSource.SEC.FailsToDeliver\Lean\Data"
```

## Universe Selection Sample
```powershell
cd ..\Lean
dotnet run --project Launcher/QuantConnect.Lean.Launcher.csproj -- `
    --algorithm-type-name QuantConnect.Algorithm.CSharp.FailsToDeliverUniverseSelectionAlgorithm `
    --data-folder "..\Lean.DataSource.SEC.FailsToDeliver\Lean\Data"
```

Each command assumes the Lean repository lives next to this dataset repository and that the downloader has already populated `Lean/Data/alternative/sec-fails-to-deliver`.
