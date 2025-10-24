from paths import create_paths
from extract import extract
from clean import convert_to_csv
from master import build_master

create_paths()
extract()
converted = convert_to_csv()
total = build_master()
print(f"converted files: {converted}, master rows: {total}")