import pandas as pd
from typing import Dict
from pathlib import Path

# Determine project root based on this file's location
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # goes up from src/ingestion/
RAW_DATA_DIR = PROJECT_ROOT / "data" / "1_raw"

def data_reader(file: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / f"{file}.csv"
    return pd.read_csv(file_path)

def load_all_data() -> Dict[str, pd.DataFrame]:
    datasets = ["customers", "support_interactions", "transactions"]
    return {name: data_reader(name) for name in datasets}
