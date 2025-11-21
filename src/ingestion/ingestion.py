import pandas as pd
from typing import Dict
from pathlib import Path

# Determine project root based on this file's location
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # goes up from src/ingestion/
RAW_DATA_DIR = PROJECT_ROOT / "data" 

def data_reader(file: str, folder: str = "1_raw") -> pd.DataFrame:
    file_path = RAW_DATA_DIR / folder / f"{file}.csv"
    return pd.read_csv(file_path)

def load_all_data(datasets = ["customers", "support_interactions", "transactions"], folder: str = "1_raw") -> Dict[str, pd.DataFrame]:
    return {name: data_reader(name, folder) for name in datasets}
