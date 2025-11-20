import pandas as pd
from typing import Dict

def data_reader(file: str) -> pd.DataFrame:
    return pd.read_csv(f"../data/1_raw/{file}.csv")

def load_all_data() -> Dict[str, pd.DataFrame]:
    datasets = ["customers", "support_interactions", "transactions"]
    return {name: data_reader(name) for name in datasets}
