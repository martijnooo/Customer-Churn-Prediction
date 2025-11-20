import sys
from pathlib import Path
import pandas as pd
import logging
from typing import Dict, List, Optional, Union

# Add the project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.ingestion.ingestion import load_all_data
from src.schema import ALLOWED_GENDERS, ALLOWED_COUNTRIES, ALLOWED_PLANS, ALLOWED_CHANNELS, ALLOWED_ISSUE_TYPES, customers_schema, support_schema, transactions_schema

class DataCleaner:
    def __init__(self):
        """Initialize DataCleaner with data.
        
        Args:
            data_path: Optional path to load data from. If None, uses load_all_data()
        """
        try:
            self.dfs = load_all_data() 
            self.cleaning_log = []
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            raise
    
    
    def drop_duplicates(self, df_name: str, subset: Union[str, List[str]], keep: str = 'first') -> None:
        """Drop duplicate rows based on specified columns.
        
        Args:
            df_name: Name of the dataframe in self.dfs
            subset: Column name or list of column names to check for duplicates
            keep: Which duplicates to keep ('first', 'last', False)
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        initial_rows = len(self.dfs[df_name])
        self.dfs[df_name] = self.dfs[df_name].drop_duplicates(subset=subset, keep=keep)
        removed_rows = initial_rows - len(self.dfs[df_name])
        
        self.cleaning_log.append(f"Removed {removed_rows} duplicates from {df_name}")
        logging.info(f"Removed {removed_rows} duplicate rows from {df_name}")
        return self
    
    def convert_date(self, df_name: str, column: str, format: str = "mixed") -> None:
        """Convert column to datetime.
        
        Args:
            df_name: Name of the dataframe
            column: Column to convert
            format: DateTime format string
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        if column not in self.dfs[df_name].columns:
            raise KeyError(f"Column '{column}' not found in {df_name}")
        
        self.dfs[df_name][column] = pd.to_datetime(
            self.dfs[df_name][column], 
            format=format,
            errors='coerce'  # Convert parsing errors to NaT
        )
        self.cleaning_log.append(f"Converted {column} to datetime in {df_name}")
        return self
    
    def filter_categorical(self, df_name: str, column_to_allowed_list: Dict[str, List]) -> None:
        """Filter categorical columns to allowed values.
        
        Args:
            df_name: Name of the dataframe
            column_to_allowed_list: Dict mapping column names to allowed values
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        for column_name, allowed_list in column_to_allowed_list.items():
            if column_name not in self.dfs[df_name].columns:
                logging.warning(f"Column '{column_name}' not found in {df_name}")
                continue
            
            initial_count = len(self.dfs[df_name])
            mask = self.dfs[df_name][column_name].isin(allowed_list)
            invalid_count = (~mask).sum()
            
            self.dfs[df_name].loc[~mask, column_name] = pd.NA
            
            self.cleaning_log.append(
                f"Filtered {invalid_count} invalid values from {column_name} in {df_name}"
            )
        return self
    
    def convert_to_int(self, df_name: str, column: str, fill_na: Optional[int] = None) -> None:
        """Convert column to integer type.
        
        Args:
            df_name: Name of the dataframe
            column: Column to convert
            fill_na: Value to fill NaN values with before conversion
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        if fill_na is not None:
            self.dfs[df_name][column] = self.dfs[df_name][column].fillna(fill_na)

        self.dfs[df_name][column] = self.dfs[df_name][column].convert_dtypes(int)

        self.cleaning_log.append(f"Converted {column} to integer in {df_name}")
        return self
    
    def remove_rows(self, df_name: str, condition: str) -> None:
        """Remove rows based on condition.
        
        Args:
            df_name: Name of the dataframe
            condition: Boolean condition string (e.g., "column == value")
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        initial_count = len(self.dfs[df_name])
        self.dfs[df_name] = self.dfs[df_name].query(f"not ({condition})")
        removed_count = initial_count - len(self.dfs[df_name])
        
        self.cleaning_log.append(f"Removed {removed_count} rows from {df_name} where {condition}")
        return self
    
    def remove_negatives(self, df_name: str, exclude_columns: List[str] = None) -> None:
        """Remove rows with negative values in numeric columns.
        
        Args:
            df_name: Name of the dataframe
            exclude_columns: Columns to exclude from negative value check
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        numeric_columns = self.dfs[df_name].select_dtypes(include='number').columns
        
        if exclude_columns:
            numeric_columns = numeric_columns.difference(exclude_columns)
        
        initial_count = len(self.dfs[df_name])
        
        for col in numeric_columns:
            self.dfs[df_name] = self.dfs[df_name][self.dfs[df_name][col] >= 0]
        
        removed_count = initial_count - len(self.dfs[df_name])
        self.cleaning_log.append(f"Removed {removed_count} rows with negative values from {df_name}")
        return self
    
    def validate_df(self, df_name: str, schema) -> bool:
        """Validate dataframe against schema.
        
        Args:
            df_name: Name of the dataframe
            schema: Schema validator
            
        Returns:
            bool: True if validation passed, False otherwise
        """
        try:
            schema.validate(self.dfs[df_name])
            self.cleaning_log.append(f"Validation passed for {df_name}")
            return True
        except Exception as e:
            logging.error(f"Validation failed for {df_name}: {e}")
            self.cleaning_log.append(f"Validation failed for {df_name}: {e}")
            return False
    
    def get_cleaning_summary(self) -> pd.DataFrame:
        """Get summary of cleaning operations."""
        return pd.DataFrame({
            'operation': self.cleaning_log,
            'timestamp': pd.Timestamp.now()
        })
    
    def save_df(self, df_name: str, filename: Optional[str] = None) -> None:
        """Save dataframe to CSV.
        
        Args:
            df_name: Name of the dataframe to save
            filename: Optional filename (uses df_name if not provided)
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        output_path = Path(f"../data/2_clean/{(filename or df_name)}.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.dfs[df_name].to_csv(output_path, index=False)
        logging.info(f"Saved {df_name} to {output_path}")


if __name__ == "__main__":
    # Initialize cleaner
    cleaner = DataCleaner()

    # Chain operations for each dataset
    (
        cleaner
        .drop_duplicates("customers", "customer_id")
        .convert_date("customers", "signup_date")
        .filter_categorical("customers", {
            "country": ALLOWED_COUNTRIES,
            "plan_type": ALLOWED_PLANS, 
            "gender": ALLOWED_GENDERS,
        })
        .convert_to_int("customers", "age")
    )

    (
        cleaner
        .convert_date("transactions", "date")
        .remove_rows("transactions", "customer_id == 'C999999'")
        .remove_negatives("transactions")
    )

    (
        cleaner
        .filter_categorical("support_interactions", {
            "channel": ALLOWED_CHANNELS,
            "issue_type": ALLOWED_ISSUE_TYPES,
        })
        .remove_negatives("support_interactions")
        .drop_duplicates("support_interactions", "ticket_id")
        .convert_date("support_interactions", "timestamp")
        .convert_to_int("support_interactions", "resolution_time_min")
        .convert_to_int("support_interactions", "was_resolved")
    )

    # Validate all datasets
    cleaner.validate_df("customers", customers_schema)
    cleaner.validate_df("transactions", transactions_schema) 
    cleaner.validate_df("support_interactions", support_schema)

    # Save all data
    cleaner.save_df("customers", "customers_clean")
    cleaner.save_df("transactions", "transactions_clean")
    cleaner.save_df("support_interactions", "support_interactions_clean")

    print(cleaner.get_cleaning_summary())