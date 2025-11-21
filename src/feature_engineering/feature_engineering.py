import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
import logging
from typing import Dict, List, Optional, Union

project_root = os.path.abspath("..")
if project_root not in sys.path:
    sys.path.append(project_root)

from src.ingestion.ingestion import load_all_data


class DataProcessor:
    def __init__(self):
        self.dfs: Dict[str, pd.DataFrame] = {}
        self._load_data() 

    def _load_data(self):
        try:
            self.dfs = load_all_data(["customers_clean", "support_interactions_clean", "transactions_clean"], "2_clean")
        except Exception as e:
            logging.error(f"Error loading initial data: {e}")
            raise # Re-raise or handle gracefully

    def calculate_trends(self, group):
        if len(group) > 1:
            return pd.Series({
                'usage_trend': np.polyfit(range(len(group)), group['data_usage_gb'], 1)[0],
                'spend_trend': np.polyfit(range(len(group)), group['amount_paid'], 1)[0],
                'usage_volatility': group['data_usage_gb'].std(),
                'spend_volatility': group['amount_paid'].std()
            })
        return pd.Series({
            'usage_trend': 0, 'spend_trend': 0, 
            'usage_volatility': 0, 'spend_volatility': 0
        })
    
    def recent_vs_historical(self, group):
        if len(group) >= 3:
            recent = group.nlargest(3, 'date')
            historical = group.nsmallest(len(group)-3, 'date')
            return pd.Series({
                'recent_avg_spend': recent['amount_paid'].mean(),
                'historical_avg_spend': historical['amount_paid'].mean() if len(historical) > 0 else 0,
                'spend_change_pct': (recent['amount_paid'].mean() - historical['amount_paid'].mean()) / historical['amount_paid'].mean() if historical['amount_paid'].mean() > 0 else 0
            })
        return pd.Series({
            'recent_avg_spend': group['amount_paid'].mean(),
            'historical_avg_spend': group['amount_paid'].mean(),
            'spend_change_pct': 0
        })
    
    def process_transaction(self):
        self.dfs["transactions_clean"].date = pd.to_datetime(self.dfs["transactions_clean"].date)
        # Time-based aggregations
        transactions_agg = self.dfs["transactions_clean"].groupby("customer_id").agg({
            'date': ["count"],  # tenure
            'call_minutes': ['mean', 'std', 'sum', 'max'],
            'data_usage_gb': ['mean', 'std', 'sum', 'max'], 
            'sms_count': ['mean', 'std', 'sum', 'max'],
            'amount_paid': ['mean', 'std', 'sum', 'max']
        }).round(2)

        # Flatten column names
        transactions_agg.columns = ['_'.join(col).strip() for col in transactions_agg.columns.values]

        trends = self.dfs["transactions_clean"].groupby("customer_id").apply(self.calculate_trends)

        recent_behavior = self.dfs["transactions_clean"].groupby("customer_id").apply(self.recent_vs_historical)

        self.dfs["transactions_processed"] = transactions_agg.merge(trends, on='customer_id', how='left')\
                                 .merge(recent_behavior, on='customer_id', how='left').reset_index()
        
    def process_support(self):
        # Basic aggregation for support interactions
        support_agg = self.dfs["support_interactions_clean"].groupby("customer_id").agg({
            'ticket_id': 'count',                    # Number of support tickets
            'resolution_time_min': ['mean', 'sum'],  # Average and total resolution time
            'was_resolved': 'mean',                  # Resolution rate
            'issue_type': 'nunique'                  # Number of unique issue types
        }).round(2)

        # Flatten column names
        support_agg.columns = ['support_ticket_count', 'avg_resolution_time_min', 
                            'total_resolution_time_min', 'resolution_rate', 'unique_issue_types']
        support_agg = support_agg.reset_index()

        self.dfs["support_processed"] = support_agg

    def process_customers(self):
        self.dfs["customers_clean"].signup_date = pd.to_datetime(self.dfs["customers_clean"].signup_date)
        self.dfs["customers_clean"]['signup_year_month'] = self.dfs["customers_clean"]['signup_date'].dt.to_period('M')
        self.dfs["customers_clean"]['signup_year'] = self.dfs["customers_clean"]['signup_date'].dt.year
        self.dfs["customers_clean"]['signup_month'] = self.dfs["customers_clean"]['signup_date'].dt.month
        self.dfs["customers_processed"] = pd.get_dummies(self.dfs["customers_clean"], columns=["country", "gender", "plan_type"])

    def save_df(self, df_name: str, filename: Optional[str] = None) -> None:
        """Save dataframe to CSV.
        
        Args:
            df_name: Name of the dataframe to save
            filename: Optional filename (uses df_name if not provided)
        """
        if df_name not in self.dfs:
            raise KeyError(f"DataFrame '{df_name}' not found")
        
        output_path = Path(f"../data/3_processed/{(filename or df_name)}.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.dfs[df_name].to_csv(output_path, index=False)
        logging.info(f"Saved {df_name} to {output_path}")
    
    def pipeline(self):
        self.process_customers()
        self.process_support()
        self.process_transaction()
        self.dfs["combined"] = self.dfs["customers_processed"].merge(
                    self.dfs["transactions_processed"].assign(customer_id=lambda x: x["customer_id"].astype(str)),
                        on="customer_id",
                        how="left").merge(
                    self.dfs["support_processed"].assign(customer_id=lambda x: x["customer_id"].astype(str)),
                    on="customer_id",
                    how="left" 
                    )

    def plot_monthly_signups(self):
        # Group by the year-month period
        monthly_signups = self.dfs["customers_clean"].groupby('signup_year_month')['customer_id'].count().reset_index()
        monthly_signups = monthly_signups.rename(columns={'customer_id': 'signup_count'})

        # Convert period to timestamp for plotting
        monthly_signups['signup_date'] = monthly_signups['signup_year_month'].dt.to_timestamp()

        plt.figure(figsize=(14, 7))
        plt.plot(monthly_signups['signup_date'], monthly_signups['signup_count'], 
                marker='o', linewidth=2, markersize=6, label='Monthly Signups')

        plt.title('Monthly Customer Signups Timeline', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('Number of Signups')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

    def plot_cohorts(self):
        # Create cohort analysis - number of customers by cohort and tenure
        cohort_data = self.dfs["combined"].groupby(['signup_year_month', 'date_count']).agg({
            'customer_id': 'count'
        }).reset_index()

        # Pivot to create cohort matrix
        cohort_pivot = cohort_data.pivot_table(
            index='signup_year_month', 
            columns='date_count', 
            values='customer_id',
            fill_value=0)
        
        # For a different perspective - cohort heatmap
        plt.figure(figsize=(14, 10))
        sns.heatmap(
            cohort_pivot, 
            annot=True, 
            fmt='g', 
            cmap='YlGnBu',
            cbar_kws={'label': 'Number of Customers'}
        )
        plt.title('Customer Cohort Analysis - Retention Heatmap')
        plt.xlabel('Tenure (Months)')
        plt.ylabel('Cohort Month')
        plt.tight_layout()
        plt.show()
                

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    processor = DataProcessor()
    processor.pipeline()
    for df in ["combined", "support_processed", "customers_processed", "transactions_processed"]:
            processor.save_df(df)
    processor.plot_monthly_signups()
    processor.plot_cohorts()
