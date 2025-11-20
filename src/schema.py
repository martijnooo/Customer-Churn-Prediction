import pandera.pandas as pa
from pandera import Column, Check
import pandas as pd

# ===============================================
# GLOBAL CONSTRAINT VARIABLES
# ===============================================

# Customers Schema Constraints
ALLOWED_COUNTRIES = ["DE", "FR", "ES", "IT", "NL"]
ALLOWED_GENDERS = ["M", "F", "X"]
ALLOWED_PLANS = ["basic", "premium", "gold"]

# Support Schema Constraints
ALLOWED_CHANNELS = ["email", "phone", "chat", "whatsapp"]
ALLOWED_ISSUE_TYPES = ["billing", "technical", "general"]

# ------------------------------
# Customers schema
# ------------------------------
customers_schema = pa.DataFrameSchema(
    columns={
        "customer_id": Column(
            str,
            nullable=False,
            unique=True,
        ),
        "signup_date": Column(
            pa.DateTime,
            nullable=False,
        ),
        "country": Column(
            str,
            nullable=True,
            # Using the variable
            checks=Check.isin(ALLOWED_COUNTRIES)
        ),
        "age": Column(
            int,
            nullable=True,
            checks=Check.ge(0)
        ),
        "gender": Column(
            str,
            nullable=True,
            # Using the variable
            checks=Check.isin(ALLOWED_GENDERS)
        ),
        "plan_type": Column(
            str,
            nullable=True,
            # Using the variable
            checks=Check.isin(ALLOWED_PLANS)
        ),
        "monthly_fee": Column(
            float,
            nullable=False,
            checks=Check.ge(0)
        ),
    },
    strict=True
)

# ------------------------------
# Transactions schema
# ------------------------------
transactions_schema = pa.DataFrameSchema(
    columns={
        "customer_id": Column(
            str,
            nullable=False,
        ),
        "date": Column(
            pa.DateTime,
            nullable=False,
        ),
        "call_minutes": Column(
            float,
            nullable=True,
            checks=Check.ge(0)
        ),
        "data_usage_gb": Column(
            float,
            nullable=True,
            checks=Check.ge(0)
        ),
        "sms_count": Column(
            int,
            nullable=True,
            checks=Check.ge(0)
        ),
        "amount_paid": Column(
            float,
            nullable=True,
            checks=Check.ge(0)
        ),
    },
    strict=True
)

# ------------------------------
# Support interactions schema
# ------------------------------
support_schema = pa.DataFrameSchema(
    columns={
        "ticket_id": Column(
            str,
            nullable=False,
            unique=True
        ),
        "customer_id": Column(
            str,
            nullable=True
        ),
        "timestamp": Column(
            pa.DateTime,
            nullable=False
        ),
        "channel": Column(
            str,
            nullable=True,
            # Using the variable
            checks=Check.isin(ALLOWED_CHANNELS)
        ),
        "issue_type": Column(
            str,
            nullable=True,
            # Using the variable
            checks=Check.isin(ALLOWED_ISSUE_TYPES)
        ),
        "resolution_time_min": Column(
            int,
            nullable=True,
            checks=Check.ge(0)
        ),
        "was_resolved": Column(
            int,
            nullable=True
        ),
    },
    strict=True
)

if __name__ == "__main__":
    df = pd.read_csv(r"C:\Programming\Ironhack\projects\Customer-Churn-Prediction\data\1_raw\customers.csv")

    validated_df = customers_schema.validate(df)
    print(validated_df)