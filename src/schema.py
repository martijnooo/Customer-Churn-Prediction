import pandera as pa
from pandera import Column, Check
import pandas as pd

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
            checks=Check.isin(["DE", "FR", "ES", "IT", "NL"])
        ),
        "age": Column(
            int,
            nullable=True,
            checks=Check.ge(0)
        ),
        "gender": Column(
            str,
            nullable=True,
            checks=Check.isin(["M", "F", "X"])
        ),
        "plan_type": Column(
            str,
            nullable=True,
            checks=Check.isin(["basic", "premium", "gold"])
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
            checks=Check.ge(0)  # assume refunds handled separately
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
            checks=Check.isin(["email", "phone", "chat", "whatsapp"])
        ),
        "issue_type": Column(
            str,
            nullable=True,
            checks=Check.isin(["billing", "technical", "general"])
        ),
        "resolution_time_min": Column(
            int,
            nullable=True,
            checks=Check.ge(0)
        ),
        "was_resolved": Column(
            bool,
            nullable=True
        ),
    },
    strict=True
)

df = pd.read_csv(r"C:\Programming\Ironhack\projects\Customer-Churn-Prediction\data\1_raw\customers.csv")

validated_df = customers_schema.validate(df)
print(validated_df)