import streamlit as st
import redis
import json
import pandas as pd
import time
from datetime import datetime

# --- CONFIGURATION ---
# Localhost because you are connecting from your host machine (outside Docker)
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REFRESH_RATE = 1  # Page refresh rate (seconds)

# --- PAGE LAYOUT ---
st.set_page_config(
    page_title="FinGuard | Real-Time Fraud Monitor",
    page_icon="🛡️",
    layout="wide"
)

# Header
st.title("🛡️ FinGuard: Real-Time Fraud Monitoring Panel")
st.markdown("Suspicious transactions detected via **Kafka** and **Spark Streaming** are displayed here in real-time.")


# --- REDIS CONNECTION ---
@st.cache_resource
def get_redis_connection():
    try:
        # decode_responses=True ensures we get strings instead of bytes
        return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    except Exception as e:
        st.error(f"Redis Connection Error: {e}")
        return None


r = get_redis_connection()

# --- MAIN LOOP (DATA STREAM) ---
# Create a placeholder to update the UI dynamically
placeholder = st.empty()

while True:
    with placeholder.container():
        # 1. Fetch all fraud keys from Redis
        try:
            keys = r.keys("fraud_alert:*")
        except:
            st.warning("Cannot reach Redis. Is Docker running?")
            keys = []

        data = []
        if keys:
            # Read JSON data for each key
            for key in keys:
                value = r.get(key)
                if value:
                    parsed_value = json.loads(value)
                    data.append(parsed_value)

            # Convert to Pandas DataFrame
            df = pd.DataFrame(data)

            # Sort by timestamp (Newest first)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values(by='timestamp', ascending=False)

            # --- METRICS (KPIs) ---
            kpi1, kpi2, kpi3 = st.columns(3)

            # KPI 1: Total Detected Cases
            kpi1.metric(
                label="Detected Cases",
                value=len(df),
                delta=f"+{len(df)} Txns"
            )

            # KPI 2: Total Blocked Amount
            total_amount = df['amount'].sum()
            kpi2.metric(
                label="Total Blocked Amount",
                value=f"${total_amount:,.2f}",
                delta="High Risk Flow",
                delta_color="inverse"
            )

            # KPI 3: Last Alert Location
            last_city = df.iloc[0]['city'] if not df.empty else "-"
            kpi3.metric(
                label="Last Alert Zone",
                value=last_city
            )

            st.divider()

            # --- TABLE VIEW ---
            st.subheader("Recent Suspicious Transactions")

            # Select and format columns for better display
            if not df.empty:
                display_df = df[['timestamp', 'card_id', 'amount', 'city', 'merchant', 'is_fraud_simulation']]

                # Highlight rows and format currency
                st.dataframe(
                    display_df.style.format({"amount": "${:.2f}"})
                    .applymap(lambda x: 'background-color: #ffcccc' if x else '', subset=['is_fraud_simulation']),
                    use_container_width=True
                )
            else:
                st.info("No suspicious transactions detected yet. System is monitoring...")

        else:
            # Empty state (No data in Redis)
            st.info("Waiting for data stream... (Is the Producer running?)")

            col1, col2 = st.columns(2)
            col1.metric("Detected Cases", "0")
            col2.metric("Blocked Amount", "$0.00")

        # Wait before the next update
        time.sleep(REFRESH_RATE)