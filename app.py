"""
Streamlit App for Egg Count Production Data Fetching
Run with: streamlit run app_count_production.py
"""

import streamlit as st
import pandas as pd
import pyodbc
from pathlib import Path
from datetime import datetime, timedelta
import time
import io

# Page config
st.set_page_config(
    page_title="Egg Count Production Data Fetcher",
    page_icon="🥚",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
    }
    </style>
""", unsafe_allow_html=True)


def fetch_data(account_id: int, start_date: str, end_date: str, progress_placeholder) -> pd.DataFrame:
    """
    Fetch egg count production data for one account and date range from SQL.
    Returns a flattened DataFrame with all measurement columns.
    """
    
    progress_placeholder.info("🔄 Connecting to SQL Server...")
    
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=saw-spf-prod-weu-ondemand.sql.azuresynapse.net;"
        "DATABASE=PowerBiViewsDB;"
        "Authentication=ActiveDirectoryInteractive;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=60;"
    )

    conn = pyodbc.connect(conn_str)

    query = f"""
    SELECT 
        AccountId,
        UploadLocalDateTime,
        BestLocalDateTime,
        BestUtcDateTime,
        LEFT(UploadLocalDateTime, 10) AS date,
        -- Extract all measurements from JSON
        Eggscounted,
        Eggscountedleft,
        Eggscountedright,
        Eggsincrease,
        Actualcapacity,
        Totalcapacity,
        Totalsetpoint,
        Speed,
        Stopscounted,
        Runningtime,
        Clock,
        LineNmbr,
        DistanceDone,
        DistanceDonePercent,
        EggsIncreaseLeft,
        EggsIncreaseRight,
        LineSetpoint,
        LocalDateTimeOffset
    FROM OPENROWSET(
        PROVIDER = 'CosmosDB',
        CONNECTION = 'Account=cs-meggsiusarchive-prod-westeurope;Database=VencomaticIOT',
        OBJECT = 'MeggsiusCountProductionData',
        SERVER_CREDENTIAL = 'cs-meggsiusarchive-prod-westeurope'
    ) WITH (
        AccountId               int         '$.AccountId.num',
        UploadLocalDateTime     varchar(50) '$.UploadLocalDateTime.string',
        BestLocalDateTime       varchar(50) '$.BestLocalDateTime.string',
        BestUtcDateTime         varchar(50) '$.BestUtcDateTime.string',
        Eggscounted             varchar(50) '$.Measurements.object.Eggscounted.string',
        Eggscountedleft         varchar(50) '$.Measurements.object.Eggscountedleft.string',
        Eggscountedright        varchar(50) '$.Measurements.object.Eggscountedright.string',
        Eggsincrease            varchar(50) '$.Measurements.object.Eggsincrease.string',
        Actualcapacity          varchar(50) '$.Measurements.object.Actualcapacity.string',
        Totalcapacity           varchar(50) '$.Measurements.object.Totalcapacity.string',
        Totalsetpoint           varchar(50) '$.Measurements.object.Totalsetpoint.string',
        Speed                   varchar(50) '$.Measurements.object.Speed.string',
        Stopscounted            varchar(50) '$.Measurements.object.Stopscounted.string',
        Runningtime             varchar(50) '$.Measurements.object.Runningtime.string',
        Clock                   varchar(50) '$.Measurements.object.Clock.string',
        LineNmbr                varchar(50) '$.Measurements.object.LineNmbr.string',
        DistanceDone            varchar(50) '$.Measurements.object.DistanceDone.string',
        DistanceDonePercent     varchar(50) '$.Measurements.object.DistanceDonePercent.string',
        EggsIncreaseLeft        varchar(50) '$.Measurements.object.EggsIncreaseLeft.string',
        EggsIncreaseRight       varchar(50) '$.Measurements.object.EggsIncreaseRight.string',
        LineSetpoint            varchar(50) '$.Measurements.object.LineSetpoint.string',
        LocalDateTimeOffset     varchar(50) '$.Measurements.object.LocalDateTimeOffset.string'
    ) AS meas
    WHERE AccountId = {account_id}
      AND LEFT(UploadLocalDateTime, 10) >= '{start_date}'
      AND LEFT(UploadLocalDateTime, 10) <= '{end_date}'
    ORDER BY UploadLocalDateTime ASC;
    """

    progress_placeholder.info("⏳ Running SQL query (this may take 1-2 minutes)...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    progress_placeholder.success(f"✅ Fetched {len(df):,} rows from database.")

    # Convert datetime columns
    df["UploadLocalDateTime"] = pd.to_datetime(df["UploadLocalDateTime"])
    df["BestLocalDateTime"] = pd.to_datetime(df["BestLocalDateTime"])
    df["BestUtcDateTime"] = pd.to_datetime(df["BestUtcDateTime"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    # Convert numeric columns
    numeric_columns = [
        'Eggscounted', 'Eggscountedleft', 'Eggscountedright', 'Eggsincrease',
        'Actualcapacity', 'Totalcapacity', 'Totalsetpoint', 'Speed',
        'Stopscounted', 'Runningtime', 'Clock', 'LineNmbr',
        'DistanceDone', 'DistanceDonePercent', 'EggsIncreaseLeft',
        'EggsIncreaseRight', 'LineSetpoint'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def get_file_size_mb(df: pd.DataFrame) -> float:
    """Estimate DataFrame size in MB."""
    return df.memory_usage(deep=True).sum() / (1024 * 1024)


def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to CSV bytes."""
    return df.to_csv(index=False).encode('utf-8')


def convert_df_to_parquet(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Parquet bytes."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer.getvalue()


# Main App
def main():
    st.markdown('<h1 class="main-header">🥚 Egg Count Production Data Fetcher</h1>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    Fetch egg count production data from the database and download in your preferred format.
    """)
    
    # Sidebar inputs
    st.sidebar.header("⚙️ Data Fetch Settings")
    
    account_id = st.sidebar.number_input(
        "Account ID (Farm ID)",
        min_value=1,
        value=76,
        step=1,
        help="Enter the farm account ID"
    )
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=30),
            help="Start date for data fetch"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now().date(),
            help="End date for data fetch"
        )
    
    # Validate dates
    if start_date >= end_date:
        st.sidebar.error("❌ Start date must be before end date!")
        return
    
    # Calculate expected duration
    days_diff = (end_date - start_date).days
    st.sidebar.info(f"📅 Fetching {days_diff} days of data")
    
    # Fetch button
    fetch_button = st.sidebar.button("🔄 Fetch Data", type="primary", use_container_width=True)
    
    # Session state to store data
    if 'df_prod' not in st.session_state:
        st.session_state.df_prod = None
        st.session_state.account_id = None
        st.session_state.start_date = None
        st.session_state.end_date = None
    
    # Fetch data
    if fetch_button:
        progress_placeholder = st.empty()
    
        try:
            # Fetch data
            start_time = time.time()
            df = fetch_data(account_id, str(start_date), str(end_date), progress_placeholder)
            fetch_time = time.time() - start_time
        
            progress_placeholder.empty()
        
            if len(df) == 0:
                st.error("❌ No data found for the specified parameters!")
                return
        
            # Store in session state
            st.session_state.df_prod = df
            st.session_state.account_id = account_id
            st.session_state.start_date = str(start_date)
            st.session_state.end_date = str(end_date)
        
            st.success(f"✅ Successfully fetched {len(df):,} rows in {fetch_time:.1f} seconds!")
        
        except Exception as e:
            progress_placeholder.empty()
            st.error(f"❌ Error fetching data: {str(e)}")
            st.info("💡 Tip: If timeout occurs, try a smaller date range")
            return
    
    # Display data and download options
    if st.session_state.df_prod is not None:
        df = st.session_state.df_prod
        account_id = st.session_state.account_id
        start_date = st.session_state.start_date
        end_date = st.session_state.end_date
        
        st.markdown("---")
        st.markdown("## 📊 Data Summary")
        
        # Calculate missing data statistics
        missing_counts = df.isnull().sum()
        total_missing = missing_counts.sum()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Rows", f"{len(df):,}")
        
        with col2:
            st.metric("Total Columns", len(df.columns))
        
        with col3:
            rows_with_missing = df.isnull().any(axis=1).sum()
            st.metric("Rows with Missing Values", f"{rows_with_missing:,}")
        
        with col4:
            file_size = get_file_size_mb(df)
            st.metric("Data Size", f"{file_size:.1f} MB")
        
        # Missing data details
        if total_missing > 0:
            st.markdown("---")
            st.markdown("### ⚠️ Missing Data Details")
            
            missing_df = pd.DataFrame({
                'Column': missing_counts.index,
                'Missing Count': missing_counts.values,
                'Missing %': (missing_counts.values / len(df) * 100).round(2)
            })
            missing_df = missing_df[missing_df['Missing Count'] > 0].sort_values('Missing Count', ascending=False)
            
            if len(missing_df) > 0:
                st.dataframe(missing_df, use_container_width=True)
            
            st.info(f"💡 {rows_with_missing:,} rows ({(rows_with_missing/len(df)*100):.1f}%) contain at least one missing value")
        else:
            st.success("✅ No missing values found in the dataset!")
        
        # Data preview
        st.markdown("---")
        st.markdown("## 📋 Data Preview")
        
        # Show first few rows
        st.markdown("**First 10 Rows:**")
        st.dataframe(df.head(10), use_container_width=True)
        
        # Column information
        with st.expander("📊 View Column Information"):
            col_info = pd.DataFrame({
                'Column': df.columns,
                'Type': df.dtypes.astype(str),
                'Non-Null Count': df.count(),
                'Null Count': df.isnull().sum(),
                'Sample Value': [str(df[col].iloc[0]) if len(df) > 0 else None for col in df.columns]
            })
            st.dataframe(col_info, use_container_width=True)
        
        # Download options
        st.markdown("---")
        st.markdown("## 💾 Download Options")
        
        # Create filename base
        filename_base = f"count_prod_{account_id}_{start_date}_{end_date}"
        
        # Prepare datasets
        df_complete = df
        df_clean = df.dropna()
        
        st.markdown(f"**Complete Dataset:** {len(df_complete):,} rows")
        st.markdown(f"**Clean Dataset (no missing values):** {len(df_clean):,} rows")
        
        if len(df_clean) == 0:
            st.warning("⚠️ Clean dataset is empty - all rows contain at least one missing value")
        
        st.markdown("---")
        
        # Download buttons in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 📦 Complete Dataset (All Data)")
            st.caption(f"{len(df_complete):,} rows • {len(df_complete.columns)} columns")
            
            # CSV download
            csv_complete = convert_df_to_csv(df_complete)
            st.download_button(
                label="📥 Download as CSV",
                data=csv_complete,
                file_name=f"{filename_base}_complete.csv",
                mime="text/csv",
                use_container_width=True,
                key="csv_complete"
            )
            
            # Parquet download
            parquet_complete = convert_df_to_parquet(df_complete)
            st.download_button(
                label="📥 Download as Parquet",
                data=parquet_complete,
                file_name=f"{filename_base}_complete.parquet",
                mime="application/octet-stream",
                use_container_width=True,
                key="parquet_complete"
            )
            
            st.info(f"💡 CSV: ~{file_size:.1f} MB\n\nParquet: ~{file_size * 0.3:.1f} MB (compressed)")
        
        with col2:
            st.markdown("### ✨ Clean Dataset (No Missing Values)")
            st.caption(f"{len(df_clean):,} rows • {len(df_clean.columns)} columns")
            
            if len(df_clean) > 0:
                # CSV download
                csv_clean = convert_df_to_csv(df_clean)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv_clean,
                    file_name=f"{filename_base}_clean.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="csv_clean"
                )
                
                # Parquet download
                parquet_clean = convert_df_to_parquet(df_clean)
                st.download_button(
                    label="📥 Download as Parquet",
                    data=parquet_clean,
                    file_name=f"{filename_base}_clean.parquet",
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="parquet_clean"
                )
                
                clean_file_size = get_file_size_mb(df_clean)
                st.info(f"💡 CSV: ~{clean_file_size:.1f} MB\n\nParquet: ~{clean_file_size * 0.3:.1f} MB (compressed)")
            else:
                st.warning("⚠️ No complete rows available")
                st.info("All rows contain at least one missing value")
        
        # Additional info
        st.markdown("---")
        st.markdown("### 📖 Download Format Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**CSV Format**")
            st.write("• Human-readable text format")
            st.write("• Compatible with Excel, pandas, etc.")
            st.write("• Larger file size")
            st.write("• Best for: Quick viewing, sharing")
        
        with col2:
            st.markdown("**Parquet Format**")
            st.write("• Binary columnar format")
            st.write("• Efficient compression (~70% smaller)")
            st.write("• Fast read/write operations")
            st.write("• Best for: Large datasets, analysis")
    
    else:
        # Welcome screen
        st.markdown("---")
        st.info("👈 Enter your parameters in the sidebar and click **Fetch Data** to begin!")
        
        st.markdown("""
        ### 📖 How to Use
        
        1. **Enter Account ID**: Your farm's account identifier
        2. **Select Date Range**: Choose start and end dates
        3. **Click Fetch Data**: Data will be downloaded from the database
        4. **Review Summary**: Check data statistics and missing values
        5. **Download**: Choose your preferred format and dataset version
        
        ### 📊 Available Data Columns
        
        **Timestamps:**
        - UploadLocalDateTime, BestLocalDateTime, BestUtcDateTime, date
        
        **Egg Counts:**
        - Eggscounted, Eggscountedleft, Eggscountedright
        - Eggsincrease, EggsIncreaseLeft, EggsIncreaseRight
        
        **Production Metrics:**
        - Actualcapacity, Totalcapacity, Totalsetpoint
        - LineNmbr, LineSetpoint
        - Speed, Runningtime, Clock
        
        **Operational Data:**
        - Stopscounted, DistanceDone, DistanceDonePercent
        - LocalDateTimeOffset
        
        ### 💾 Download Options
        
        **Complete Dataset:** All fetched data including rows with missing values
        
        **Clean Dataset:** Only rows with no missing values (all columns populated)
        
        **Formats:**
        - **CSV**: Easy to view and share, larger file size
        - **Parquet**: Compressed format, smaller size, faster for analysis
        
        ### 💡 Tips
        
        - Start with smaller date ranges (7-30 days) for faster queries
        - For large datasets (>50MB), use Parquet format
        - Check missing data statistics before downloading
        - Clean dataset ensures all columns have values
        """)


if __name__ == "__main__":
    main()