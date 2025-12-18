# Egg Count Production Data Fetcher

A Streamlit application for fetching and downloading egg count production data from the database.

## 🚀 Quick Start

### Prerequisites
- Windows 10/11
- Python 3.8 or higher ([Download here](https://www.python.org/downloads/))
- Access to company database (Azure AD credentials)
- Connected to company network/VPN

### Installation & Running

1. **Download this repository:**
   - Click the green "Code" button above
   - Select "Download ZIP"
   - Extract to your Desktop or Documents folder

2. **Open Command Prompt:**
   - Press `Windows + R`
   - Type: `cmd`
   - Press Enter

3. **Navigate to the folder:**
   ```cmd
   cd "C:\Users\YOUR_USERNAME\Desktop\bb-main"
   ```
   *(Replace with your actual folder path)*

4. **Install dependencies (first time only):**
   ```cmd
   pip install streamlit pandas pyodbc plotly pyarrow
   ```

5. **Run the application:**
   ```cmd
   streamlit run app.py
   ```

6. **Your browser will open automatically** at `http://localhost:8501`

7. **Azure Login:** A browser popup will appear - sign in with your company email and password

---

## 📖 How to Use

1. **Select Account ID** (Farm ID) from the sidebar
2. **Choose Date Range** (start and end dates)
3. **Click "Fetch Data"** button
4. Wait for data to load (1-2 minutes for large date ranges)
5. **Review the data summary** and missing value statistics
6. **Download your data:**
   - **Complete Dataset:** All data including rows with missing values
   - **Clean Dataset:** Only rows with no missing values
   - **Formats:** CSV (human-readable) or Parquet (compressed, 70% smaller)

---

## 📊 Available Data

### Timestamps
- `UploadLocalDateTime`, `BestLocalDateTime`, `BestUtcDateTime`, `date`

### Egg Counts
- `Eggscounted`, `Eggscountedleft`, `Eggscountedright`
- `Eggsincrease`, `EggsIncreaseLeft`, `EggsIncreaseRight`

### Production Metrics
- `Actualcapacity`, `Totalcapacity`, `Totalsetpoint`
- `LineNmbr`, `LineSetpoint`
- `Speed`, `Runningtime`, `Clock`

### Operational Data
- `Stopscounted`, `DistanceDone`, `DistanceDonePercent`
- `LocalDateTimeOffset`

---

## 💡 Tips

- **Start with smaller date ranges** (7-30 days) for faster queries
- **Use Parquet format** for large datasets (>50MB) - it's much smaller
- **Check missing data statistics** before downloading
- **Complete dataset** includes all data (some columns may be empty)
- **Clean dataset** includes only complete rows (all columns have values)

---

## 🔧 Troubleshooting

### "Python is not recognized"
- Install Python from [python.org](https://www.python.org/downloads/)
- During installation, check **"Add Python to PATH"**
- Restart Command Prompt after installation

### "pip is not recognized"
```cmd
python -m pip install streamlit pandas pyodbc plotly pyarrow
```

### "ODBC Driver not found"
The ODBC Driver 17 for SQL Server should already be installed on company computers. If not, contact IT support.

### "Login timeout" or "Authentication failed"
- Make sure you're connected to company network or VPN
- Try signing in again with your company credentials
- Contact IT if you can't access the database

### App won't start
- Make sure you installed the dependencies (step 4 above)
- Check that you're in the correct folder
- Try closing and reopening Command Prompt

### Browser doesn't open automatically
Manually open your browser and go to: `http://localhost:8501`

### To stop the app
- In the Command Prompt window, press `Ctrl + C`
- Or simply close the Command Prompt window

---

## 🖥️ System Requirements

- **OS:** Windows 10/11
- **Python:** 3.8 or higher
- **RAM:** 4GB minimum (8GB recommended for large datasets)
- **Storage:** 100MB for application + space for downloaded data
- **Network:** Company network access or VPN connection

---

## 📦 Dependencies

- `streamlit` - Web application framework
- `pandas` - Data manipulation
- `pyodbc` - Database connection
- `plotly` - (Not used but listed for compatibility)
- `pyarrow` - Parquet file support

---

## 🔐 Security & Privacy

- Uses Azure Active Directory authentication (your company credentials)
- No data is stored on external servers
- All data stays on your local computer
- Database connection is encrypted (TLS/SSL)

---

## 📝 Notes

- Your computer must remain running while using the app
- Data fetching may take 1-2 minutes for large date ranges
- The app runs locally on your computer (not in the cloud)
- Each user needs to run their own instance

---

## 🆘 Support

For issues or questions:
- Check the Troubleshooting section above
- Contact: IT Support / Data Team
- GitHub Issues: [Report a bug](https://github.com/siddharthganjoo/bb/issues)

---

## 📄 License

Internal use only - Vencomatic Group

---

## 🔄 Version History

**v1.0** - December 2025
- Initial release
- Fetch egg count production data
- Download as CSV or Parquet
- Complete and Clean dataset options