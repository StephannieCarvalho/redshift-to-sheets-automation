# redshift-to-sheets-automation

This project automates the **extraction of data from Redshift** and sends the results to a specific tab in a **Google Sheets spreadsheet**. Ideal for daily or scheduled routines, ensuring your spreadsheets are always up to date.

---

# ✨ Why This Project?

The main goal of this automation is to **facilitate the enrichment of quantitative data with qualitative insights** that come directly from customers. By keeping Redshift data automatically updated in Google Sheets, the team can focus on analysis and manual enrichment, without worrying about extraction.

---

# 🛠️ Technologies Used

* **Python 3.9+**: Main programming language.
* **Google Sheets API**: For interacting with Google spreadsheets.
* **Google Service Account (JSON)**: Secure authentication method to access the API without manual intervention.
* **PostgreSQL / Redshift Connector (`psycopg2-binary`)**: For connecting and executing queries on Redshift.
* **`pandas`**: Library for data manipulation and organization.
* **`python-dotenv`**: For managing environment variables (sensitive credentials).
* **`gspread`**: Python library to interface with Google Sheets API.
* **`oauth2client`**: Library for authenticating with the Google service account.

---

# 📦 Installation & Setup

To configure and run this project, follow the steps below:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-account/redshift-to-sheets.git
    cd redshift-to-sheets
    ```

2.  **Create and activate a Virtual Environment (Venv):**
    It is highly recommended to use a virtual environment to manage project dependencies in isolation.
    ```bash
    python -m venv venv
    .\venv\Scripts\activate   # On Windows
    source venv/bin/activate  # On macOS/Linux
    ```

3.  **Install dependencies:**
    With the virtual environment activated, install all required libraries:
    ```bash
    pip install psycopg2-binary pandas gspread python-dotenv oauth2client
    ```

4.  **Configure Google Sheets Credentials:**
    * Go to Google Cloud Console and create a new project or use an existing one.
    * Enable the "Google Sheets API" and "Google Drive API".
    * Create a "Service Account".
    * Generate a **JSON key** for this service account.
    * **Rename the downloaded JSON file** to `mimo-prod-env-b31af693e832.json` and place it in the **root of your project**.
    * **Share your Google Sheets file** with the service account email (the `client_email` inside the JSON file). Make sure the service account has **Editor** permission on the spreadsheet.

5.  **Set Up Environment Variables (.env):**
    Create a file named `.env` in the **root of your project** (in the same folder as `export_redshift_to_sheets.py`) with the following content, replacing with your Redshift credentials:
    ```dotenv
    REDSHIFT_HOST=your_redshift_host
    REDSHIFT_PORT=5439
    REDSHIFT_DB=your_database
    REDSHIFT_USER=your_redshift_user
    REDSHIFT_PASSWORD=your_redshift_password
    ```
    *Remember: The `.env` file should never be committed to Git as it contains sensitive information!*

---

# ⚙️ How to Use

With the environment configured, run the main script:

1.  **Activate the virtual environment** (if not already activated).
2.  **Run the script:**
    ```bash
    python export_redshift_to_sheets.py
    ```
    The script will connect to Redshift, extract the data, and send it to the `dados_redshift` tab in the `Mimo Live | Business Intelligence` spreadsheet. The previous content of the tab will be cleared before the update.

---

# ⚠️ Notes & Error Handling

* The script was developed to handle `NaN` (Not a Number) values from Redshift by converting them into empty strings before uploading to Google Sheets.
* In case of errors in connecting to Redshift, authenticating with Google Sheets, or uploading data, informative messages will be displayed in the console.
* For large volumes of data (millions of cells), consider additional optimizations or using `gspread-pandas` for better performance.

---

# 🤝 Contributing

Contributions are welcome! If you find bugs, have suggestions for improvement, or want to add new features, feel free to open an *issue* or submit a *pull request*.

---
