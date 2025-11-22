# Fortnite_ETL

A Python-based ETL (Extract, Transform, Load) pipeline for retrieving Fortnite player statistics from the Fortnite API, processing the data, and storing it in an Azure SQL database.

---

## Features

- **Data Extraction**: Fetches player statistics from the Fortnite API.
- **Data Transformation**: Cleans and processes the data for consistency and accuracy.
- **Data Loading**: Uploads the processed data to an Azure SQL database.
- **Automated Workflows**: Uses GitHub Actions to test and maintain the pipeline.

---

## Repository Structure

```
Fortnite_ETL/
├── __main__.py               # Entry point for the ETL pipeline
├── README.md                 # Project documentation
├── requirements.txt          # Python dependencies
├── .github/
│   └── workflows/
│       └── action_flow.yml   # GitHub Actions workflow for automation
└── src/
    ├── __init__.py           # Package initialization
    ├── azure_db.py           # Azure SQL database connection and operations
    ├── fortnite_api.py       # Functions for interacting with the Fortnite API
```

---

## Prerequisites

Before running the project, ensure you have the following:

1. **Python 3.8+** installed on your system.
2. **Azure SQL Database** configured and accessible.
3. **Fortnite API Key** for accessing player statistics.
4. **Environment Variables**:
   - `HEADER_API_TYPE`
   - `HEADER_API_VALUE`
   - `DB_SERVER`
   - `DB_DATABASE`
   - `DB_UID`
   - `DB_PWD`

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Seb70197/Fortnite_ETL.git
   cd Fortnite_ETL
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   - Create a `.env` file in the root directory and add the required variables:
     ```
     HEADER_API_TYPE=your_api_type
     HEADER_API_VALUE=your_api_value
     DB_SERVER=your_db_server
     DB_DATABASE=your_db_name
     DB_UID=your_db_username
     DB_PWD=your_db_password
     ```

---

## Usage

1. Run the ETL pipeline:
   ```bash
   python __main__.py
   ```

2. The pipeline will:
   - Connect to the Azure SQL database.
   - Retrieve existing player data.
   - Fetch the latest player statistics from the Fortnite API.
   - Clean and process the data.
   - Upload the data to the database.

---

## Automation with GitHub Actions

This repository includes a GitHub Actions workflow (`.github/workflows/action_flow.yml`) to automate testing and maintenance tasks.

- **Scheduled Runs**: The workflow is triggered daily at 7:00 AM UTC.
- **Testing**: Ensures the ETL pipeline runs successfully in a controlled environment.

---

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a detailed description of your changes.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contact

For questions or support, please contact **Seb70197** via GitHub.

---

## Acknowledgments

- [Fortnite API](https://fortniteapi.com/) for providing player statistics.
- [Azure SQL Database](https://azure.microsoft.com/en-us/services/sql-database/) for data storage.
- [GitHub Actions](https://github.com/features/actions) for workflow automation.