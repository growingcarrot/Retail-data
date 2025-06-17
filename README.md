This solution ingests data from Azure Blob Storage and loads it into a SQLite database.

## Prerequisites

- Python 3.12

- Required packages: see requirements.txt

## Setup

1. Clone the repository.

2. Create a virtual environment and activate it:

python -m venv venv

source venv/bin/activate (or venv\Scripts\activate on Windows)

3. Install dependencies:

pip install -r requirements.txt

4. Edit .env with your Azure credentials

5. Run the script:

You can choose to run it in auto mode (process previous date) or in manual mode (process the date of your choice)

- In auto mode: python retail_pipeline.py --auto
- In manual mode: python retail_pipeline.py --date YYYY-MM-DD
    ex: python retail_pipeline.py --date 2023-11-24

## Database

The database is stored in `retail_data.db` (SQLite). It can be queried using SQLite browser or the command line.
