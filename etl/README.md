# ETL Package for Mice Protein Expression

## Quick Start

# 1. Install dependencies:
```bash
poetry install

# 2 Set environment variables
$env:DB_HOST="your_host"
$env:DB_PORT="your_port"
$env:DB_NAME="your_name"
$env:DB_USER="your_user"
$env:DB_PASSWORD="your_password"

#3 Run ETL pipeline:
python etl/main.py --source "https://drive.google.com/uc?id=1m7QX3GeQMWw2Ni4EGKnte37ikID7Z5Gj"

#4 Project Structure
etl/ - ETL package
data/raw/ - raw data (auto-created)
data/processed/ - processed data (auto-created)

# 5. Install the dependencies
В терминале PyCharm выполните:
poetry install

# 6. Запустите ETL
python etl/main.py --source "https://drive.google.com/uc?id=1m7QX3GeQMWw2Ni4EGKnte37ikID7Z5Gj"
ваш_проект/
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── validate.py
│   └── main.py
├── data/
│   ├── raw/          # пустая
│   └── processed/    # пустая
├── pyproject.toml
├── requirements.txt
├── environment.yml
└── README.md