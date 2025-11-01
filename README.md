Data Engineering Project - Molecular Mechanisms of Down Syndrome
📋 Project Overview
Комплексный проект анализа данных для изучения молекулярных механизмов синдрома Дауна и эффектов терапии меманином. Проект включает полный ETL-конвейер, статистический анализ, машинное обучение и визуализацию данных.

🏗️ Project Structure
text
my_project/
├── etl/                           # ETL package
│   ├── __init__.py
│   ├── extract.py                 # Data extraction
│   ├── transform.py               # Data transformation
│   ├── load.py                    # Data loading
│   ├── validate.py                # Data validation
│   └── main.py                    # ETL pipeline
├── notebooks/
│   └── EDA.ipynb                  # Exploratory Data Analysis
├── scripts/
│   ├── data_type_conversion.py    # Data type optimization (HW 1-3)
│   ├── write_to_db.py             # PostgreSQL integration
│   ├── api_reader.py              # Makeup API client
│   └── data_parser.py             # F1 news parser
├── data/
│   ├── raw/                       # Raw data storage
│   └── processed/                 # Processed data storage
├── images/                        # Documentation images
├── pyproject.toml                 # Poetry dependencies
├── requirements.txt               # Pip dependencies
├── environment.yml               # Conda environment
└── README.md

🧬 Data Source
Основной датасет: Mice Protein Expression
Платформа: Kaggle
Ссылка: https://www.kaggle.com/datasets/ruslankl/mice-protein-expression
Размер: 1,080 образцов, 82 признака
Особенности: 77 белковых маркеров коры головного мозга

Ключевые переменные:
Genotype: 0 - контроль, 1 - трисомия
Treatment: Memantine (меманин), Saline (солевой раствор)
Behavior: C/S (обучение), S/C (контроль)
class: комбинация факторов (напр., c-CS-m)

⚙️ Installation & Setup
Способ 1: Poetry (рекомендуется)
poetry install
Способ 2: Pip
pip install -r requirements.txt
Способ 3: Conda
conda env create -f environment.yml
conda activate my-project

Основные зависимости:
Анализ данных: pandas, numpy, scikit-learn, scipy
Визуализация: matplotlib, seaborn
Работа с БД: sqlalchemy, pyarrow, psycopg2-binary
Парсинг: requests, beautifulsoup4, wget
Ноутбуки: jupyterlab

🚀 Quick Start
1. Установите зависимости (выберите один способ):
bash
# Способ 1: Poetry
poetry install
# Способ 2: Pip  
pip install -r requirements.txt
# Способ 3: Conda
conda env create -f environment.yml
conda activate my-project

2. Запустите ETL пайплайн:
bash
python etl/main.py --source "https://drive.google.com/uc?id=1m7QX3GeQMWw2Ni4EGKnte37ikID7Z5Gj"

3. Запустите EDA анализ: 
bash
jupyter notebook notebooks/EDA.ipynb

📊 Key Scientific Findings
Молекулярные нарушения:
61% белков значимо изменены при трисомии (47 из 77)
Системный характер молекулярных нарушений

Терапевтические эффекты:
Меманин модулирует 66% нарушенных белков
Восстановление ключевых сигнальных путей

Нейропластичность:
Измененная молекулярная реакция на обучение у трисомных мышей
Иная архитектура пластичности, а не ее снижение

Машинное обучение:
Точность классификации: 97.5% (Random Forest)
Ключевые маркеры: APP_N, ITSN1_N, AcetylH3K9_N

🛠️ Module Descriptions
1. ETL Pipeline (etl/)
Функциональность:
Извлечение данных с Google Drive
Преобразование типов (float64 → float16)
Загрузка в Parquet формате
Валидация качества данных

Запуск:
bash
python etl/main.py --source "https://drive.google.com/uc?id=1m7QX3GeQMWw2Ni4EGKnte37ikID7Z5Gj"

2. Data Type Optimization (Задания 1-3)
Файл: scripts/data_loader.py

Функциональность:
Автоматическое преобразование типов данных
Оптимизация памяти через float16
Конвертация в булевый тип для Genotype
Сохранение в Parquet

3. PostgreSQL Integration
Файл: scripts/write_to_db.py
Безопасность: Учетные данные через переменные среды

# Установка переменных среды (Windows)
$env:DB_HOST="your_host"
$env:DB_PORT="your_port"
$env:DB_NAME="your_name"
$env:DB_USER="your_user"
$env:DB_PASSWORD="your_password"

4. EDA Analysis (notebooks/EDA.ipynb)
📊 Просмотреть EDA анализ в nbviewer
Структура анализа:
Подготовка данных
Анализ качества данных
Статистический анализ
Сравнительный анализ
Машинное обучение
Визуализация

5. API Reader (scripts/api_example.py)
Источник: https://makeup-api.herokuapp.com/api/v1/products.json

6. F1 News Parser (scripts/data_parser2.py)
Источник: https://www.f1news.ru/

🔬 Научная значимость
Проект предоставляет:
Молекулярные мишени для терапии синдрома Дауна
Биомаркеры для диагностики и мониторинга
Основу для разработки прецизионного лечения
Новое понимание механизмов нейропластичности

📝 Примечания
Все чувствительные данные хранятся в переменных среды
Исходные данные доступны через Google Drive интеграцию
EDA анализ содержит полную статистическую верификацию
Проект демонстрирует полный цикл работы с данными: от ETL до ML

👥 Автор
Проект разработан в рамках курса Data Engineering с применением современных инструментов анализа данных и машинного обучения.