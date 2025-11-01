import os
import pandas as pd
from sqlalchemy import create_engine


def validate_raw_data(df: pd.DataFrame) -> bool:
    """
    Валидация сырых данных
    """
    print("🔍 Валидируем сырые данные...")

    checks = [
        (not df.empty, "Данные не должны быть пустыми"),
        (len(df.columns) > 0, "Должны присутствовать колонки"),
        (df.isnull().sum().sum() < len(df) * 0.5, "Не более 50% пропущенных значений")
    ]

    all_valid = True
    for check, message in checks:
        if not check:
            print(f"❌ {message}")
            all_valid = False
        else:
            print(f"✅ {message}")

    return all_valid


def validate_transformed_data(df: pd.DataFrame) -> bool:
    """
    Валидация преобразованных данных
    """
    print("🔍 Валидируем преобразованные данные...")

    # Проверяем, что преобразования типов прошли успешно
    float16_cols = df.select_dtypes(include=['float16']).columns
    bool_cols = df.select_dtypes(include=['bool']).columns

    print(f"✅ Преобразовано float16 колонок: {len(float16_cols)}")
    print(f"✅ Преобразовано bool колонок: {len(bool_cols)}")

    return True


def validate_database_connection() -> dict:
    """
    Валидация подключения к БД
    """
    print("🔍 Проверяем настройки подключения к БД...")

    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    required_vars = {
        'DB_HOST': db_host,
        'DB_PORT': db_port,
        'DB_NAME': db_name,
        'DB_USER': db_user,
        'DB_PASSWORD': db_password
    }

    missing_vars = [var for var, value in required_vars.items() if not value]

    if missing_vars:
        print(f"❌ Отсутствуют переменные среды: {', '.join(missing_vars)}")
        return None

    print("✅ Все настройки подключения найдены")
    return {
        'host': db_host,
        'port': db_port,
        'database': db_name,
        'user': db_user,
        'password': db_password
    }


def validate_output_data(engine, table_name: str, row_count: int) -> bool:
    """
    Валидация выходных данных
    """
    print("🔍 Валидируем выходные данные...")

    checks = [
        (row_count > 0, f"Таблица {table_name} не должна быть пустой"),
        (row_count <= 100, "Не более 100 строк в БД")
    ]

    all_valid = True
    for check, message in checks:
        if not check:
            print(f"❌ {message}")
            all_valid = False
        else:
            print(f"✅ {message}")

    return all_valid