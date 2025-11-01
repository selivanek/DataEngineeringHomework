import os
import pandas as pd
from sqlalchemy import create_engine, text
from .validate import validate_database_connection, validate_output_data


def load_to_database(df: pd.DataFrame, table_name: str = "sevalneva", max_rows: int = 100):
    """
    Загрузка данных в PostgreSQL (максимум 100 строк)
    """
    print("💾 Загружаем данные в БД...")

    # Проверка переменных среды
    db_config = validate_database_connection()
    if not db_config:
        return False

    # Ограничение количества строк
    if len(df) > max_rows:
        df_limited = df.head(max_rows)
        print(f"📊 Ограничиваем данные до {max_rows} строк")
    else:
        df_limited = df
        print(f"📊 Используем все {len(df)} строк")

    try:
        # Подключение к БД
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)

        # Запись в БД
        df_limited.to_sql(
            table_name,
            engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        print(f"✅ Данные записаны в таблицу: {table_name}")

        # Проверка результата
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM public.{table_name}"))
            row_count = result.scalar()

        print(f"📊 В таблице '{table_name}' теперь {row_count} строк")

        if validate_output_data(engine, table_name, row_count):
            return True

    except Exception as e:
        print(f"❌ Ошибка загрузки в БД: {e}")
        return False


def save_processed_parquet(df: pd.DataFrame, output_path: str):
    """
    Сохранение обработанных данных в Parquet
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"💾 Обработанные данные сохранены в: {output_path}")