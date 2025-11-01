import pandas as pd
import os
from .validate import validate_raw_data


def extract_data(source_url: str, raw_data_path: str) -> pd.DataFrame:
    """
    Загрузка и валидация сырых данных
    """
    print("📥 Загружаем сырые данные...")

    # Загрузка данных
    raw_data = pd.read_csv(source_url)
    print(f"✅ Данные загружены! Строк: {len(raw_data)}, Колонок: {len(raw_data.columns)}")

    # Валидация
    if validate_raw_data(raw_data):
        # Сохранение сырых данных
        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)
        raw_data.to_csv(raw_data_path, index=False)
        print(f"💾 Сырые данные сохранены в: {raw_data_path}")

    return raw_data