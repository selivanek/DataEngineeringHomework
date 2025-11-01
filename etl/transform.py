import pandas as pd
import numpy as np
import os
from .validate import validate_transformed_data


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Трансформация данных: приведение типов
    """
    print("🔄 Выполняем трансформации...")

    # Оптимизация float64 -> float16
    float64_cols = df.select_dtypes(include=['float64']).columns
    for col in float64_cols:
        min_val = df[col].min()
        max_val = df[col].max()

        if min_val >= -65500 and max_val <= 65500:
            df[col] = df[col].astype(np.float16)
            print(f"   🔄 {col}: float64 -> float16")

    # Преобразование Genotype в bool
    if "Genotype" in df.columns:
        df["Genotype"] = df["Genotype"].astype(bool)
        print(f"   🔄 Genotype -> bool")

    print("✅ Трансформации завершены!")
    print(f"📊 Типы данных после преобразования:\n{df.dtypes}")

    if validate_transformed_data(df):
        return df


def save_transformed_data(df: pd.DataFrame, processed_path: str):
    """
    Сохранение преобразованных данных
    """
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df.to_parquet(processed_path, index=False)
    print(f"💾 Преобразованные данные сохранены в: {processed_path}")