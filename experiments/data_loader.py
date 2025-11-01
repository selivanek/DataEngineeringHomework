
import pandas as pd
import numpy as np

FILE_ID = "1m7QX3GeQMWw2Ni4EGKnte37ikID7Z5Gj"
file_url = f"https://drive.google.com/uc?id={FILE_ID}"

raw_data = pd.read_csv(file_url)

print(raw_data.head(10))


df = pd.DataFrame(raw_data)

print(df.dtypes)
print('\n')


float64_cols = df.select_dtypes(include=['float64']).columns

for col in float64_cols:
        # Проверяем, что значения в пределах диапазона float16
        min_val = df[col].min()
        max_val = df[col].max()

        # Диапазон float16: примерно от -65500 до 65500
        if min_val >= -65500 and max_val <= 65500:
            df[col] = df[col].astype(np.float16)

df["Genotype"] = df["Genotype"].astype(bool)

print(df.dtypes)
print('\n')
print(df.values)
df.to_parquet("updated_dataset.parquet");


