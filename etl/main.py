import argparse
from extract import extract_data
from transform import transform_data, save_transformed_data
from load import load_to_database, save_processed_parquet


def run_etl_pipeline(source_url: str, table_name: str = "sevalneva", max_rows: int = 100):
    """
    Полный ETL pipeline
    """
    print("=" * 50)
    print("🚀 Запуск ETL пайплайна")
    print("=" * 50)

    try:
        # EXTRACT
        raw_df = extract_data(source_url, "data/raw/raw_data.csv")

        # TRANSFORM
        transformed_df = transform_data(raw_df)
        save_transformed_data(transformed_df, "data/processed/transformed_data.parquet")

        # LOAD
        success = load_to_database(transformed_df, table_name, max_rows)
        if success:
            save_processed_parquet(transformed_df.head(max_rows), "data/processed/final_data.parquet")

        print("=" * 50)
        print("🎉 ETL пайплайн завершен!")
        print("=" * 50)

    except Exception as e:
        print(f"❌ Ошибка в ETL пайплайне: {e}")


def main():
    parser = argparse.ArgumentParser(description='ETL Pipeline для обработки данных')
    parser.add_argument('--source', type=str, required=True,
                        help='URL источника данных (обязательный)')
    parser.add_argument('--table', type=str, default='sevalneva',
                        help='Имя таблицы в БД (по умолчанию: sevalneva)')
    parser.add_argument('--max-rows', type=int, default=100,
                        help='Максимальное количество строк для загрузки в БД (по умолчанию: 100)')

    args = parser.parse_args()

    run_etl_pipeline(args.source, args.table, args.max_rows)


if __name__ == "__main__":
    main()