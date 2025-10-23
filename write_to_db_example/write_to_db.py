import os
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import io
def main():
    print("=" * 50)
    print("🚀 Выполнение домашнего задания")
    print("=" * 50)
    print("ℹ️  Все учетные данные берутся из переменных среды Windows")
    print("=" * 50)

    # 1. ПРОВЕРКА ПЕРЕМЕННЫХ СРЕДЫ
    print("\n1. 🔍 Проверяем настройки подключения в переменных среды...")

    # Получаем данные ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ WINDOWS
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    # Проверяем, что все переменные среды установлены
    if not all([db_host, db_port, db_name, db_user, db_password]):
        print("❌ Ошибка: Не все настройки базы данных найдены в переменных среды!")
        print("   Убедитесь, что в переменных среды Windows установлены:")
        print("   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        print("\n   💡 Как установить переменные среды:")
        print("   - Win + R → sysdm.cpl → Enter")
        print("   - Дополнительно → Переменные среды")
        print("   - В разделе 'Переменные среды пользователя' нажми 'Создать'")
        return

    print("   ✅ Все настройки подключения найдены в переменных среды Windows")
    print(f"   📍 Хост (DB_HOST): {db_host}")
    print(f"   🔌 Порт (DB_PORT): {db_port}")
    print(f"   🗃️  База данных (DB_NAME): {db_name}")
    print(f"   👤 Пользователь (DB_USER): {db_user}")
    print(f"   🔒 Пароль (DB_PASSWORD): ***")

    # 2. ПОДКЛЮЧЕНИЕ К PostgreSQL БД homeworks (используя переменные среды)
    print("\n2. 🔗 Подключаемся к PostgreSQL БД homeworks...")
    print("   📝 Используем данные из переменных среды")

    try:
        # Создаем строку подключения ИЗ ПЕРЕМЕННЫХ СРЕДЫ
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        # Создаем подключение
        engine = create_engine(connection_string)
        print("   ✅ Подключение к PostgreSQL успешно!")
        print("   💡 Данные для подключения взяты из переменных среды Windows")

    except Exception as e:
        print(f"   ❌ Ошибка подключения: {e}")
        return

    # 3. ЗАГРУЗКА ДАННЫХ ИЗ PARQUET ФАЙЛА
    print("\n3. 📥 Загружаем данные из GitHub...")

    # Ссылка на Parquet файл в репозитории GitHub
    parquet_url = "https://github.com/selivanek/DataEngineeringHomework/raw/main/updated_dataset.parquet"

    try:
        # Скачиваем файл из интернета
        print("   📡 Скачиваем файл из GitHub...")
        response = requests.get(parquet_url)
        response.raise_for_status()

        # Читаем данные в DataFrame
        df = pd.read_parquet(io.BytesIO(response.content))
        print(f"   ✅ Файл загружен! Всего строк: {len(df)}")

    except Exception as e:
        print(f"   ❌ Ошибка загрузки файла: {e}")
        print("   💡 Проверьте подключение к интернету")
        return

    # 4. ПОДГОТОВКА ДАННЫХ (первые 100 строк)
    print("\n4. 📊 Подготавливаем данные...")

    if len(df) > 100:
        df_100 = df.head(100)
        print(f"   ✅ Взяли первые 100 строк из {len(df)}")
    else:
        df_100 = df
        print(f"   ✅ В файле всего {len(df)} строк, берем все")

    # 5. ЗАПИСЬ ДАННЫХ В ТАБЛИЦУ
    print("\n5. 💾 Записываем данные в таблицу sevalneva...")
    print("   📝 Используем подключение через переменные среды")

    try:
        # Записываем данные в таблицу 'sevalneva'
        df_100.to_sql(
            'sevalneva',
            engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        print("   ✅ Данные успешно записаны в таблицу 'sevalneva'!")

    except Exception as e:
        print(f"   ❌ Ошибка записи: {e}")
        return

    # 6. ПРОВЕРКА РЕЗУЛЬТАТА
    print("\n6. ✅ Проверяем результат...")

    try:
        # Считаем сколько строк в таблице
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM public.sevalneva"))
            row_count = result.scalar()

        print(f"   ✅ В таблице 'sevalneva' теперь {row_count} строк")

        # Показываем информацию о таблице
        print(f"\n   📊 Информация о таблице:")
        print(f"      - Имя: sevalneva")
        print(f"      - Строк: {row_count}")
        print(f"      - Колонок: {len(df_100.columns)}")

    except Exception as e:
        print(f"   ❌ Ошибка проверки: {e}")
        return

    # 7. ЗАВЕРШЕНИЕ
    print("\n" + "=" * 50)
    print("🎉 ВСЕ ПУНКТЫ ЗАДАНИЯ ВЫПОЛНЕНЫ!")
    print("💡 Все учетные данные брались из переменных среды Windows")
    print("=" * 50)


if __name__ == "__main__":
    main()

