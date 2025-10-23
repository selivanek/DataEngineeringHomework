# API Reader Example - Makeup Products

Проект для загрузки данных о косметических продуктах через открытое API и анализа с помощью Pandas DataFrame.

## Описание проекта
Скрипт `api_reader.py` взаимодействует с Makeup API, позволяя получать информацию о косметических продуктах различных брендов. Данные загружаются в структурированном виде в Pandas DataFrame для дальнейшего анализа.

## Источник данных
**Каталог API**: https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/  
**Используемое API**: Makeup API  
**Базовый URL**: https://makeup-api.herokuapp.com/api/v1/products.json

## Технические требования
- Python 3.6+
- Библиотеки: pandas, requests

## Установка зависимостей
```bash
pip install pandas requests