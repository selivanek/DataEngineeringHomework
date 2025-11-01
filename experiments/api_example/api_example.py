import requests
import pandas as pd
url = "https://makeup-api.herokuapp.com/api/v1/products.json"
personal_input = input('Введите имя бренда косметики\n').lower()


params = {
    "brand": "nyx"
}
try:
    if personal_input != '':
        params["brand"] = personal_input

    # Выполняем GET-запрос
    response = requests.get(url, params=params)



    # Проверяем успешность запроса
    response.raise_for_status()

    # Парсим JSON-ответ
    data = response.json()

    # Преобразуем в DataFrame
    # Если ответ — список словарей
    df = pd.DataFrame(data)

    # Если ответ вложенный (например, {'results': [...]})
    # df = pd.DataFrame(data['results'])

    with pd.option_context('display.max_columns', None, 'display.width', None):
        print(df.head(5))  # вывести первые 5 строк

except requests.exceptions.RequestException as e:
    print(f"Ошибка при запросе: {e}")