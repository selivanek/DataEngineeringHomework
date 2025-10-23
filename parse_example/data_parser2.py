import requests
from bs4 import BeautifulSoup
import pandas as pd


def parse_f1news_working():
    """Рабочий парсер для F1News.ru на основе анализа структуры"""
    url = "https://www.f1news.ru/"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        print("🚀 Запускаем парсинг F1News.ru...")
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')

        news = []

        # ВАРИАНТ 1: Если есть стандартные новостные блоки
        news_blocks = soup.select('article, .news-item, .b-news-item, [class*="news"]')

        if news_blocks:
            print(f"🎯 Найдено новостных блоков: {len(news_blocks)}")

            for block in news_blocks:
                try:
                    # Извлекаем заголовок
                    title_elem = block.find(['h1', 'h2', 'h3', 'h4']) or block.find('a')
                    title = title_elem.get_text(strip=True) if title_elem else None

                    if not title or len(title) < 10:
                        continue

                    # Извлекаем ссылку
                    link_elem = block.find('a', href=True)
                    link = link_elem['href'] if link_elem else None
                    if link and link.startswith('/'):
                        link = "https://www.f1news.ru" + link

                    # Извлекаем описание
                    desc_elem = block.find('p') or block.find(class_=lambda x: x and ('desc' in x or 'text' in x))
                    description = desc_elem.get_text(strip=True) if desc_elem else None

                    # Извлекаем изображение
                    img_elem = block.find('img')
                    image = img_elem.get('src') if img_elem else None
                    if image and image.startswith('//'):
                        image = 'https:' + image
                    elif image and image.startswith('/'):
                        image = 'https://www.f1news.ru' + image

                    news.append({
                        "title": title,
                        "ref": link,
                        "content_small": description,
                        "image": image
                    })

                except Exception as e:
                    continue

        # ВАРИАНТ 2: Если не нашли блоки, ищем по ссылкам
        if not news:
            print("🔍 Ищем новости по ссылкам...")
            all_links = soup.find_all('a', href=True)

            for link in all_links:
                try:
                    title = link.get_text(strip=True)
                    href = link['href']

                    # Фильтруем новостные ссылки
                    if (title and len(title) > 20 and
                            ('f1' in href.lower() or 'news' in href.lower()) and
                            not any(word in title.lower() for word in ['читать', 'подробнее', 'все', '...']) and
                            not href.startswith(('javascript:', 'mailto:', '#'))):

                        # Делаем ссылку абсолютной
                        if href.startswith('/'):
                            href = "https://www.f1news.ru" + href
                        elif not href.startswith('http'):
                            href = "https://www.f1news.ru/" + href

                        # Ищем описание рядом со ссылкой
                        description = None
                        parent = link.parent
                        if parent:
                            paragraphs = parent.find_all('p')
                            for p in paragraphs:
                                text = p.get_text(strip=True)
                                if text and text != title and len(text) > 30:
                                    description = text
                                    break

                        news.append({
                            "title": title,
                            "ref": href,
                            "content_small": description,
                            "image": None
                        })

                        if len(news) >= 15:  # Ограничиваем количество
                            break

                except Exception:
                    continue

        # Создаем DataFrame
        df = pd.DataFrame(news)

        if not df.empty:
            print(f"✅ УСПЕХ! Получено {len(df)} новостей!")
            # Удаляем дубликаты
            df = df.drop_duplicates(subset=['title', 'ref'])
            print(f"📊 После удаления дубликатов: {len(df)} новостей")
        else:
            print("❌ Не удалось извлечь новости")

        return df

    except Exception as e:
        print(f"❌ Ошибка парсинга: {e}")
        return pd.DataFrame()


# Запускаем парсер
data = parse_f1news_working()

if not data.empty:
    print("\n📰 ПОСЛЕДНИЕ НОВОСТИ F1:")
    print("=" * 80)
    for idx, row in data.head(10).iterrows():
        print(f"{idx + 1}. {row['title']}")
        print(f"   🔗 {row['ref']}")
        if row['content_small']:
            print(f"   📝 {row['content_small'][:100]}...")
        if row['image']:
            print(f"   🖼️ {row['image']}")
        print()
else:
    print("😞 Не удалось получить данные с F1News.ru")


