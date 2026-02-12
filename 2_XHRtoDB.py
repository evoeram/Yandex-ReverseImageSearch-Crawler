import json
import sqlite3
from pathlib import Path

# Путь к папке с JSON-файлами
XHR_DIR = Path('XHR')  # или абсолютный путь, например: Path('/путь/к/XHR')

# Подключение к БД (один раз для всех файлов)
conn = sqlite3.connect('yandex_images.db')
cur = conn.cursor()

# Создание таблиц (если ещё не созданы)
cur.execute("""
    CREATE TABLE IF NOT EXISTS images (
        id TEXT PRIMARY KEY,
        docid TEXT,
        documentid TEXT,
        reqid TEXT,
        rimId TEXT,
        pos INTEGER,
        url TEXT,
        origUrl TEXT,
        image_url TEXT,
        alt TEXT,
        width INTEGER,
        height INTEGER,
        origWidth INTEGER,
        origHeight INTEGER,
        title TEXT,
        domain TEXT,
        snippet_url TEXT,
        freshness_counter INTEGER,
        is_gif BOOLEAN,
        ecom_shield BOOLEAN,
        censored BOOLEAN,
        loading_state TEXT
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS image_variants (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        image_id TEXT NOT NULL,
        variant_type TEXT NOT NULL,  -- 'preview', 'dups', 'thumb'
        url TEXT,
        width INTEGER,
        height INTEGER,
        file_size_bytes INTEGER,
        is_mixed_image BOOLEAN,
        origin_url TEXT,
        origin_width INTEGER,
        origin_height INTEGER,
        FOREIGN KEY (image_id) REFERENCES images(id)
    )
""")

# Получаем все .json файлы в папке XHR
json_files = XHR_DIR.glob("*.json")

for json_file in json_files:
    print(f"Обработка файла: {json_file}")
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Ошибка при чтении {json_file}: {e}")
        continue

    # Проверяем структуру данных
    try:
        entities = data['blocks'][1]['params']['adapterData']['serpList']['items']['entities']
    except KeyError as e:
        print(f"Пропущен файл {json_file}: отсутствует ключ {e}")
        continue

    for entity_id, img in entities.items():
        # Вставка в images
        cur.execute("""
            INSERT OR IGNORE INTO images (
                id, docid, documentid, reqid, rimId, pos,
                url, origUrl, image_url, alt,
                width, height, origWidth, origHeight,
                title, domain, snippet_url,
                freshness_counter, is_gif, ecom_shield, censored, loading_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entity_id,
            img.get('docid'),
            img.get('documentid'),
            img.get('reqid'),
            img.get('rimId'),
            img.get('pos'),
            img.get('url'),
            img.get('origUrl'),
            img.get('image'),  # это image_url
            img.get('alt'),
            img.get('width'),
            img.get('height'),
            img.get('origWidth'),
            img.get('origHeight'),
            img.get('snippet', {}).get('title'),
            img.get('snippet', {}).get('domain'),
            img.get('snippet', {}).get('url'),
            img.get('freshnessCounter'),
            bool(img.get('gifLabel')),
            bool(img.get('ecomShield')),
            bool(img.get('censored')),
            img.get('loading')
        ))

        # Обработка viewerData
        vd = img.get('viewerData', {})

        # Превью
        for item in vd.get('preview', []):
            cur.execute("""
                INSERT INTO image_variants (
                    image_id, variant_type, url, width, height, file_size_bytes,
                    is_mixed_image, origin_url, origin_width, origin_height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entity_id,
                'preview',
                item.get('url'),
                item.get('w'),
                item.get('h'),
                item.get('fileSizeInBytes'),
                bool(item.get('isMixedImage')),
                item.get('origin', {}).get('url'),
                item.get('origin', {}).get('w'),
                item.get('origin', {}).get('h')
            ))

        # Дубли (dups)
        for item in vd.get('dups', []):
            cur.execute("""
                INSERT INTO image_variants (
                    image_id, variant_type, url, width, height, file_size_bytes,
                    is_mixed_image, origin_url, origin_width, origin_height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entity_id,
                'dups',
                item.get('url'),
                item.get('w'),
                item.get('h'),
                item.get('fileSizeInBytes'),
                bool(item.get('isMixedImage')),
                item.get('origin', {}).get('url'),
                item.get('origin', {}).get('w'),
                item.get('origin', {}).get('h')
            ))

        # Thumb
        thumb = vd.get('thumb')
        if thumb:
            cur.execute("""
                INSERT INTO image_variants (
                    image_id, variant_type, url, width, height
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                entity_id,
                'thumb',
                thumb.get('url'),
                thumb.get('w'),
                thumb.get('h')
            ))

# Сохранение всех изменений и закрытие соединения
conn.commit()
conn.close()

print("Все файлы обработаны.")