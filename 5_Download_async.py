#!/usr/bin/env python3
"""
Простой загрузчик изображений из SQLite БД без конфигов.
Поддерживает: yandex_images.db → папка yandex_images
              vk_images.db      → папка vk_images
Сохраняет оба URL (url + origin_url) в трекер и в .txt-файл рядом с изображением.
"""

import asyncio
import sqlite3
import hashlib
import mimetypes
import sys
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional

import aiohttp
import aiofiles


# === Вспомогательные функции ===

def normalize_url(url: Optional[str]) -> Optional[str]:
    """Нормализует URL: добавляет схему, убирает лишние пробелы."""
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith(("http://", "https://")):
        return None
    return url


def get_file_extension(url: str, content_type: Optional[str]) -> str:
    """Определяет расширение файла по Content-Type или URL."""
    ext = None
    if content_type:
        ct = content_type.split(";")[0].strip()
        ext = mimetypes.guess_extension(ct)
    if not ext:
        parsed = urlparse(url)
        suffix = Path(parsed.path).suffix.lower()
        if suffix and 1 < len(suffix) <= 5 and suffix.count(".") == 1:
            ext = suffix
    return ext if ext and ext.startswith(".") else ".jpg"


def build_local_path(
    download_root: Path,
    image_id: str,
    variant_type: str,
    width: Optional[int],
    height: Optional[int],
    url: str,
) -> Path:
    """Формирует уникальный путь для сохранения изображения."""
    w = width or 0
    h = height or 0
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    base = f"{image_id}_{variant_type}_{w}x{h}_{url_hash}"
    safe_base = "".join(c if c.isalnum() or c in "._- " else "_" for c in base)
    if len(safe_base) > 200:
        safe_base = safe_base[:150] + "_" + safe_base[-42:]
    return download_root / safe_base


def write_sidecar(
    path: Path,
    url: Optional[str],
    origin_url: Optional[str],
    image_id: str,
    variant_type: str,
    width: Optional[int],
    height: Optional[int],
) -> None:
    """Создаёт .txt-файл с метаданными рядом с изображением."""
    sidecar = path.with_suffix(".txt")
    if sidecar.exists():
        return
    try:
        with sidecar.open("w", encoding="utf-8") as f:
            f.write(f"url: {url or ''}\n")
            f.write(f"origin_url: {origin_url or ''}\n")
            f.write(f"image_id: {image_id}\n")
            f.write(f"variant_type: {variant_type}\n")
            f.write(f"width: {width or ''}\n")
            f.write(f"height: {height or ''}\n")
    except Exception:
        pass  # Игнорируем ошибки записи сайдкара


# === Работа с БД трекера ===

def init_tracker_db(tracker_db: Path) -> None:
    """Инициализирует БД трекера."""
    with sqlite3.connect(tracker_db) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS download_log (
                variant_id INTEGER PRIMARY KEY,
                image_id TEXT NOT NULL,
                variant_type TEXT NOT NULL,
                url TEXT,
                origin_url TEXT,
                local_path TEXT,
                status TEXT CHECK(status IN ('pending', 'downloaded', 'failed', 'skipped')) DEFAULT 'pending',
                error_message TEXT,
                file_size_bytes INTEGER,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


def get_pending_tasks(source_db: Path, tracker_db: Path) -> list:
    """Возвращает задачи, которые ещё не были загружены."""
    try:
        with sqlite3.connect(tracker_db) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("SELECT variant_id FROM download_log WHERE status = 'downloaded'")
            downloaded = {row["variant_id"] for row in cur.fetchall()}
    except sqlite3.OperationalError:
        downloaded = set()

    with sqlite3.connect(source_db) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT id, image_id, variant_type, url, origin_url, width, height
            FROM image_variants
            WHERE url IS NOT NULL OR origin_url IS NOT NULL
        """)
        all_tasks = cur.fetchall()

    return [t for t in all_tasks if t["id"] not in downloaded]


def update_tracker(tracker_db: Path, record: tuple) -> None:
    """Обновляет запись в трекере."""
    with sqlite3.connect(tracker_db) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO download_log
            (variant_id, image_id, variant_type, url, origin_url,
             local_path, status, error_message, file_size_bytes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, record)
        conn.commit()


def select_download_url(url: Optional[str], origin_url: Optional[str], prefer_origin: bool) -> Optional[str]:
    """Выбирает URL для загрузки."""
    if prefer_origin:
        candidate = normalize_url(origin_url)
        if candidate:
            return candidate
    candidate = normalize_url(url)
    return candidate or normalize_url(origin_url)


# === Загрузка изображений ===

async def download_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    task: sqlite3.Row,
    download_root: Path,
    base_sleep: float,
    retry_attempts: int,
    prefer_origin: bool,
) -> tuple:
    """Загружает одно изображение."""
    async with semaphore:
        await asyncio.sleep(base_sleep)

        variant_id = task["id"]
        image_id = task["image_id"]
        variant_type = task["variant_type"]
        url = task["url"]
        origin_url = task["origin_url"]
        width = task["width"]
        height = task["height"]

        download_url = select_download_url(url, origin_url, prefer_origin)
        if not download_url:
            return (
                variant_id, image_id, variant_type, url, origin_url,
                None, "skipped", "No valid URL", None
            )

        error_msg = "Unknown error"
        final_path = None

        for attempt in range(retry_attempts + 1):
            try:
                async with session.get(download_url) as resp:
                    resp.raise_for_status()
                    content_type = resp.headers.get("Content-Type", "")
                    ext = get_file_extension(download_url, content_type)
                    final_path = build_local_path(
                        download_root, image_id, variant_type, width, height, download_url
                    ).with_suffix(ext)

                    # Если файл уже существует — пропускаем загрузку, но сохраняем метаданные
                    if final_path.exists():
                        size = final_path.stat().st_size
                        write_sidecar(final_path, url, origin_url, image_id, variant_type, width, height)
                        return (
                            variant_id, image_id, variant_type, url, origin_url,
                            str(final_path), "downloaded", None, size
                        )

                    # Загрузка файла
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    temp_path = final_path.with_suffix(".tmp")
                    total_bytes = 0

                    async with aiofiles.open(temp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            if chunk:
                                await f.write(chunk)
                                total_bytes += len(chunk)

                    temp_path.replace(final_path)
                    write_sidecar(final_path, url, origin_url, image_id, variant_type, width, height)
                    return (
                        variant_id, image_id, variant_type, url, origin_url,
                        str(final_path), "downloaded", None, total_bytes
                    )

            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    await asyncio.sleep(retry_after)
                    continue
                error_msg = f"HTTP {e.status}"
            except (aiohttp.ClientConnectorError, asyncio.TimeoutError):
                error_msg = "Connection/timeout error"
            except aiohttp.ClientError as e:
                error_msg = f"aiohttp error: {type(e).__name__}"
            except (PermissionError, OSError) as e:
                error_msg = f"OS error: {type(e).__name__}"
            except Exception as e:
                error_msg = f"Unexpected: {type(e).__name__}: {str(e)[:100]}"

            if attempt < retry_attempts:
                await asyncio.sleep(1.0)
                continue
            break

        return (
            variant_id, image_id, variant_type, url, origin_url,
            str(final_path) if final_path else None,
            "failed", error_msg, None
        )


async def run_downloads_async(
    source_db: Path,
    download_root: Path,
    tracker_db: Path,
    max_workers: int,
    prefer_origin: bool,
    show_progress: bool,
) -> None:
    """Основной цикл асинхронной загрузки."""
    init_tracker_db(tracker_db)
    tasks = get_pending_tasks(source_db, tracker_db)
    print(f"Найдено задач для загрузки: {len(tasks):,}")

    if not tasks:
        generate_report(tracker_db)
        return

    semaphore = asyncio.Semaphore(max_workers)
    connector = aiohttp.TCPConnector(limit=max_workers)
    timeout = aiohttp.ClientTimeout(total=40, connect=10)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={"User-Agent": user_agent},
    ) as session:
        coros = [
            download_one(
                session, semaphore, task,
                download_root, base_sleep=0.05,
                retry_attempts=3, prefer_origin=prefer_origin
            )
            for task in tasks
        ]

        if show_progress:
            try:
                from tqdm import tqdm
                with tqdm(total=len(coros), desc="📥 Загрузка", unit="файл") as pbar:
                    for future in asyncio.as_completed(coros):
                        result = await future
                        update_tracker(tracker_db, result)
                        pbar.update(1)
            except ImportError:
                show_progress = False
                print("tqdm не установлен — прогресс-бар отключён.")

        if not show_progress:
            for future in asyncio.as_completed(coros):
                result = await future
                update_tracker(tracker_db, result)

    generate_report(tracker_db)


def generate_report(tracker_db: Path) -> None:
    """Выводит итоговый отчёт."""
    with sqlite3.connect(tracker_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT status, COUNT(*) FROM download_log GROUP BY status")
        summary = dict(cur.fetchall())

        def top_errors(status: str):
            cur.execute("""
                SELECT error_message, COUNT(*) as cnt
                FROM download_log
                WHERE status = ? AND error_message IS NOT NULL
                GROUP BY error_message
                ORDER BY cnt DESC
                LIMIT 10
            """, (status,))
            return cur.fetchall()

        top_failed = top_errors("failed")
        top_skipped = top_errors("skipped")

    total = sum(summary.values())
    downloaded = summary.get("downloaded", 0)
    failed = summary.get("failed", 0)
    skipped = summary.get("skipped", 0)

    print("\n" + "=" * 70)
    print("✅ ЗАГРУЗКА ЗАВЕРШЕНА — ИТОГОВЫЙ ОТЧЁТ")
    print("=" * 70)
    print(f"Всего обработано: {total:,}")
    print(f"✅ Успешно:       {downloaded:,} ({downloaded / total * 100:.1f}%)")
    print(f"❌ Ошибки:        {failed:,} ({failed / total * 100:.1f}%)")
    print(f"⏭️  Пропущено:    {skipped:,} ({skipped / total * 100:.1f}%)")

    if top_failed:
        print("\n🔍 Топ-10 причин ошибок:")
        for msg, cnt in top_failed:
            print(f"  • {msg} → {cnt}")

    if top_skipped:
        print("\n⏭️  Топ-10 причин пропусков:")
        for msg, cnt in top_skipped:
            print(f"  • {msg} → {cnt}")

    print(f"\n📊 Трекер: {tracker_db}")
    print("=" * 70)


# === Точка входа ===

def main() -> None:
    print("Выберите источник изображений:")
    print("1) yandex_images.db  → сохранить в папку yandex_images")
    print("2) vk_images.db      → сохранить в папку vk_images")
    choice = input("Введите 1 или 2: ").strip()

    if choice == "1":
        source_db = Path("yandex_images.db")
        download_root = Path("yandex_images")
    elif choice == "2":
        source_db = Path("vk_images.db")
        download_root = Path("vk_images")
    else:
        print("❌ Неверный выбор. Выход.")
        sys.exit(1)

    if not source_db.exists():
        print(f"❌ База данных не найдена: {source_db}")
        sys.exit(1)

    download_root.mkdir(exist_ok=True)
    tracker_db = download_root / "tracker.db"

    # Проверка tqdm для прогресс-бара
    try:
        import tqdm  # noqa: F401
        show_progress = True
    except ImportError:
        show_progress = False
        print("ℹ️  tqdm не установлен — отображение прогресса отключено.")

    print(f"\n🚀 Запуск загрузки из {source_db} в {download_root}")
    print(f"   Трекер: {tracker_db}")
    print(f"   Потоков: 40")
    print()

    try:
        asyncio.run(
            run_downloads_async(
                source_db=source_db,
                download_root=download_root,
                tracker_db=tracker_db,
                max_workers=40,
                prefer_origin=False,  # Используем url, fallback на origin_url
                show_progress=show_progress,
            )
        )
    except KeyboardInterrupt:
        print("\n⚠️  Загрузка прервана пользователем.")
        sys.exit(130)


if __name__ == "__main__":
    main()