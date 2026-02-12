#!/usr/bin/env python3
"""
Очистка БД изображений от дубликатов.
Поддерживает: yandex_images.db и vk_images.db
Создаёт резервную копию перед удалением.
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime
import shutil


def create_backup(db_path: Path) -> Path:
    """Создаёт резервную копию БД с временной меткой."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.stem}_backup_{timestamp}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    print(f"✅ Создана резервная копия: {backup_path.name}")
    return backup_path


def remove_duplicates_image_variants(conn: sqlite3.Connection) -> int:
    """
    Удаляет дубликаты из image_variants.
    Сохраняет запись с минимальным id в каждой группе дубликатов.
    """
    cursor = conn.cursor()

    # Подсчёт записей до удаления
    cursor.execute("SELECT COUNT(*) FROM image_variants")
    total_before = cursor.fetchone()[0]

    # Удаление дубликатов
    cursor.execute("""
        DELETE FROM image_variants
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM image_variants
            GROUP BY 
                image_id,
                variant_type,
                COALESCE(url, ''),
                COALESCE(origin_url, ''),
                COALESCE(width, -1),
                COALESCE(height, -1),
                COALESCE(file_size_bytes, -1),
                COALESCE(is_mixed_image, -1),
                COALESCE(origin_width, -1),
                COALESCE(origin_height, -1)
        )
    """)

    removed = cursor.rowcount
    cursor.execute("SELECT COUNT(*) FROM image_variants")
    total_after = cursor.fetchone()[0]

    print(f"🧹 image_variants: {total_before:,} → {total_after:,} (удалено {removed:,})")
    return removed


def remove_duplicates_images(conn: sqlite3.Connection) -> int:
    """
    Удаляет дубликаты из images.
    Сохраняет запись с минимальным rowid в каждой группе дубликатов.
    """
    cursor = conn.cursor()

    # Подсчёт записей до удаления
    cursor.execute("SELECT COUNT(*) FROM images")
    total_before = cursor.fetchone()[0]

    # Удаление дубликатов
    cursor.execute("""
        DELETE FROM images
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM images
            GROUP BY 
                COALESCE(id, ''),
                COALESCE(docid, ''),
                COALESCE(documentid, ''),
                COALESCE(reqid, ''),
                COALESCE(rimId, ''),
                COALESCE(url, ''),
                COALESCE(origUrl, ''),
                COALESCE(image_url, ''),
                COALESCE(alt, ''),
                COALESCE(width, -1),
                COALESCE(height, -1),
                COALESCE(origWidth, -1),
                COALESCE(origHeight, -1),
                COALESCE(title, ''),
                COALESCE(domain, ''),
                COALESCE(snippet_url, ''),
                COALESCE(freshness_counter, -1),
                COALESCE(is_gif, -1),
                COALESCE(ecom_shield, -1),
                COALESCE(censored, -1),
                COALESCE(loading_state, '')
        )
    """)

    removed = cursor.rowcount
    cursor.execute("SELECT COUNT(*) FROM images")
    total_after = cursor.fetchone()[0]

    print(f"🧹 images:         {total_before:,} → {total_after:,} (удалено {removed:,})")
    return removed


def vacuum_database(db_path: Path) -> None:
    """Сжимает БД после удаления записей."""
    print("📦 Сжатие базы данных (VACUUM)...")
    conn = sqlite3.connect(db_path)
    conn.execute("VACUUM")
    conn.close()
    print("✅ База данных сжата")


def process_database(db_path: Path) -> None:
    """Основная логика обработки одной БД."""
    if not db_path.exists():
        print(f"❌ База данных не найдена: {db_path}")
        sys.exit(1)

    print(f"\n🔧 Обработка: {db_path}")
    size_before = db_path.stat().st_size / 1024 / 1024
    print(f"📊 Размер до: {size_before:.2f} МБ")

    # Создание резервной копии
    backup_path = create_backup(db_path)

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN TRANSACTION")

        # Удаление дубликатов
        print("\n⚡ Удаление дубликатов...")
        removed_variants = remove_duplicates_image_variants(conn)
        removed_images = remove_duplicates_images(conn)

        conn.commit()
        conn.close()

        # Сжатие БД
        vacuum_database(db_path)

        # Итоги
        size_after = db_path.stat().st_size / 1024 / 1024
        print(f"\n✅ Очистка завершена!")
        print(f"   Размер после: {size_after:.2f} МБ")
        print(
            f"   Экономия:     {size_before - size_after:.2f} МБ ({(size_before - size_after) / size_before * 100:.1f}%)")
        print(f"\nℹ️  Резервная копия сохранена: {backup_path.name}")

    except Exception as e:
        print(f"\n❌ Ошибка при обработке: {e}")
        print(f"⚠️  Восстанавливаем из резервной копии...")
        shutil.copy2(backup_path, db_path)
        print("✅ База данных восстановлена из резервной копии")
        sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("🧹 ОЧИСТКА БАЗЫ ДАННЫХ ОТ ДУБЛИКАТОВ")
    print("=" * 60)
    print("\nВыберите базу данных для обработки:")
    print("1) yandex_images.db")
    print("2) vk_images.db")
    choice = input("\nВведите 1 или 2: ").strip()

    if choice == "1":
        db_path = Path("yandex_images.db")
    elif choice == "2":
        db_path = Path("vk_images.db")
    else:
        print("❌ Неверный выбор. Выход.")
        sys.exit(1)

    process_database(db_path)

    print("\n" + "=" * 60)
    print("✅ Готово! База данных очищена от дубликатов.")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Операция прервана пользователем.")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)