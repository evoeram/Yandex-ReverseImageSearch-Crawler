#!/usr/bin/env python3
"""
Поиск и удаление дубликатов изображений (точных, приблизительных и по имени файла).
Поддержка WebP и всех популярных форматов.
"""

import sys
import logging
import argparse
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Поддерживаемые расширения (включая WebP)
SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp"}


# === Вспомогательные функции изображений ===

def convert_to_rgb(img):
    """Конвертирует изображение в RGB с белым фоном для прозрачных пикселей."""
    if img.mode == "RGBA":
        from PIL import Image
        background = Image.new("RGB", img.size, (255, 255, 255))
        background.paste(img, mask=img.split()[-1])
        return background
    if img.mode == "P":
        img = img.convert("RGBA")
        return convert_to_rgb(img)
    if img.mode != "RGB":
        return img.convert("RGB")
    return img


def get_image_resolution(path: Path) -> Optional[Tuple[int, int]]:
    """Получает реальное разрешение изображения без полной загрузки в память."""
    try:
        from PIL import Image
        with Image.open(path) as img:
            return img.width, img.height
    except Exception as exc:
        logger.warning("⚠️  Не удалось определить разрешение %s: %s", path.name, exc)
        return None


def compute_hash_and_resolution(path: Path, hash_func=None, hash_size: int = 8) -> Optional[Tuple[str, int, int]]:
    """
    Вычисляет хеш (если указан) и разрешение изображения.
    Возвращает: (хеш_строка, ширина, высота) или None при ошибке.
    """
    try:
        from PIL import Image
        import imagehash

        with Image.open(path) as img:
            img_rgb = convert_to_rgb(img)
            width, height = img_rgb.width, img_rgb.height
            if hash_func:
                img_hash = hash_func(img_rgb, hash_size=hash_size)
                return str(img_hash), width, height
            else:
                img_hash = imagehash.dhash(img_rgb)
                return str(img_hash), width, height
    except Exception as exc:
        logger.warning("⚠️  Не удалось обработать %s: %s", path.name, exc)
        return None


# === Точные дубликаты ===

def find_exact_duplicates(
        folder: Path, max_workers: int = 8, show_progress: bool = True
) -> dict[str, List[Tuple[Path, int, Path]]]:
    """Находит группы точных дубликатов по dhash."""
    from PIL import Image
    import imagehash

    image_files = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTS
    ]
    if not image_files:
        logger.warning("📁 В папке не найдено поддерживаемых изображений")
        return {}

    logger.info(f"🔍 Найдено изображений: {len(image_files):,}")
    hash_groups = defaultdict(list)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(compute_hash_and_resolution, f): f
            for f in image_files
        }

        if show_progress:
            try:
                from tqdm import tqdm
                with tqdm(total=len(futures), desc="📊 Хеширование", unit="файл") as pbar:
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            img_hash, w, h = result
                            f = futures[future]
                            hash_groups[img_hash].append((f, w * h, f))
                        pbar.update(1)
            except ImportError:
                show_progress = False
                logger.info("ℹ️  tqdm не установлен — прогресс будет текстовым")

        if not show_progress:
            total = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                if result:
                    img_hash, w, h = result
                    f = futures[future]
                    hash_groups[img_hash].append((f, w * h, f))
                if i % 100 == 0 or i == total:
                    logger.info(f"  Прогресс: {i}/{total} ({i / total * 100:.1f}%)")

    duplicates = {h: files for h, files in hash_groups.items() if len(files) > 1}
    logger.info(f"✅ Найдено групп точных дубликатов: {len(duplicates)}")
    return duplicates


def process_exact_duplicates(
        duplicates: dict[str, List[Tuple[Path, int, Path]]],
        dry_run: bool = True,
        interactive: bool = False,
) -> int:
    """Удаляет/помечает точные дубликаты, оставляя изображение с максимальным разрешением."""
    total_deleted = 0

    for img_hash, files in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True):
        # Сортировка: сначала по площади (убывание), затем по имени для детерминированности
        files.sort(key=lambda x: (x[1], x[2].name), reverse=True)
        to_keep, _, _ = files[0]
        to_delete = [f for f, _, _ in files[1:]]

        logger.info(
            f"\nХеш {img_hash[:8]}... | Группа: {len(files)} файлов | Оставляем: {to_keep.name}"
        )
        for fp in to_delete:
            action = "[DRY-RUN] 🔍 Будет удалён" if dry_run else "🗑️ Удалён"
            logger.info(f"  {action}: {fp.name}")

            if not dry_run:
                if interactive:
                    confirm = input(f"    Подтвердите удаление {fp.name}? [y/N]: ").strip().lower()
                    if confirm != "y":
                        logger.info(f"    Пропущен: {fp.name}")
                        continue
                try:
                    fp.unlink()
                    total_deleted += 1
                except OSError as exc:
                    logger.error(f"    ❌ Ошибка удаления {fp.name}: {exc}")

    return total_deleted


# === Приблизительные дубликаты ===

def find_near_duplicates(
        folder: Path,
        hash_method: str = "phash",
        hash_size: int = 8,
        threshold: int = 12,
        max_workers: int = 8,
        show_progress: bool = True,
) -> List[Tuple[Path, Path, int]]:
    """Находит пары приблизительных дубликатов."""
    import imagehash

    hash_functions = {
        "phash": imagehash.phash,
        "dhash": imagehash.dhash,
        "ahash": imagehash.average_hash,
        "whash": imagehash.whash,
    }
    hash_func = hash_functions[hash_method]

    image_files = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTS
    ]
    if not image_files:
        return []

    logger.info(f"🔍 Найдено изображений: {len(image_files):,}")
    image_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(compute_hash_and_resolution, f, hash_func, hash_size): f
            for f in image_files
        }

        if show_progress:
            try:
                from tqdm import tqdm
                with tqdm(total=len(futures), desc="📊 Хеширование", unit="файл") as pbar:
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            img_hash, w, h = result
                            f = futures[future]
                            image_data.append((f, img_hash, w * h))
                        pbar.update(1)
            except ImportError:
                pass

    pairs = []
    n = len(image_data)
    logger.info(f"🔍 Поиск приблизительных дубликатов (порог={threshold}) среди {n} изображений...")

    for i in range(n):
        path1, hash1_str, res1 = image_data[i]
        hash1 = imagehash.hex_to_hash(hash1_str)
        for j in range(i + 1, n):
            path2, hash2_str, res2 = image_data[j]
            hash2 = imagehash.hex_to_hash(hash2_str)
            distance = hash1 - hash2
            if distance <= threshold:
                pairs.append((path1, path2, distance))

    logger.info(f"✅ Найдено пар приблизительных дубликатов: {len(pairs)}")
    return pairs


def process_near_duplicates(
        pairs: List[Tuple[Path, Path, int]],
        dry_run: bool = True,
        interactive: bool = False,
) -> int:
    """Удаляет дубликаты из пар, оставляя изображение с бОльшим разрешением."""
    from PIL import Image

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a, b):
        parent[find(a)] = find(b)

    for path1, path2, _ in pairs:
        union(path1, path2)

    clusters = defaultdict(list)
    for path, _ in [(p, None) for p, _, _ in pairs] + [(p2, None) for _, p2, _ in pairs]:
        root = find(path)
        if path not in clusters[root]:
            clusters[root].append(path)

    total_deleted = 0
    deleted_paths = set()

    for cluster_files in clusters.values():
        if len(cluster_files) <= 1:
            continue

        files_with_res = []
        for fp in cluster_files:
            res = get_image_resolution(fp)
            area = res[0] * res[1] if res else 0
            files_with_res.append((fp, area))

        # Сортировка: сначала по площади, затем по имени
        files_with_res.sort(key=lambda x: (x[1], x[0].name), reverse=True)
        to_keep, _ = files_with_res[0]
        to_delete = [f for f, _ in files_with_res[1:] if f not in deleted_paths]

        if not to_delete:
            continue

        logger.info(f"\nКластер: {len(cluster_files)} файлов | Оставляем: {to_keep.name}")
        for fp in to_delete:
            action = "[DRY-RUN] 🔍 Будет удалён" if dry_run else "🗑️ Удалён"
            logger.info(f"  {action}: {fp.name}")

            if not dry_run:
                if interactive:
                    confirm = input(f"    Подтвердите удаление {fp.name}? [y/N]: ").strip().lower()
                    if confirm != "y":
                        logger.info(f"    Пропущен: {fp.name}")
                        continue
                try:
                    fp.unlink()
                    deleted_paths.add(fp)
                    total_deleted += 1
                except OSError as exc:
                    logger.error(f"    ❌ Ошибка удаления {fp.name}: {exc}")

    return total_deleted


# === Дубликаты по имени файла (новый режим) ===

def extract_filename_prefix(path: Path) -> Optional[str]:
    """
    Извлекает префикс из имени файла (часть до первого '_').
    Пример: "00a353d87e73daddc325fe7f9b6b5ff8_dups_360x360_8e33e8cd.webp"
            -> "00a353d87e73daddc325fe7f9b6b5ff8"
    """
    stem = path.stem  # Имя без расширения
    parts = stem.split('_', 1)  # Разделяем только по первому '_'
    if len(parts) < 2 or not parts[0]:
        logger.debug("⚠️  Не удалось извлечь префикс из %s", path.name)
        return None
    return parts[0]


def get_filename_quality_score(path: Path) -> int:
    """
    Возвращает числовой приоритет для типа изображения по имени файла.
    Чем выше значение — тем "качественнее" файл.
    """
    name_lower = path.name.lower()
    if "preview" in name_lower or "full" in name_lower:
        return 100
    elif "dups" in name_lower:
        return 50
    elif "thumb" in name_lower or "thumbnail" in name_lower:
        return 10
    else:
        return 25  # нейтральный приоритет для остальных


def find_duplicates_by_filename_prefix(
        folder: Path, max_workers: int = 8, show_progress: bool = True
) -> dict[str, List[Tuple[Path, int, int]]]:
    """
    Группирует изображения по хешу в начале имени файла (до первого '_').
    Возвращает словарь: {префикс: [(путь, площадь_изображения, приоритет_имени), ...]}
    """
    image_files = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTS
    ]
    if not image_files:
        logger.warning("📁 В папке не найдено поддерживаемых изображений")
        return {}

    logger.info(f"🔍 Найдено изображений: {len(image_files):,}")
    prefix_groups = defaultdict(list)

    def _process_file(path: Path) -> Optional[Tuple[str, int, int]]:
        """Извлекает префикс, вычисляет площадь и приоритет."""
        prefix = extract_filename_prefix(path)
        if not prefix:
            return None

        res = get_image_resolution(path)
        if res is None:
            return None
        width, height = res
        quality_score = get_filename_quality_score(path)
        return prefix, width * height, quality_score

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_file, f): f
            for f in image_files
        }

        if show_progress:
            try:
                from tqdm import tqdm
                with tqdm(total=len(futures), desc="📊 Анализ имён", unit="файл") as pbar:
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            prefix, area, quality = result
                            f = futures[future]
                            prefix_groups[prefix].append((f, area, quality))
                        pbar.update(1)
            except ImportError:
                show_progress = False
                logger.info("ℹ️  tqdm не установлен — прогресс будет текстовым")

        if not show_progress:
            total = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                if result:
                    prefix, area, quality = result
                    f = futures[future]
                    prefix_groups[prefix].append((f, area, quality))
                if i % 100 == 0 or i == total:
                    logger.info(f"  Прогресс: {i}/{total} ({i / total * 100:.1f}%)")

    # Фильтруем группы с одним файлом
    duplicates = {p: files for p, files in prefix_groups.items() if len(files) > 1}
    logger.info(f"✅ Найдено групп по имени файла: {len(duplicates)}")
    return duplicates


def process_duplicates_by_filename_prefix(
        duplicates: dict[str, List[Tuple[Path, int, int]]],
        dry_run: bool = True,
        interactive: bool = False,
) -> int:
    """
    Удаляет дубликаты по имени файла, оставляя изображение:
    1. С наибольшим разрешением (площадью)
    2. При одинаковом разрешении — с наибольшим приоритетом по имени (preview > dups > thumb)
    """
    total_deleted = 0

    for prefix, files in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True):
        # Сортировка: сначала по площади (убывание), затем по приоритету имени, затем по имени
        files.sort(key=lambda x: (x[1], x[2], x[0].name), reverse=True)
        to_keep, _, _ = files[0]
        to_delete = [f for f, _, _ in files[1:]]

        # Получаем реальное разрешение для логирования
        res_keep = get_image_resolution(to_keep)
        res_str = f"{res_keep[0]}x{res_keep[1]}" if res_keep else "N/A"

        logger.info(
            f"\nПрефикс {prefix[:8]}... | Группа: {len(files)} файлов | Оставляем: {to_keep.name} ({res_str})"
        )
        for fp in to_delete:
            res = get_image_resolution(fp)
            res_str = f"{res[0]}x{res[1]}" if res else "N/A"
            action = "[DRY-RUN] 🔍 Будет удалён" if dry_run else "🗑️ Удалён"
            logger.info(f"  {action}: {fp.name} ({res_str})")

            if not dry_run:
                if interactive:
                    confirm = input(f"    Подтвердите удаление {fp.name}? [y/N]: ").strip().lower()
                    if confirm != "y":
                        logger.info(f"    Пропущен: {fp.name}")
                        continue
                try:
                    fp.unlink()
                    total_deleted += 1
                except OSError as exc:
                    logger.error(f"    ❌ Ошибка удаления {fp.name}: {exc}")

    return total_deleted


# === CLI ===

def parse_args():
    parser = argparse.ArgumentParser(
        description="Поиск и удаление дубликатов изображений",
        epilog="Примеры:\n"
               "  python dedup.py ./images\n"
               "  python dedup.py ./images --mode filename --dry-run\n"
               "  python dedup.py ./images --mode both --hash-method phash --threshold 10",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("folder", nargs="?", help="Папка с изображениями (если не указана — запросится интерактивно)")
    parser.add_argument(
        "--mode",
        choices=["exact", "near", "both", "filename"],
        default=None,
        help="Режим: exact (точные), near (приблизительные), both (оба), filename (по имени файла)"
    )
    parser.add_argument("--hash-method", choices=["phash", "dhash", "ahash", "whash"], default="phash",
                        help="Метод хеширования для приблизительных дубликатов (по умолчанию: phash)")
    parser.add_argument("--hash-size", type=int, default=8, help="Размер хеша (4-16, по умолчанию: 8)")
    parser.add_argument("--threshold", type=int, default=12,
                        help="Порог расстояния для приблизительных дубликатов (по умолчанию: 12)")
    parser.add_argument("--max-workers", type=int, default=8, help="Количество потоков для обработки (по умолчанию: 8)")
    parser.add_argument("--dry-run", action="store_true", help="Режим предпросмотра (без удаления)")
    parser.add_argument("--interactive", action="store_true", help="Подтверждение каждого удаления")
    return parser.parse_args()


def main():
    args = parse_args()

    # Запрос папки, если не указана
    if args.folder:
        folder = Path(args.folder).resolve()
    else:
        print("=" * 70)
        print("🖼️  УДАЛЕНИЕ ДУБЛИКАТОВ ИЗОБРАЖЕНИЙ")
        print("=" * 70)
        folder_input = input("\n Перетащите папку сюда или введите путь: ").strip().strip('"')
        folder = Path(folder_input).resolve()

    if not folder.exists() or not folder.is_dir():
        logger.error(f"❌ Папка не найдена: {folder}")
        sys.exit(1)

    # Запрос режима, если не указан
    if args.mode:
        mode = args.mode
    else:
        print("\nВыберите режим поиска дубликатов:")
        print("  1) Точные дубликаты (быстро, по хешу)")
        print("  2) Приблизительные дубликаты (похожие изображения)")
        print("  3) Оба режима (сначала точные, потом приблизительные)")
        print("  4) По имени файла (группировка по хешу до первого '_')")
        choice = input("Введите 1, 2, 3 или 4: ").strip()
        mode_map = {"1": "exact", "2": "near", "3": "both", "4": "filename"}
        mode = mode_map.get(choice, "both")

    dry_run = args.dry_run
    if not dry_run and not args.folder:
        warn = "⚠️  ВНИМАНИЕ: Будут УДАЛЕНЫ файлы!" if not dry_run else "🔍 Режим предпросмотра (удаления не будет)"
        print(f"\n{warn}")
        confirm = input("Продолжить? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Отменено пользователем.")
            sys.exit(0)

    logger.info(f"\n📁 Папка: {folder}")
    logger.info(f"⚙️  Режим: {mode}")
    logger.info(f"🧵 Потоков: {args.max_workers}")
    logger.info(f"💧 Dry-run: {'ДА' if dry_run else 'НЕТ'}\n")

    total_deleted = 0

    # Точные дубликаты
    if mode in ("exact", "both"):
        logger.info("🔍 Поиск ТОЧНЫХ дубликатов...")
        duplicates = find_exact_duplicates(folder, max_workers=args.max_workers)
        if duplicates:
            deleted = process_exact_duplicates(duplicates, dry_run=dry_run, interactive=args.interactive)
            total_deleted += deleted
            if not dry_run:
                logger.info(f"✅ Удалено точных дубликатов: {deleted}")
        else:
            logger.info("✅ Точных дубликатов не найдено")

    # Приблизительные дубликаты
    if mode in ("near", "both"):
        logger.info(f"\n🔍 Поиск ПРИБЛИЗИТЕЛЬНЫХ дубликатов (метод={args.hash_method}, порог={args.threshold})...")
        pairs = find_near_duplicates(
            folder,
            hash_method=args.hash_method,
            hash_size=args.hash_size,
            threshold=args.threshold,
            max_workers=args.max_workers,
        )
        if pairs:
            deleted = process_near_duplicates(pairs, dry_run=dry_run, interactive=args.interactive)
            total_deleted += deleted
            if not dry_run:
                logger.info(f"✅ Удалено приблизительных дубликатов: {deleted}")
        else:
            logger.info("✅ Приблизительных дубликатов не найдено")

    # Дубликаты по имени файла
    if mode == "filename":
        logger.info("🔍 Поиск дубликатов ПО ИМЕНИ ФАЙЛА (группировка по хешу до первого '_')...")
        duplicates = find_duplicates_by_filename_prefix(folder, max_workers=args.max_workers)
        if duplicates:
            deleted = process_duplicates_by_filename_prefix(duplicates, dry_run=dry_run, interactive=args.interactive)
            total_deleted += deleted
            if not dry_run:
                logger.info(f"✅ Удалено дубликатов по имени: {deleted}")
        else:
            logger.info("✅ Дубликатов по имени файла не найдено")

    # Итог
    print("\n" + "=" * 70)
    print("✅ ОБРАБОТКА ЗАВЕРШЕНА")
    print("=" * 70)
    if dry_run:
        print(f"🔍 Режим предпросмотра — файлы НЕ удалены")
        print(f"📊 Найдено дубликатов для удаления: {total_deleted}")
    else:
        print(f"🗑️  Удалено дубликатов: {total_deleted}")
    print(f"📁 Обработана папка: {folder}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        # Проверка зависимостей
        try:
            import PIL  # noqa: F401
            import imagehash  # noqa: F401
        except ImportError:
            print("❌ Требуются зависимости: pip install Pillow imagehash")
            print("   Для поддержки WebP убедитесь, что установлена версия Pillow >= 7.0")
            sys.exit(1)

        # Проверка поддержки WebP в текущей версии Pillow
        try:
            from PIL import features

            if not features.check('webp'):
                logger.warning(
                    "⚠️  WebP не поддерживается текущей версией Pillow. Обновите: pip install --upgrade Pillow")
        except Exception:
            pass  # features может отсутствовать в старых версиях

        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Операция прервана пользователем.")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)