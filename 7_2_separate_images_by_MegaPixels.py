#!/usr/bin/env python3
"""
Сортировка изображений по мегапикселям — исправленная версия для Windows.
Гарантированное создание папок до копирования + защита от длинных путей.
"""

import sys
import os
import shutil
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Callable

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp"}


def enable_long_paths_windows():
    """Включает поддержку длинных путей (>260 символов) в Windows через реестр (только информирование)."""
    if sys.platform == "win32":
        print("ℹ️  Windows: для работы с длинными путями (>260 символов) убедитесь, что:")
        print("    • Windows 10 1607+ или Windows 11")
        print(
            "    • Включена политика: Компьютер\HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem\\LongPathsEnabled = 1")
        print("    • Или используйте короткий путь к папке назначения\n")


def get_megapixels(path: Path) -> float | None:
    """Возвращает количество мегапикселей изображения или None при ошибке."""
    try:
        from PIL import Image
        with Image.open(path) as img:
            width, height = img.size
            return (width * height) / 1_000_000
    except Exception:
        return None


def get_bucket_name_mp_rounded(mp: float, step: float = 0.1) -> str:
    """Группировка по округлённым значениям (0.1, 0.2, 0.3...)."""
    rounded = round(mp / step) * step
    return f"{rounded:.1f}_MPix".replace(".", "_")


def get_bucket_name_ranges(mp: float) -> str:
    """Группировка по диапазонам: 0-2, 2-5, 5-10, 10-20, 20+."""
    if mp < 2:
        return "0_2_MPix"
    elif mp < 5:
        return "2_5_MPix"
    elif mp < 10:
        return "5_10_MPix"
    elif mp < 20:
        return "10_20_MPix"
    else:
        return "20plus_MPix"


def get_bucket_name_coarse(mp: float) -> str:
    """Группировка по крупным диапазонам: 0-5, 5-10, 10+."""
    if mp < 5:
        return "0_5_MPix"
    elif mp < 10:
        return "5_10_MPix"
    else:
        return "10plus_MPix"


def get_bucket_name_very_coarse(mp: float) -> str:
    """Группировка по очень крупным диапазонам: <1, 1-3, 3-8, 8+."""
    if mp < 1:
        return "under_1_MPix"
    elif mp < 3:
        return "1_3_MPix"
    elif mp < 8:
        return "3_8_MPix"
    else:
        return "8plus_MPix"


STRATEGIES = {
    "1": ("Точный (0.1 Мп)", lambda mp: get_bucket_name_mp_rounded(mp, 0.1)),
    "2": ("Средний (0.5 Мп)", lambda mp: get_bucket_name_mp_rounded(mp, 0.5)),
    "3": ("Диапазоны 0-2/2-5/5-10/10-20/20+", get_bucket_name_ranges),
    "4": ("Крупные 0-5/5-10/10+", get_bucket_name_coarse),
    "5": ("Очень крупные <1/1-3/3-8/8+", get_bucket_name_very_coarse),
}


def safe_create_dir(path: Path) -> bool:
    """Надёжное создание директории с повторными попытками для Windows."""
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            path.mkdir(parents=True, exist_ok=True)
            # Дополнительная проверка для Windows
            if sys.platform == "win32" and not path.exists():
                time.sleep(0.05 * (attempt + 1))
                continue
            return True
        except Exception as e:
            if attempt == max_attempts - 1:
                print(f"❌ Критическая ошибка создания папки {path}: {e}")
                return False
            time.sleep(0.05 * (attempt + 1))
    return False


def process_file(
        file_path: Path,
        output_dir: Path,
        bucket_name: str,
        mode: str,
) -> Tuple[bool, str, str, float | None]:
    """Обрабатывает один файл с повторными попытками при ошибках ФС."""
    try:
        target_dir = output_dir / bucket_name

        # Гарантируем существование папки
        if not target_dir.exists():
            if not safe_create_dir(target_dir):
                return False, file_path.name, f"Не удалось создать папку: {target_dir}", None

        # Формируем целевой путь с разрешением конфликтов
        target_path = target_dir / file_path.name
        counter = 1
        while target_path.exists():
            stem = file_path.stem
            suffix = file_path.suffix
            target_path = target_dir / f"{stem}_{counter}{suffix}"
            counter += 1

        # Повторные попытки копирования/перемещения
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                if mode == "move":
                    shutil.move(str(file_path), str(target_path))
                else:
                    shutil.copy2(file_path, target_path)
                break
            except (FileNotFoundError, PermissionError) as e:
                if attempt < max_attempts - 1:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise
        else:
            raise RuntimeError("Превышено количество попыток операции ввода-вывода")

        mp = get_megapixels(file_path)
        return True, file_path.name, bucket_name, mp
    except Exception as e:
        return False, file_path.name, str(e), None


def sort_images(
        input_dir: Path,
        output_dir: Path,
        strategy_func: Callable[[float], str],
        mode: str = "copy",
        max_workers: int = 8,
        show_progress: bool = True,
) -> dict:
    """Основная функция сортировки изображений."""
    # Собираем изображения
    image_files: List[Path] = [
        f for f in input_dir.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTS
    ]

    if not image_files:
        print(f"⚠️  В папке {input_dir} не найдено изображений для обработки.")
        return {"total": 0, "buckets": {}, "failed": 0, "failed_classify": 0}

    print(f"🔍 Найдено изображений: {len(image_files):,}")
    print(f"⚙️  Режим: {'Копирование' if mode == 'copy' else 'Перемещение'}")
    print()

    # Классифицируем изображения и собираем уникальные бакеты
    tasks: List[Tuple[Path, str, float]] = []
    failed_classify = []
    bucket_names = set()

    for f in image_files:
        mp = get_megapixels(f)
        if mp is not None:
            bucket = strategy_func(mp)
            tasks.append((f, bucket, mp))
            bucket_names.add(bucket)
        else:
            failed_classify.append(f.name)

    if failed_classify:
        print(f"⚠️  Не удалось определить размер для {len(failed_classify)} файлов")
        if len(failed_classify) <= 5:
            for name in failed_classify:
                print(f"   • {name}")
        print()

    # === КРИТИЧЕСКИ ВАЖНО: создаём ВСЕ папки бакетов ДО запуска потоков ===
    print(f"📁 Создание {len(bucket_names)} папок групп...")
    created_buckets = []
    failed_buckets = []

    for bucket in sorted(bucket_names):
        bucket_path = output_dir / bucket
        if safe_create_dir(bucket_path):
            created_buckets.append(bucket)
        else:
            failed_buckets.append(bucket)

    if failed_buckets:
        print(f"❌ Не удалось создать {len(failed_buckets)} папок. Прерывание операции.")
        return {"total": 0, "buckets": {}, "failed": len(image_files), "failed_classify": len(failed_classify)}

    print(f"✅ Успешно создано папок: {len(created_buckets)}")
    print()

    # Обрабатываем файлы
    bucket_stats = {}
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_file, file_path, output_dir, bucket, mode)
            for file_path, bucket, _ in tasks
        ]

        if show_progress:
            try:
                from tqdm import tqdm
                with tqdm(total=len(futures), desc="🖼️  Сортировка", unit="файл") as pbar:
                    for future in as_completed(futures):
                        success, name, info, mp = future.result()
                        if success:
                            bucket_stats[info] = bucket_stats.get(info, 0) + 1
                        else:
                            failed += 1
                            if failed <= 5:  # Показываем первые 5 ошибок
                                print(f"\n❌ {name}: {info}")
                            elif failed == 6:
                                print("   ... дополнительные ошибки скрыты для краткости ...")
                        pbar.update(1)
            except ImportError:
                show_progress = False
                print("ℹ️  tqdm не установлен — прогресс будет текстовым\n")

        if not show_progress:
            total = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                success, name, info, mp = future.result()
                if success:
                    bucket_stats[info] = bucket_stats.get(info, 0) + 1
                else:
                    failed += 1
                if i % 50 == 0 or i == total:
                    print(f"  Прогресс: {i}/{total} ({i / total * 100:.1f}%)")

    return {
        "total": len(tasks),
        "buckets": dict(sorted(bucket_stats.items(), key=lambda x: x[0])),
        "failed": failed,
        "failed_classify": len(failed_classify),
    }


def main():
    print("=" * 70)
    print("📊 СОРТИРОВКА ИЗОБРАЖЕНИЙ ПО МЕГАПИКСЕЛЯМ (исправленная версия для Windows)")
    print("=" * 70)

    # Включаем информирование о длинных путях для Windows
    enable_long_paths_windows()

    # Запрос исходной папки
    while True:
        input_input = input(
            "\n Перетащите исходную папку сюда или введите путь: "
        ).strip().strip('"')
        input_dir = Path(input_input).resolve()

        if not input_dir.exists():
            print(f"❌ Папка не найдена: {input_dir}")
            continue
        if not input_dir.is_dir():
            print(f"❌ Это не папка: {input_dir}")
            continue
        break

    # Запрос выходной папки
    print("\nКуда сохранить отсортированные изображения?")
    print("  1) В подпапку 'sorted_by_mpix' внутри исходной папки")
    print("  2) В другую папку (укажите путь)")
    while True:
        out_choice = input("Введите 1 или 2: ").strip()
        if out_choice == "1":
            output_dir = input_dir / "sorted_by_mpix"
            break
        elif out_choice == "2":
            out_input = input("  Перетащите папку назначения или введите путь: ").strip().strip('"')
            output_dir = Path(out_input).resolve()
            if not safe_create_dir(output_dir):
                print(f"❌ Не удалось создать папку: {output_dir}")
                continue
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Выбор режима
    print("\nВыберите режим обработки:")
    print("  1) Копировать (исходные файлы останутся на месте)")
    print("  2) Переместить (исходные файлы будут удалены из исходной папки)")
    while True:
        mode_choice = input("Введите 1 или 2: ").strip()
        if mode_choice == "1":
            mode = "copy"
            break
        elif mode_choice == "2":
            mode = "move"
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Выбор стратегии группировки
    print("\nВыберите стратегию группировки по мегапикселям:")
    for key, (name, _) in STRATEGIES.items():
        print(f"  {key}) {name}")
    while True:
        strat_choice = input("Введите номер (1-5): ").strip()
        if strat_choice in STRATEGIES:
            strategy_name, strategy_func = STRATEGIES[strat_choice]
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Подтверждение
    print("\n" + "-" * 70)
    print(f"📁 Источник:   {input_dir}")
    print(f"📁 Назначение: {output_dir}")
    print(f"⚙️  Режим:     {'Копирование' if mode == 'copy' else 'Перемещение'}")
    print(f"📊 Стратегия:  {strategy_name}")
    print("-" * 70)

    confirm = input("\nНачать сортировку? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Отменено пользователем.")
        sys.exit(0)

    # Проверка зависимостей
    try:
        from PIL import Image  # noqa: F401
    except ImportError:
        print("\n❌ Требуется библиотека Pillow. Установите: pip install Pillow")
        sys.exit(1)

    # Запуск обработки
    print("\n🚀 Запуск сортировки...\n")
    results = sort_images(
        input_dir=input_dir,
        output_dir=output_dir,
        strategy_func=strategy_func,
        mode=mode,
        max_workers=min(16, (os.cpu_count() or 4) * 2),
        show_progress=True,
    )

    # Итоговый отчёт
    print("\n" + "=" * 70)
    print("✅ СОРТИРОВКА ЗАВЕРШЕНА")
    print("=" * 70)
    print(f"📁 Обработано изображений: {results['total']:,}")
    if results['failed_classify'] > 0:
        print(f"⚠️  Не удалось определить размер: {results['failed_classify']:,}")
    print(f"❌ Ошибки копирования/перемещения: {results['failed']:,}")
    print()

    # Сортируем бакеты для красивого вывода
    buckets = results["buckets"]
    if buckets:
        print("📊 Распределение по папкам:")

        # Парсим имена для сортировки по числовому значению
        def sort_key(name):
            try:
                if "plus" in name or "under" in name:
                    return float('inf')
                nums = [float(x) for x in name.replace("_MPix", "").replace("_", ".").split("_") if
                        x.replace(".", "").isdigit()]
                return nums[0] if nums else 0
            except:
                return 0

        for bucket in sorted(buckets.keys(), key=sort_key):
            count = buckets[bucket]
            pct = count / results["total"] * 100 if results["total"] > 0 else 0
            # Человекочитаемое имя папки
            pretty = bucket.replace("_MPix", " MPix").replace("_", ".")
            print(f"   • {pretty:25s} : {count:5d} файлов ({pct:5.1f}%)")

    print()
    print(f"📁 Результаты сохранены в: {output_dir}")
    print("=" * 70)

    if mode == "copy":
        print("\nℹ️  Исходные файлы остались в папке-источнике (режим копирования).")
    else:
        print("\nℹ️  Исходные файлы перемещены в папку назначения (режим перемещения).")

    if results['failed'] > 0:
        print(f"\n⚠️  {results['failed']} файлов не были обработаны. Проверьте ошибки выше.")
        if sys.platform == "win32":
            print("💡 Совет для Windows: используйте более короткий путь к папке назначения")
            print("   (например, C:\\sorted вместо длинного пути в пользовательской директории)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Операция прервана пользователем.")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)