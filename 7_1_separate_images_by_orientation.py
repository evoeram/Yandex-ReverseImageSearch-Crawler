#!/usr/bin/env python3
"""
Разделение изображений по ориентации: альбомная (landscape), книжная (portrait), квадратная (square).
Без конфигов — всё управление через интерактивный CLI.
"""

import sys
import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif"}


def get_image_orientation(path: Path) -> str:
    """Определяет ориентацию изображения: 'landscape', 'portrait' или 'square'."""
    try:
        from PIL import Image
        with Image.open(path) as img:
            width, height = img.size
            if width > height:
                return "landscape"
            elif height > width:
                return "portrait"
            else:
                return "square"
    except Exception as e:
        raise ValueError(f"Не удалось открыть как изображение: {e}")


def process_file(
        file_path: Path,
        target_dir: Path,
        mode: str,  # "move" или "copy"
) -> Tuple[bool, str, str]:
    """Обрабатывает один файл: копирует или перемещает в целевую папку."""
    try:
        target_path = target_dir / file_path.name

        # Если файл с таким именем уже существует — добавляем суффикс
        counter = 1
        while target_path.exists():
            stem = file_path.stem
            suffix = file_path.suffix
            target_path = target_dir / f"{stem}_{counter}{suffix}"
            counter += 1

        if mode == "move":
            shutil.move(str(file_path), str(target_path))
        else:  # copy
            shutil.copy2(file_path, target_path)

        return True, file_path.name, target_path.parent.name
    except Exception as e:
        return False, file_path.name, str(e)


def separate_images(
        source_dir: Path,
        mode: str = "move",
        handle_square: str = "separate",  # "separate", "landscape", "portrait"
        max_workers: int = 8,
        show_progress: bool = True,
) -> dict:
    """Основная функция разделения изображений."""
    # Создаём целевые папки
    landscape_dir = source_dir / "landscape"
    portrait_dir = source_dir / "portrait"
    square_dir = source_dir / "square" if handle_square == "separate" else None

    landscape_dir.mkdir(exist_ok=True)
    portrait_dir.mkdir(exist_ok=True)
    if square_dir:
        square_dir.mkdir(exist_ok=True)

    # Собираем изображения
    image_files: List[Path] = [
        f for f in source_dir.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTS
    ]

    if not image_files:
        print(f"⚠️  В папке {source_dir} не найдено изображений для обработки.")
        return {"total": 0, "landscape": 0, "portrait": 0, "square": 0, "failed": 0}

    print(f"🔍 Найдено изображений: {len(image_files):,}")
    print(f"⚙️  Режим: {'Перемещение' if mode == 'move' else 'Копирование'}")
    print(f"📦 Квадратные изображения: {handle_square}")
    print()

    # Классифицируем изображения
    tasks: List[Tuple[Path, Path]] = []
    failed_classify = []

    for f in image_files:
        try:
            orientation = get_image_orientation(f)

            if orientation == "square":
                if handle_square == "separate":
                    target = square_dir
                elif handle_square == "landscape":
                    target = landscape_dir
                else:  # portrait
                    target = portrait_dir
            elif orientation == "landscape":
                target = landscape_dir
            else:  # portrait
                target = portrait_dir

            tasks.append((f, target))
        except Exception as e:
            failed_classify.append((f.name, str(e)))

    if failed_classify:
        print(f"⚠️  Не удалось определить ориентацию для {len(failed_classify)} файлов:")
        for name, err in failed_classify[:5]:  # Показываем первые 5 ошибок
            print(f"   • {name}: {err}")
        if len(failed_classify) > 5:
            print(f"   ... и ещё {len(failed_classify) - 5} файлов")
        print()

    # Обрабатываем файлы
    results = {"landscape": 0, "portrait": 0, "square": 0, "failed": 0}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_file, file_path, target_dir, mode)
            for file_path, target_dir in tasks
        ]

        if show_progress:
            try:
                from tqdm import tqdm
                with tqdm(total=len(futures), desc="🖼️  Обработка", unit="файл") as pbar:
                    for future in as_completed(futures):
                        success, name, info = future.result()
                        if success:
                            if info == "landscape":
                                results["landscape"] += 1
                            elif info == "portrait":
                                results["portrait"] += 1
                            elif info == "square":
                                results["square"] += 1
                        else:
                            results["failed"] += 1
                            print(f"\n❌ Ошибка: {name} → {info}")
                        pbar.update(1)
            except ImportError:
                show_progress = False
                print("ℹ️  tqdm не установлен — прогресс будет текстовым\n")

        if not show_progress:
            total = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                success, name, info = future.result()
                if success:
                    if info == "landscape":
                        results["landscape"] += 1
                    elif info == "portrait":
                        results["portrait"] += 1
                    elif info == "square":
                        results["square"] += 1
                else:
                    results["failed"] += 1
                if i % 50 == 0 or i == total:
                    print(f"  Прогресс: {i}/{total} ({i / total * 100:.1f}%)")

    results["total"] = len(tasks)
    return results


def main():
    print("=" * 70)
    print("🖼️  РАЗДЕЛЕНИЕ ИЗОБРАЖЕНИЙ ПО ОРИЕНТАЦИИ")
    print("=" * 70)

    # Запрос пути к папке
    while True:
        folder_input = input(
            "\n Перетащите папку с изображениями сюда или введите путь: "
        ).strip().strip('"')
        source_dir = Path(folder_input).resolve()

        if not source_dir.exists():
            print(f"❌ Папка не найдена: {source_dir}")
            continue
        if not source_dir.is_dir():
            print(f"❌ Это не папка: {source_dir}")
            continue
        break

    # Запрос режима работы
    print("\nВыберите режим обработки:")
    print("  1) Переместить файлы (исходные файлы будут удалены из папки)")
    print("  2) Скопировать файлы (исходные файлы останутся на месте)")
    while True:
        mode_choice = input("Введите 1 или 2: ").strip()
        if mode_choice == "1":
            mode = "move"
            break
        elif mode_choice == "2":
            mode = "copy"
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Запрос обработки квадратных изображений
    print("\nКак обрабатывать квадратные изображения (1:1)?")
    print("  1) В отдельную папку 'square' (рекомендуется)")
    print("  2) Считать альбомными (в папку 'landscape')")
    print("  3) Считать книжными (в папку 'portrait')")
    while True:
        square_choice = input("Введите 1, 2 или 3: ").strip()
        handle_square_map = {"1": "separate", "2": "landscape", "3": "portrait"}
        if square_choice in handle_square_map:
            handle_square = handle_square_map[square_choice]
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Подтверждение
    print("\n" + "-" * 70)
    print(f"📁 Источник:   {source_dir}")
    print(f"⚙️  Режим:     {'Перемещение' if mode == 'move' else 'Копирование'}")
    print(f"📦 Квадратные: {handle_square}")
    print("-" * 70)

    confirm = input("\nНачать обработку? [y/N]: ").strip().lower()
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
    print("\n🚀 Запуск обработки...\n")
    results = separate_images(
        source_dir=source_dir,
        mode=mode,
        handle_square=handle_square,
        max_workers=min(16, (os.cpu_count() or 4) * 2),
        show_progress=True,
    )

    # Итоговый отчёт
    print("\n" + "=" * 70)
    print("✅ ОБРАБОТКА ЗАВЕРШЕНА")
    print("=" * 70)
    print(f"📁 Обработано изображений: {results['total']:,}")
    print(f"   • Альбомная ориентация:  {results['landscape']:,}")
    print(f"   • Книжная ориентация:    {results['portrait']:,}")
    if handle_square == "separate":
        print(f"   • Квадратные:            {results['square']:,}")
    print(f"   • Ошибки обработки:      {results['failed']:,}")
    print()
    print(f"📁 Результаты сохранены в подпапках:")
    print(f"   • {source_dir / 'landscape'}")
    print(f"   • {source_dir / 'portrait'}")
    if handle_square == "separate":
        print(f"   • {source_dir / 'square'}")
    print("=" * 70)

    if mode == "copy":
        print("\nℹ️  Исходные файлы остались в основной папке (режим копирования).")
    else:
        print("\nℹ️  Исходные файлы перемещены в подпапки (режим перемещения).")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Операция прервана пользователем.")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)