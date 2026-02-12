#!/usr/bin/env python3
"""
Оценка качества изображений с защитой от утечек памяти и зависаний.
"""

import sys
import os
import json
import shutil
import sqlite3
import time
import gc
import psutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import torch

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp"}
MAX_IMAGE_SIZE = 1920  # Максимальный размер по большей стороне
TIMEOUT_SECONDS = 10.0  # Лимит времени обработки одного изображения
VRAM_WARNING_THRESHOLD = 85  # Процент использования VRAM для предупреждения
RAM_WARNING_THRESHOLD = 85  # Процент использования RAM для предупреждения


def check_memory_usage():
    """Проверяет использование VRAM и RAM, возвращает кортеж (vram_pct, ram_pct)"""
    ram_pct = psutil.virtual_memory().percent

    if torch.cuda.is_available():
        try:
            torch.cuda.synchronize()
            vram_total = torch.cuda.get_device_properties(0).total_memory
            vram_allocated = torch.cuda.memory_allocated(0)
            vram_reserved = torch.cuda.memory_reserved(0)
            vram_used = vram_allocated + vram_reserved
            vram_pct = (vram_used / vram_total) * 100
            return vram_pct, ram_pct
        except:
            return 0.0, ram_pct
    return 0.0, ram_pct


def safe_empty_cache():
    """Безопасная очистка кэша CUDA и сборка мусора"""
    try:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            torch.cuda.synchronize()
        gc.collect()
    except Exception as e:
        print(f"⚠️  Ошибка при очистке памяти: {e}")


def resize_image_if_needed(image_path: Path, max_size: int = MAX_IMAGE_SIZE) -> Optional[Path]:
    """Ресайзит изображение до максимального размера, сохраняя пропорции. Возвращает путь к временному файлу или оригиналу."""
    from PIL import Image

    try:
        with Image.open(image_path) as img:
            img = img.convert('RGB')  # Универсальный формат для моделей
            width, height = img.size

            # Проверяем, нужно ли уменьшать
            if max(width, height) <= max_size:
                return image_path

            # Вычисляем новые размеры с сохранением пропорций
            if width > height:
                new_width = max_size
                new_height = int(height * max_size / width)
            else:
                new_height = max_size
                new_width = int(width * max_size / height)

            # Ресайз с качественной интерполяцией
            img = img.resize((new_width, new_height), Image.LANCZOS)

            # Создаем временный файл
            temp_path = image_path.with_name(f"{image_path.stem}_resized{image_path.suffix}")
            img.save(temp_path, quality=95, optimize=True)
            return temp_path

    except Exception as e:
        print(f"⚠️  Ошибка ресайза {image_path.name}: {e}")
        return image_path  # Возвращаем оригинал при ошибке


def check_dependencies():
    """Проверяет наличие всех необходимых зависимостей."""
    missing = []
    try:
        import PIL  # noqa: F401
    except ImportError:
        missing.append("Pillow")

    try:
        import torch  # noqa: F401
    except ImportError:
        missing.append("torch")

    try:
        import pyiqa  # noqa: F401
    except ImportError:
        missing.append("pyiqa")

    try:
        import psutil  # noqa: F401
    except ImportError:
        missing.append("psutil")

    if missing:
        print("❌ Отсутствуют необходимые зависимости:")
        for pkg in missing:
            print(f"   • {pkg}")
        print("\nУстановите командой:")
        print("   pip install Pillow torch pyiqa psutil")
        if "torch" in missing:
            print("\n💡 Для GPU-версии torch: https://pytorch.org/get-started/locally/")
        sys.exit(1)

    # Проверка CUDA
    import torch
    print(f"✅ Доступные устройства: {'CUDA' if torch.cuda.is_available() else 'CPU'}")
    if torch.cuda.is_available():
        print(f"   GPU: {torch.cuda.get_device_name(0)}")
    else:
        print("   ⚠️  GPU недоступен — будет использоваться CPU (медленнее)")


def get_device():
    """Определяет устройство для моделей (GPU/CPU)."""
    import torch
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def load_models(use_musiq: bool, use_clipiqa: bool, device) -> Tuple[Optional[Any], Optional[Any]]:
    """Загружает выбранные модели с прогрессом."""
    import pyiqa

    musiq_model = None
    clipiqa_model = None

    if use_musiq:
        print("⏳ Загрузка модели MUSIQ (может занять 1-2 минуты при первом запуске)...")
        try:
            musiq_model = pyiqa.create_metric('musiq', device=device, as_loss=False)
            print("✅ MUSIQ загружена")
        except Exception as e:
            print(f"⚠️  Не удалось загрузить MUSIQ: {e}")
            print("   Продолжаем без этой модели")

    if use_clipiqa:
        print("⏳ Загрузка модели CLIPIQA...")
        try:
            clipiqa_model = pyiqa.create_metric('clipiqa', device=device, as_loss=False)
            print("✅ CLIPIQA загружена")
        except Exception as e:
            print(f"⚠️  Не удалось загрузить CLIPIQA: {e}")
            print("   Продолжаем без этой модели")

    if not musiq_model and not clipiqa_model:
        print("❌ Ни одна модель не загружена. Выход.")
        sys.exit(1)

    return musiq_model, clipiqa_model


def assess_quality(
        image_path: Path,
        musiq_model,
        clipiqa_model,
        device,
        use_musiq: bool,
        use_clipiqa: bool,
) -> Dict[str, Any]:
    """Оценивает качество изображения выбранными моделями с защитой от ошибок памяти."""
    import torch

    result = {
        "musiq": None,
        "clipiqa_raw": None,
        "clipiqa": None,
        "max_score": 0.0,
        "error": None
    }

    try:
        # MUSIQ
        if use_musiq and musiq_model is not None:
            try:
                with torch.no_grad():
                    score = musiq_model(str(image_path)).item()
                result["musiq"] = round(score, 2)
            except RuntimeError as e:
                if "out of memory" in str(e).lower():
                    result["error"] = f"CUDA OOM при обработке MUSIQ. Попробуйте уменьшить размер изображений."
                else:
                    result["error"] = f"MUSIQ error: {str(e)[:100]}"
            except Exception as e:
                result["error"] = f"MUSIQ error: {str(e)[:100]}"

        # CLIPIQA
        if use_clipiqa and clipiqa_model is not None:
            try:
                with torch.no_grad():
                    raw_score = clipiqa_model(str(image_path)).item()
                result["clipiqa_raw"] = round(raw_score, 4)
                result["clipiqa"] = round(raw_score * 100.0, 2)
            except RuntimeError as e:
                if "out of memory" in str(e).lower() and not result["error"]:
                    result["error"] = f"CUDA OOM при обработке CLIPIQA. Попробуйте уменьшить размер изображений."
                elif not result["error"]:
                    result["error"] = f"CLIPIQA error: {str(e)[:100]}"
            except Exception as e:
                if not result["error"]:
                    result["error"] = f"CLIPIQA error: {str(e)[:100]}"

        # Выбираем максимальный скор
        scores = []
        if result["musiq"] is not None:
            scores.append(result["musiq"])
        if result["clipiqa"] is not None:
            scores.append(result["clipiqa"])

        if scores:
            result["max_score"] = round(max(scores), 2)

        return result

    except Exception as e:
        result["error"] = f"Critical error: {str(e)[:100]}"
        result["max_score"] = 0.0
        return result


def get_image_info(image_path: Path) -> Dict[str, Any]:
    """Получает техническую информацию об изображении."""
    from PIL import Image

    try:
        with Image.open(image_path) as img:
            width, height = img.size
            format = img.format
            mode = img.mode
        file_size = image_path.stat().st_size
        return {
            "width": width,
            "height": height,
            "format": format,
            "mode": mode,
            "file_size": file_size
        }
    except Exception as e:
        return {
            "width": None,
            "height": None,
            "format": None,
            "mode": None,
            "file_size": None,
            "error": str(e)
        }


def create_database(db_path: Path) -> sqlite3.Connection:
    """Создаёт БД для хранения результатов."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS image_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL UNIQUE,
            width INTEGER,
            height INTEGER,
            format TEXT,
            mode TEXT,
            file_size INTEGER,
            musiq_score REAL,
            clipiqa_raw_score REAL,
            clipiqa_score REAL,
            max_score REAL,
            category TEXT,
            error_message TEXT,
            processing_time REAL,
            vram_usage REAL,
            ram_usage REAL,
            processed_at TEXT
        )
    """)
    conn.commit()
    return conn


def process_image(
        file_path: Path,
        musiq_model,
        clipiqa_model,
        device,
        use_musiq: bool,
        use_clipiqa: bool,
        max_size: int = MAX_IMAGE_SIZE
) -> Tuple[str, Dict[str, Any]]:
    """Обрабатывает одно изображение с ресайзом и мониторингом памяти."""
    # Ресайз изображения при необходимости
    resized_path = resize_image_if_needed(file_path, max_size)
    needs_cleanup = (resized_path != file_path)

    try:
        # Замер использования памяти ДО обработки
        vram_before, ram_before = check_memory_usage()

        # Получаем информацию об изображении (из оригинала)
        image_info = get_image_info(file_path)

        # Оцениваем качество (из ресайзнутой версии)
        start_time = time.time()
        quality_result = assess_quality(
            resized_path, musiq_model, clipiqa_model, device,
            use_musiq, use_clipiqa
        )
        processing_time = time.time() - start_time

        # Замер использования памяти ПОСЛЕ обработки
        vram_after, ram_after = check_memory_usage()

        # Определяем категорию по max_score
        max_score = quality_result.get("max_score", 0)
        if max_score >= 65.0:
            category = "high"
        elif max_score >= 50.0:
            category = "medium"
        else:
            category = "low"

        result = {
            **image_info,
            **quality_result,
            "category": category,
            "path": str(file_path),
            "processing_time": round(processing_time, 2),
            "vram_usage": round(vram_after, 1),
            "ram_usage": round(ram_after, 1)
        }

        # Проверка на превышение лимита времени
        if processing_time > TIMEOUT_SECONDS:
            if not result.get("error"):
                result["error"] = f"Превышено время обработки: {processing_time:.1f} сек (лимит {TIMEOUT_SECONDS} сек)"
            else:
                result["error"] += f" | Время: {processing_time:.1f} сек"

        # Проверка на критическое использование памяти
        if vram_after > VRAM_WARNING_THRESHOLD:
            result["error"] = (result.get("error", "") + f" | VRAM usage high: {vram_after:.1f}%").strip(" | ")
        if ram_after > RAM_WARNING_THRESHOLD:
            result["error"] = (result.get("error", "") + f" | RAM usage high: {ram_after:.1f}%").strip(" | ")

        return file_path.name, result

    finally:
        # Удаляем временный файл ресайза
        if needs_cleanup and resized_path.exists():
            try:
                resized_path.unlink()
            except:
                pass


def sort_files(
        results: Dict[str, Any],
        source_dir: Path,
        output_dir: Path,
        mode: str = "copy"
) -> Dict[str, int]:
    """Сортирует файлы по папкам на основе категории качества."""
    stats = {"high": 0, "medium": 0, "low": 0, "failed": 0}

    for filename, data in results.items():
        if "error" in data and data["error"]:
            stats["failed"] += 1
            continue

        category = data.get("category", "low")
        target_dir = output_dir / category
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / filename

        src_path = source_dir / filename
        if not src_path.exists():
            continue

        try:
            if mode == "move":
                shutil.move(str(src_path), str(target_path))
            else:
                shutil.copy2(src_path, target_path)
            stats[category] += 1
        except Exception as e:
            print(f"⚠️  Ошибка сортировки {filename}: {e}")
            stats["failed"] += 1

    return stats


def main():
    print("=" * 70)
    print("🖼️  ОЦЕНКА КАЧЕСТВА ИЗОБРАЖЕНИЙ (защита от утечек памяти)")
    print("=" * 70)

    # Проверка зависимостей
    print("\n🔍 Проверка зависимостей...")
    check_dependencies()

    # Выбор папки с изображениями
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

        # Проверка наличия изображений
        image_files = [
            f for f in source_dir.iterdir()
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTS
        ]
        if not image_files:
            print(f"⚠️  В папке не найдено поддерживаемых изображений {list(SUPPORTED_EXTS)}")
            continue

        print(f"✅ Найдено изображений: {len(image_files):,}")
        break

    # Выбор моделей
    print("\nВыберите модели для оценки качества:")
    print("  1) Только MUSIQ (рекомендуется для общего качества)")
    print("  2) Только CLIPIQA (быстрее, но менее точна)")
    print("  3) Обе модели (будет использован максимальный скор)")
    while True:
        model_choice = input("Введите 1, 2 или 3: ").strip()
        if model_choice == "1":
            use_musiq, use_clipiqa = True, False
            models_str = "MUSIQ"
            break
        elif model_choice == "2":
            use_musiq, use_clipiqa = False, True
            models_str = "CLIPIQA"
            break
        elif model_choice == "3":
            use_musiq, use_clipiqa = True, True
            models_str = "MUSIQ + CLIPIQA"
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Выбор режима сортировки
    print("\nВыберите режим сортировки:")
    print("  1) Копировать (исходные файлы сохранятся)")
    print("  2) Переместить (исходные файлы будут удалены из исходной папки)")
    while True:
        mode_choice = input("Введите 1 или 2: ").strip()
        if mode_choice == "1":
            sort_mode = "copy"
            break
        elif mode_choice == "2":
            sort_mode = "move"
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Выбор папки назначения
    print("\nКуда сохранить отсортированные изображения?")
    default_output = source_dir / "quality_sorted"
    print(f"  1) В подпапку 'quality_sorted' внутри исходной папки ({default_output.name})")
    print("  2) В другую папку (укажите путь)")
    while True:
        out_choice = input("Введите 1 или 2: ").strip()
        if out_choice == "1":
            output_dir = default_output
            break
        elif out_choice == "2":
            out_input = input("  Перетащите папку назначения или введите путь: ").strip().strip('"')
            output_dir = Path(out_input).resolve()
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
                if not output_dir.is_dir():
                    raise NotADirectoryError
            except Exception as e:
                print(f"❌ Не удалось создать папку: {e}")
                continue
            break
        else:
            print("Неверный выбор. Попробуйте снова.")

    # Подтверждение
    print("\n" + "-" * 70)
    print(f"📁 Источник:   {source_dir}")
    print(f"📁 Назначение: {output_dir}")
    print(f"🧠 Модели:     {models_str}")
    print(f"⚙️  Режим:     {'Копирование' if sort_mode == 'copy' else 'Перемещение'}")
    print(f"🖼️  Макс. размер изображения: {MAX_IMAGE_SIZE}px по большей стороне")
    print(f"⏱️  Таймаут обработки: {TIMEOUT_SECONDS} сек на изображение")
    print("-" * 70)

    confirm = input("\nНачать оценку качества? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Отменено пользователем.")
        sys.exit(0)

    # Загрузка моделей
    print("\n" + "=" * 70)
    device = get_device()
    musiq_model, clipiqa_model = load_models(use_musiq, use_clipiqa, device)
    print("=" * 70)

    # Подготовка БД
    db_path = output_dir / "quality_report.db"
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = create_database(db_path)
    cursor = conn.cursor()

    # Обработка изображений
    print("\n🚀 Начало обработки изображений...")
    print(f"   ⚠️  Защита активна: ресайз до {MAX_IMAGE_SIZE}px, таймаут {TIMEOUT_SECONDS} сек\n")
    results = {}
    total = len(image_files)
    start_time = time.time()
    timeout_count = 0
    oom_count = 0

    try:
        # Прогресс-бар
        try:
            from tqdm import tqdm
            progress_iter = tqdm(image_files, desc="📊 Оценка качества", unit="файл")
        except ImportError:
            print("ℹ️  tqdm не установлен — прогресс будет текстовым")
            progress_iter = image_files

        for i, file_path in enumerate(progress_iter, 1):
            filename = file_path.name
            data = {}

            try:
                # Обработка с замером времени
                start_img = time.time()
                filename, data = process_image(
                    file_path, musiq_model, clipiqa_model, device,
                    use_musiq, use_clipiqa, MAX_IMAGE_SIZE
                )
                elapsed = time.time() - start_img

                # Проверка таймаута
                if elapsed > TIMEOUT_SECONDS:
                    timeout_count += 1
                    safe_empty_cache()
                    if 'tqdm' in sys.modules:
                        from tqdm import tqdm
                        tqdm.write(
                            f"⏱️  {filename}: пропущено (обработка {elapsed:.1f} сек > лимит {TIMEOUT_SECONDS} сек)")
                    else:
                        print(f"⏱️  {filename}: пропущено (обработка {elapsed:.1f} сек > лимит {TIMEOUT_SECONDS} сек)")

                # Проверка ошибок памяти
                if data.get("error") and "out of memory" in str(data["error"]).lower():
                    oom_count += 1
                    safe_empty_cache()

            except Exception as e:
                error_msg = f"Необработанная ошибка: {str(e)[:150]}"
                data = {
                    "error": error_msg,
                    "max_score": 0.0,
                    "category": "low",
                    "processing_time": round(time.time() - start_img, 2),
                    "vram_usage": 0.0,
                    "ram_usage": 0.0
                }
                safe_empty_cache()
                if 'tqdm' in sys.modules:
                    from tqdm import tqdm
                    tqdm.write(f"❌ {filename}: {error_msg}")
                else:
                    print(f"❌ {filename}: {error_msg}")

            # Сохранение в БД
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO image_quality 
                    (filename, width, height, format, mode, file_size,
                     musiq_score, clipiqa_raw_score, clipiqa_score, max_score, category,
                     error_message, processing_time, vram_usage, ram_usage, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    filename,
                    data.get("width"),
                    data.get("height"),
                    data.get("format"),
                    data.get("mode"),
                    data.get("file_size"),
                    data.get("musiq"),
                    data.get("clipiqa_raw"),
                    data.get("clipiqa"),
                    data.get("max_score"),
                    data.get("category"),
                    data.get("error"),
                    data.get("processing_time", 0.0),
                    data.get("vram_usage", 0.0),
                    data.get("ram_usage", 0.0),
                    datetime.now().isoformat()
                ))
                conn.commit()
            except Exception as e:
                print(f"⚠️  Ошибка сохранения в БД для {filename}: {e}")

            results[filename] = data

            # Периодическая очистка памяти (каждые 10 изображений)
            if i % 10 == 0:
                safe_empty_cache()

            # Текстовый прогресс без tqdm
            if 'tqdm' not in sys.modules and i % 50 == 0:
                elapsed_total = time.time() - start_time
                speed = i / elapsed_total if elapsed_total > 0 else 0
                eta = (total - i) / speed if speed > 0 else 0
                print(f"  {i}/{total} ({i / total * 100:.1f}%) | {speed:.1f} файлов/сек | Осталось: {eta / 60:.1f} мин")

    finally:
        conn.close()
        safe_empty_cache()  # Финальная очистка

    elapsed_total = time.time() - start_time
    print(f"\n✅ Обработка завершена за {elapsed_total / 60:.1f} минут ({elapsed_total / total:.2f} сек/файл)")
    if timeout_count > 0:
        print(f"⏱️  Пропущено изображений по таймауту (> {TIMEOUT_SECONDS} сек): {timeout_count}")
    if oom_count > 0:
        print(f"MemoryWarning Пропущено изображений из-за нехватки памяти: {oom_count}")

    # Сортировка файлов
    print("\n📁 Сортировка файлов по папкам...")
    sort_stats = sort_files(results, source_dir, output_dir, sort_mode)

    # Сохранение JSON-отчёта
    json_path = output_dir / "quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Итоговый отчёт
    print("\n" + "=" * 70)
    print("✅ ОЦЕНКА КАЧЕСТВА ЗАВЕРШЕНА")
    print("=" * 70)

    # Статистика по категориям
    categories = {"high": [], "medium": [], "low": []}
    errors = []
    for data in results.values():
        if data.get("error"):
            errors.append(data)
        else:
            cat = data.get("category", "low")
            if cat in categories:
                categories[cat].append(data)

    total_processed = sum(len(v) for v in categories.values())
    print(f"📁 Обработано изображений: {total_processed:,} из {total:,}")
    print(f"⚠️  Ошибок обработки:      {len(errors):,}")
    if timeout_count > 0 or oom_count > 0:
        print(f"⏱️  Пропущено по таймауту/OOM: {timeout_count + oom_count:,}")
    print()

    for cat, items in categories.items():
        if items:
            scores = [d["max_score"] for d in items if d.get("max_score") is not None]
            avg = sum(scores) / len(scores) if scores else 0
            pct = len(items) / total_processed * 100 if total_processed > 0 else 0
            label = {"high": "Высокое (≥65)", "medium": "Среднее (50-64)", "low": "Низкое (<50)"}[cat]
            print(f"   • {label:25s} : {len(items):5d} файлов ({pct:5.1f}%) | Средний скор: {avg:5.1f}")

    print()
    print(f"💾 Результаты сохранены:")
    print(f"   • База данных:  {db_path}")
    print(f"   • JSON-отчёт:   {json_path}")
    print(f"   • Отсортированные изображения:")
    for cat in ["high", "medium", "low"]:
        cat_dir = output_dir / cat
        if cat_dir.exists():
            print(f"      - {cat_dir}")

    print("=" * 70)

    if sort_mode == "copy":
        print("\nℹ️  Исходные файлы остались в папке-источнике (режим копирования).")
    else:
        print("\nℹ️  Исходные файлы перемещены в папку назначения (режим перемещения).")

    # Советы по улучшению
    print("\n💡 Советы:")
    print(f"   • Все изображения автоматически ресайзятся до {MAX_IMAGE_SIZE}px для защиты от утечек памяти")
    print("   • При обработке очень больших изображений (>50 МП) рекомендуется уменьшить MAX_IMAGE_SIZE в коде")
    if timeout_count > 0:
        print("   ⚠️  Некоторые изображения обрабатывались дольше 10 сек — проверьте их размер и сложность")
    if oom_count > 0:
        print("   ⚠️  Обнаружены ошибки нехватки памяти — уменьшите размер изображений или используйте CPU")
    if not torch.cuda.is_available():
        print("   • ⚠️  Используется CPU — обработка может быть медленной")
        print("      Рассмотрите установку GPU-версии PyTorch для ускорения")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Операция прервана пользователем.")
        safe_empty_cache()
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        safe_empty_cache()
        import traceback

        traceback.print_exc()
        sys.exit(1)