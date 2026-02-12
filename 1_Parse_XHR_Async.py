import asyncio
import json
import os
import traceback
from pathlib import Path
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

# ========= НАСТРОЙКИ =========
IMAGES_FOLDER = Path(".")
XHR_FOLDER = "XHR"
MAX_TABS = 10
HEADLESS = True

os.makedirs(XHR_FOLDER, exist_ok=True)


# ---------- Логирование ошибок ----------
def log_error(stage, filename, exception):
    err_file = Path("errors.log")
    with err_file.open("a", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write(f"❌ ОШИБКА | Файл: {filename} | Этап: {stage}\n")
        f.write("-" * 80 + "\n")
        f.write(f"Тип ошибки: {type(exception).__name__}\n")
        f.write(f"Сообщение: {str(exception)}\n")
        f.write("Traceback:\n")
        f.write("".join(traceback.format_exception(type(exception), exception, exception.__traceback__)))
        f.write("\n" + "=" * 80 + "\n\n")


# ---------- Получение JPG файлов ----------
def get_jpg_files(folder_path):
    return [
        f for f in Path(folder_path).iterdir()
        if f.is_file() and f.suffix.lower() in [".jpg", ".jpeg"]
    ]


# ---------- Обработка XHR ----------
async def save_xhr(response, img_path, xhr_counter):
    url = response.url
    try:
        status = response.status
        headers = response.headers
        content_type = headers.get("content-type", "")

        # --- Игнорируем пустые ответы с 404 ---
        if status == 404:
            return False  # конец результатов — это норм

        text_body = await response.text()
        if not text_body.strip():
            raise ValueError(f"Пустой ответ (status={status})")

        base_filename = os.path.join(
            XHR_FOLDER,
            f"{img_path.stem}_xhr_{xhr_counter:03d}"
        )

        if "application/json" in content_type.lower():
            try:
                data = json.loads(text_body)
                with open(base_filename + ".json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except json.JSONDecodeError as je:
                with open(base_filename + "_CORRUPT_JSON.txt", "w", encoding="utf-8") as f:
                    f.write(text_body[:5000])
                raise Exception(f"Некорректный JSON: {je}")
        else:
            with open(base_filename + "_NOT_JSON.txt", "w", encoding="utf-8") as f:
                f.write(
                    f"URL: {url}\n"
                    f"STATUS: {status}\n"
                    f"CONTENT-TYPE: {content_type}\n\n"
                    f"{text_body[:5000]}"
                )
            raise ValueError(f"Ответ не JSON (content-type={content_type}, status={status})")

        return True
    except Exception as e:
        log_error("Сохранение XHR", img_path.name, e)
        return False


# ---------- Обработка одного изображения ----------
async def process_image(context, img_path, semaphore, progress_bar):
    async with semaphore:
        page = await context.new_page()
        xhr_counter = 0
        xhr_tasks = []

        # --- Обработчик XHR ---
        def handle_response(response):
            nonlocal xhr_counter
            if "yandex.ru/images/search" in response.url and "format=json" in response.url and "serpList%2Ffetch" in response.url:
                xhr_counter += 1
                xhr_tasks.append(asyncio.create_task(save_xhr(response, img_path, xhr_counter)))

        page.on("response", handle_response)

        try:
            await page.goto("https://yandex.ru/images/")
            await page.click('button[aria-label="Поиск по картинке"]')
            await page.wait_for_selector('.CbirPanel-Main', timeout=10000)

            await page.set_input_files('input[type="file"]', str(img_path))
            await page.wait_for_selector('.CbirNavigation-TabsItem_name_similar-page', timeout=20000)
            await page.click('.CbirNavigation-TabsItem_name_similar-page')
            await page.wait_for_selector('.SerpItem-Thumb a.Link', timeout=30000)

            # === Устойчивый цикл подгрузки "Показать ещё" ===
            while True:
                error_locator = page.locator('.FetchListNotice.FetchListButton-Error')
                if await error_locator.count() > 0:
                    break  # конец подгрузки с ошибкой

                show_more_locator = page.locator('.FetchListButton-Button:enabled')
                if await show_more_locator.count() == 0:
                    break  # кнопки больше нет

                try:
                    await show_more_locator.first.click(timeout=10000)  # увеличенный таймаут
                    await asyncio.sleep(0.3)  # ждём подгрузку
                except Exception:
                    if await show_more_locator.count() == 0:
                        break
                    continue

        except Exception as e:
            log_error("Подгрузка результатов", img_path.name, e)
        finally:
            page.remove_listener("response", handle_response)
            if xhr_tasks:
                await asyncio.gather(*xhr_tasks, return_exceptions=True)
            await page.close()
            progress_bar.update(1)


# ---------- Главная функция ----------
async def main():
    images = get_jpg_files(IMAGES_FOLDER)
    if not images:
        raise FileNotFoundError("❌ JPG файлы не найдены")

    print(f"🔍 Найдено изображений: {len(images)}")

    semaphore = asyncio.Semaphore(MAX_TABS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context()

        # --- Прогресс-бар с tqdm.asyncio ---
        progress_bar = tqdm(total=len(images), desc="Обработка изображений", unit="файл")

        tasks = [
            process_image(context, img_path, semaphore, progress_bar)
            for img_path in images
        ]

        await asyncio.gather(*tasks)
        await browser.close()
        progress_bar.close()

    print("\n🏁 Обработка всех изображений завершена!")


if __name__ == "__main__":
    asyncio.run(main())
