import logging
import sqlite3
from collections import Counter
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
import os


class DomainClassifier:
    """Classifies domains based on predefined service mappings."""

    def __init__(self) -> None:
        self._service_domains: Dict[str, str] = {
            'userapi.com': 'VK / userapi',
            'vkuserphoto.ru': 'VK / userapi',
            'mycdn.me': 'VK / userapi',
            'okcdn.ru': 'OK.ru',
            'googleusercontent.com': 'Google',
            'ytimg.com': 'Google',
            'yt3.googleusercontent.com': 'Google',
            'blogger.com': 'Google',
            'staticflickr.com': 'Flickr',
            'my.mail.ru': 'Mail.ru / My.Mail',
            'foto.my.mail.ru': 'Mail.ru / My.Mail',
            'fotokto.ru': 'Fotokto.ru',
            '.mt.ru': 'MT.ru',
            'yandex.net': 'Yandex',
            'yandex.ru': 'Yandex',
            'img-fotki.yandex.ru': 'Yandex',
            'avatars.dzeninfra.ru': 'Yandex',
            'icdn.ru': 'ICDN.ru',
            'imgbb.ru': 'ImgBB',
            'pinimg.com': 'Pinterest',
            'behance.net': 'Behance',
            'wikimedia.org': 'Wikimedia',
            'livejournal.com': 'LiveJournal',
            'imgur.com': 'Imgur',
            'mm.bing.net': 'Bing',
        }

        # Domains that require substring matching
        self._service_substrings: Dict[str, str] = {
            'mail.ru': 'Mail.ru / My.Mail',  # Special case: check for avt- or filed in domain
        }

    def classify(self, domain: str) -> str:
        """
        Classifies a domain into a service or returns the domain itself if not classified.

        Args:
            domain: The domain to classify.

        Returns:
            The service name if classified, otherwise the original domain.
        """
        domain_lower = domain.lower()

        # Check for exact matches first
        for service_domain, service_name in self._service_domains.items():
            if service_domain.startswith('.'):
                # Domain suffix match (e.g., .mt.ru)
                if domain_lower.endswith(service_domain):
                    return service_name
            elif service_domain == domain_lower:
                return service_name

        # Check for substring matches
        for service_substring, service_name in self._service_substrings.items():
            if service_substring in domain_lower:
                if service_substring == 'mail.ru':
                    if 'avt-' in domain_lower or 'filed' in domain_lower:
                        return service_name

        # Special substring checks
        if any(x in domain_lower for x in ['userapi.com', 'vkuserphoto.ru', 'mycdn.me']):
            return 'VK / userapi'
        if 'okcdn.ru' in domain_lower:
            return 'OK.ru'
        if any(x in domain_lower for x in [
            'googleusercontent.com',
            'ytimg.com',
            'yt3.googleusercontent.com',
            'blogger.com'
        ]):
            return 'Google'
        if 'staticflickr.com' in domain_lower:
            return 'Flickr'
        if any(x in domain_lower for x in ['my.mail.ru', 'foto.my.mail.ru']) or (
                'mail.ru' in domain_lower and ('avt-' in domain_lower or 'filed' in domain_lower)):
            return 'Mail.ru / My.Mail'
        if 'fotokto.ru' in domain_lower:
            return 'Fotokto.ru'
        if domain_lower.endswith('.mt.ru'):
            return 'MT.ru'
        if any(x in domain_lower for x in [
            'yandex.net',
            'yandex.ru',
            'img-fotki.yandex.ru',
            'avatars.dzeninfra.ru'
        ]):
            return 'Yandex'
        if 'icdn.ru' in domain_lower:
            return 'ICDN.ru'
        if 'imgbb.ru' in domain_lower:
            return 'ImgBB'
        if 'pinimg.com' in domain_lower:
            return 'Pinterest'
        if 'behance.net' in domain_lower:
            return 'Behance'
        if 'wikimedia.org' in domain_lower:
            return 'Wikimedia'
        if 'livejournal.com' in domain_lower:
            return 'LiveJournal'
        if 'imgur.com' in domain_lower:
            return 'Imgur'
        if 'mm.bing.net' in domain_lower:
            return 'Bing'

        return domain  # Return original domain if no match


class ImageUrlAnalyzer:
    """Analyzes image URLs from a database and provides classification statistics."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.classifier = DomainClassifier()
        self.logger = logging.getLogger(__name__)

    def _connect_to_database(self, path: str = None) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        """Establishes a connection to the SQLite database."""
        db_to_connect = path if path else self.db_path
        try:
            conn = sqlite3.connect(db_to_connect)
            cursor = conn.cursor()
            self.logger.info(f"Successfully connected to the database: {db_to_connect}")
            return conn, cursor
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise

    def _fetch_image_urls(self, cursor: sqlite3.Cursor) -> List[str]:
        """Fetches non-empty origUrl values from the images table."""
        query = "SELECT origUrl FROM images WHERE origUrl IS NOT NULL AND origUrl != ''"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            self.logger.info(f"Fetched {len(rows)} non-empty records from origUrl column.")
            return [row[0] for row in rows]
        except sqlite3.Error as e:
            self.logger.error(f"Query execution error: {e}")
            raise

    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalizes the URL by adding protocol if missing."""
        url = url.strip()
        if not url:
            return None
        if url.startswith("//"):
            return "https:" + url
        if url.startswith(("http://", "https://")):
            return url
        return None  # Invalid URL format

    def _parse_and_classify_urls(self, urls: List[str]) -> Tuple[Counter, Counter]:
        """Parses URLs, extracts domains, and classifies them."""
        main_services: List[str] = []
        other_domains: List[str] = []

        for url in urls:
            normalized_url = self._normalize_url(url)
            if not normalized_url:
                continue

            try:
                parsed = urlparse(normalized_url)
                domain = parsed.netloc.lower()
                if not domain:
                    continue

                service = self.classifier.classify(domain)
                if service == domain:
                    other_domains.append(domain)
                else:
                    main_services.append(service)
            except Exception as e:
                self.logger.warning(f"Failed to parse URL {url}: {e}")
                continue

        return Counter(main_services), Counter(other_domains)

    def analyze(self) -> Tuple[Counter, Counter]:
        """Performs the complete analysis and returns classified counters."""
        conn, cursor = self._connect_to_database()
        try:
            urls = self._fetch_image_urls(cursor)
            main_counter, other_counter = self._parse_and_classify_urls(urls)
            return main_counter, other_counter
        finally:
            conn.close()
            self.logger.info("Database connection closed.")

    def print_results(self, main_counter: Counter, other_counter: Counter, top_other_limit: int = 100) -> None:
        """Prints the analysis results in a formatted manner."""
        print("\n🏆 Топ-сервисов (по агрегированным CDN):")
        print("-" * 50)
        for service, count in main_counter.most_common():
            print(f"{service:<30} : {count:>6}")

        print(f"\n🌐 Всего основных сервисов: {len(main_counter)}")

        if other_counter:
            print(f"\n🔍 Прочие домены (не вошедшие в основные категории) — топ-{top_other_limit}:")
            print("-" * 60)
            for domain, count in other_counter.most_common(top_other_limit):
                print(f"{domain:<40} : {count:>4}")
            if len(other_counter) > top_other_limit:
                print(f"... и ещё {len(other_counter) - top_other_limit} других доменов.")
            print(f"\n📦 Всего 'прочих' уникальных доменов: {len(other_counter)}")

    def _chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def extract_vk_data_to_new_db(self, new_db_path: str = "vk_images.db", chunk_size: int = 500) -> None:
        """
        Извлекает все записи, связанные с ВК, и сохраняет их в новую базу данных.
        Использует chunking для избежания ошибки 'too many SQL variables'.
        """
        conn_old, cursor_old = self._connect_to_database()

        # Создаем новую БД
        conn_new, cursor_new = self._connect_to_database(path=new_db_path)

        # Создаем таблицы в новой БД (структура как в старой)
        # images (22 столбца)
        cursor_new.execute("""
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
        # image_variants (11 столбцов + 1 AUTOINCREMENT)
        cursor_new.execute("""
            CREATE TABLE IF NOT EXISTS image_variants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_id TEXT NOT NULL,
                variant_type TEXT NOT NULL,
                url TEXT,
                width INTEGER,
                height INTEGER,
                file_size_bytes INTEGER,
                is_mixed_image BOOLEAN,
                origin_url TEXT,
                origin_width INTEGER,
                origin_height INTEGER,
                FOREIGN KEY(image_id) REFERENCES images(id)
            )
        """)

        # Сначала находим все id изображений из таблицы images, которые связаны с ВК
        # Используем classify для проверки origUrl или image_url
        cursor_old.execute("SELECT id, origUrl, image_url FROM images")
        image_rows = cursor_old.fetchall()

        vk_image_ids = set()
        for row in image_rows:
            img_id, orig_url, img_url = row
            # Проверяем origUrl
            if orig_url:
                norm_orig_url = self._normalize_url(orig_url)
                if norm_orig_url:
                    try:
                        parsed = urlparse(norm_orig_url)
                        domain = parsed.netloc.lower()
                        service = self.classifier.classify(domain)
                        if 'VK' in service:  # Проверяем, содержит ли классификация 'VK'
                            vk_image_ids.add(img_id)
                            continue  # Нашли по origUrl, можно идти к следующей записи
                    except Exception:
                        pass
            # Если origUrl не дал результата, проверяем image_url
            if img_url:
                norm_img_url = self._normalize_url(img_url)
                if norm_img_url:
                    try:
                        parsed = urlparse(norm_img_url)
                        domain = parsed.netloc.lower()
                        service = self.classifier.classify(domain)
                        if 'VK' in service:  # Проверяем, содержит ли классификация 'VK'
                            vk_image_ids.add(img_id)
                    except Exception:
                        pass

        self.logger.info(f"Найдено {len(vk_image_ids)} изображений, связанных с ВК.")

        # Затем копируем эти изображения в новую БД
        if vk_image_ids:
            # Преобразуем set в list для chunking
            vk_image_ids_list = list(vk_image_ids)
            total_copied_images = 0
            total_copied_variants = 0

            # Проходим по чанкам
            for chunk_ids in self._chunks(vk_image_ids_list, chunk_size):
                placeholders = ','.join('?' for _ in chunk_ids)
                # Копируем изображения для чанка
                cursor_old.execute(f"SELECT * FROM images WHERE id IN ({placeholders})", chunk_ids)
                chunk_image_data = cursor_old.fetchall()

                if chunk_image_data:
                    insert_images_query = """
                        INSERT INTO images VALUES (
                            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                        )
                    """
                    cursor_new.executemany(insert_images_query, chunk_image_data)
                    total_copied_images += len(chunk_image_data)
                    self.logger.debug(f"Скопировано {len(chunk_image_data)} записей изображений для текущего чанка.")

            # Зафиксируем изменения для images
            conn_new.commit()
            self.logger.info(f"Всего скопировано {total_copied_images} записей изображений в новую БД.")

            # Теперь копируем связанные записи из image_variants по чанкам
            for chunk_ids in self._chunks(vk_image_ids_list, chunk_size):
                placeholders = ','.join('?' for _ in chunk_ids)
                cursor_old.execute(f"SELECT * FROM image_variants WHERE image_id IN ({placeholders})", chunk_ids)
                chunk_variant_data = cursor_old.fetchall()

                if chunk_variant_data:
                    insert_variants_query = """
                        INSERT INTO image_variants VALUES (
                            ?,?,?,?,?,?,?,?,?,?,?
                        )
                    """
                    cursor_new.executemany(insert_variants_query, chunk_variant_data)
                    total_copied_variants += len(chunk_variant_data)
                    self.logger.debug(f"Скопировано {len(chunk_variant_data)} записей вариантов изображений для текущего чанка.")

            # Зафиксируем изменения для image_variants
            conn_new.commit()
            self.logger.info(f"Всего скопировано {total_copied_variants} записей вариантов изображений в новую БД.")

        else:
            self.logger.info("Не найдено изображений, связанных с ВК.")

        conn_old.close()
        conn_new.close()
        self.logger.info(f"Экспорт данных ВК завершен. Новая БД: {new_db_path}")


def configure_logging() -> None:
    """Configures logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def main() -> None:
    """Main function to run the image URL analysis."""
    configure_logging()
    logger = logging.getLogger(__name__)

    db_path = 'yandex_images.db'

    try:
        analyzer = ImageUrlAnalyzer(db_path)

        # --- Новый функционал ---
        logger.info("Начинаем экспорт данных ВК в новую БД...")
        analyzer.extract_vk_data_to_new_db("vk_images.db")
        logger.info("Экспорт данных ВК завершен.")

        # --- Старый функционал ---
        main_counter, other_counter = analyzer.analyze()
        analyzer.print_results(main_counter, other_counter)
    except Exception as e:
        logger.error(f"An error occurred during analysis: {e}")
        return 1

    logger.info("Analysis completed successfully.")
    return 0


if __name__ == "__main__":
    exit(main())
