import re
import asyncio
from typing import List, Optional
from datetime import datetime
from ..models.product import Product, PriceUnit
from playwright.async_api import async_playwright, Browser
from bs4 import BeautifulSoup
from ..storage.parquet_storage import ParquetStorage
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class StarTechCrawler:
    """Optimized parallel crawler for StarTech.com.bd"""

    def __init__(self, max_concurrent: int = 5, storage_path: str = "data"):
        self.base_url = "https://www.startech.com.bd"
        self.site_name = "StarTech"
        self.rate_limit_delay = 0.1
        self.max_concurrent = max_concurrent
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.storage = ParquetStorage(storage_path)
        self._cleanup_scheduled = False

    async def __aenter__(self):
        await self.setup_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def setup_browser(self):
        """Initialize Playwright browser"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )

    async def close(self):
        """Close browser and cleanup resources"""
        try:
            if self.browser:
                await self.browser.close()
                self.browser = None
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

        try:
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
        except Exception as e:
            logger.error(f"Error stopping playwright: {e}")

    async def fetch_with_new_page(
        self, url: str, wait_for_selector: Optional[str] = None, max_retries: int = 2
    ) -> Optional[str]:
        """Fetch page content using a new browser page with retry logic"""
        for attempt in range(max_retries + 1):
            context = None
            page = None
            try:
                # Create new context for this request
                context = await self.browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                )

                page = await context.new_page()

                # Add anti-detection
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)

                # Navigate to page with increased timeout
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Wait for selector if provided
                if wait_for_selector:
                    await page.wait_for_selector(wait_for_selector, timeout=10000)

                # Quick scroll
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(0.1)

                # Get content
                content = await page.content()
                logger.info(f"Successfully fetched {url}")
                return content

            except Exception as e:
                logger.error(
                    f"Error fetching {url} (attempt {attempt + 1}/{max_retries + 1}): {e}"
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
                return None
            finally:
                # Clean up resources
                try:
                    if page:
                        await page.close()
                except Exception as e:
                    logger.error(f"Error closing page: {e}")

                try:
                    if context:
                        await context.close()
                except Exception as e:
                    logger.error(f"Error closing context: {e}")

        return None  # If all retries failed

    async def extract_product_urls(
        self, category_url: str, max_pages: int = 1
    ) -> List[str]:
        """Extract product URLs from a category"""
        product_urls = []

        for page_num in range(1, max_pages + 1):
            page_url = (
                f"{category_url}?page={page_num}" if page_num > 1 else category_url
            )

            logger.info(f"Fetching page {page_num}: {page_url}")
            html = await self.fetch_with_new_page(
                page_url, wait_for_selector="div.p-item"
            )

            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            containers = soup.find_all("div", class_="p-item")

            for container in containers:
                link = container.find("a", href=True)
                if link:
                    href = link["href"]
                    if not href.startswith("http"):
                        href = self.base_url + href
                    if href not in product_urls:
                        product_urls.append(href)

            # Check for next page
            pagination = soup.find("ul", class_="pagination")
            if pagination:
                next_page = pagination.find("a", string=str(page_num + 1))
                if not next_page:
                    break
            else:
                break

        logger.info(f"Found {len(product_urls)} products")
        return product_urls

    async def extract_product_data(self, product_url: str) -> Optional[Product]:
        """Extract product data from a product page"""
        html = await self.fetch_with_new_page(product_url, wait_for_selector="h1")

        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        # Extract product ID from URL
        product_id_match = re.search(r"/([^/]+)$", product_url.rstrip("/"))
        product_id = (
            f"startech_{product_id_match.group(1)}"
            if product_id_match
            else f"startech_{hash(product_url)}"
        )

        # Extract title
        title_elem = soup.find("h1", class_="product-name")
        if not title_elem:
            title_elem = soup.find("h1")
        title = self.clean_text(title_elem.get_text()) if title_elem else None

        if not title:
            return None

        # Extract price
        price = None
        original_price = None

        price_elem = soup.find("td", class_="product-info-price")
        if price_elem:
            price = self.extract_price(price_elem.get_text())

        regular_price_elem = soup.find("td", class_="regular-price")
        if regular_price_elem:
            original_price = self.extract_price(regular_price_elem.get_text())

        # Extract brand
        brand = None
        brand_label = soup.find("td", string=re.compile(r"Brand"))
        if brand_label:
            brand_value = brand_label.find_next_sibling("td")
            if brand_value:
                brand = self.clean_text(brand_value.get_text())

        # Extract SKU/Product Code
        sku = None
        code_label = soup.find("td", string=re.compile(r"Product Code"))
        if code_label:
            code_value = code_label.find_next_sibling("td")
            if code_value:
                sku = self.clean_text(code_value.get_text())

        # Extract stock status
        in_stock = False
        status_label = soup.find("td", string=re.compile(r"Status"))
        if status_label:
            status_value = status_label.find_next_sibling("td")
            if status_value:
                status_text = status_value.get_text().lower()
                in_stock = "in stock" in status_text

        # Extract description
        description = None
        desc_section = soup.find("section", id="description")
        if desc_section:
            description = self.clean_text(desc_section.get_text())[:500]

        # Extract images
        images = []
        img_container = soup.find("div", class_="product-img-holder")
        if img_container:
            img_tags = img_container.find_all("img")
            for img in img_tags:
                img_src = img.get("src")
                if img_src:
                    if not img_src.startswith("http"):
                        img_src = self.base_url + img_src
                    images.append(img_src)

        # Extract specifications
        specifications = {}
        spec_section = soup.find("section", id="specification")
        if spec_section:
            spec_tables = spec_section.find_all("table")
            for table in spec_tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        key = self.clean_text(cells[0].get_text())
                        value = self.clean_text(cells[1].get_text())
                        if key and value:
                            specifications[key] = value

        # Extract category
        category = None
        breadcrumb = soup.find("ol", class_="breadcrumb")
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if len(links) > 1:
                category = self.clean_text(links[-1].get_text())

        return Product(
            product_id=product_id,
            url=product_url,
            site_name=self.site_name,
            title=title,
            description=description,
            price=price or 0.0,
            original_price=original_price,
            currency=PriceUnit.BDT,
            brand=brand,
            category=category,
            images=images,
            main_image=images[0] if images else None,
            in_stock=in_stock,
            sku=sku,
            specifications=specifications,
            scraped_at=datetime.now(),
        )

    async def crawl_category_parallel(
        self,
        category_url: str,
        max_pages: int = 1,
        skip_duplicates: bool = True,
        skip_if_scraped_within_hours: Optional[int] = 24,
    ) -> List[Product]:
        """Crawl a category with parallel product extraction and immediate saving"""
        from datetime import datetime, timedelta

        # Get existing URLs and recent URLs for skip checking
        existing_urls = set()
        if skip_duplicates:
            existing_urls = self.storage.get_existing_product_urls(self.site_name)
            logger.info(f"Found {len(existing_urls)} existing product URLs to skip")

        recent_urls = set()
        if skip_if_scraped_within_hours:
            cutoff = datetime.now() - timedelta(hours=skip_if_scraped_within_hours)
            recent_urls = self.storage.get_urls_after_date(cutoff, self.site_name)
            logger.info(f"Found {len(recent_urls)} recently scraped URLs to skip")

        # Get product URLs from category
        all_product_urls = await self.extract_product_urls(category_url, max_pages)

        if not all_product_urls:
            return []

        # Filter out URLs we should skip BEFORE crawling
        urls_to_skip = existing_urls.union(recent_urls)
        product_urls = [url for url in all_product_urls if url not in urls_to_skip]

        skipped_before_crawl = len(all_product_urls) - len(product_urls)
        if skipped_before_crawl > 0:
            logger.info(f"Skipping {skipped_before_crawl} already crawled URLs")

        if not product_urls:
            logger.info("All products already crawled, nothing new to fetch")
            return []

        products = []
        saved_count = 0
        failed_count = 0

        # Process in batches
        for i in range(0, len(product_urls), self.max_concurrent):
            batch = product_urls[i : i + self.max_concurrent]

            logger.info(
                f"Processing batch {i // self.max_concurrent + 1} ({len(batch)} new products)"
            )

            # Create tasks for parallel execution
            tasks = [self.extract_product_data(url) for url in batch]

            # Wait for all tasks to complete
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process and save results immediately
            for result in batch_results:
                if isinstance(result, Product):
                    # Save immediately (no need to check duplicates again)
                    if self.storage.save_product_immediate(
                        result, skip_duplicates=False, skip_if_scraped_within_hours=None
                    ):
                        products.append(result)
                        saved_count += 1
                        logger.info(f"Saved: {result.title[:50]}...")
                    else:
                        failed_count += 1
                elif isinstance(result, Exception):
                    logger.error(f"Error: {result}")
                    failed_count += 1

            # Small delay between batches
            await asyncio.sleep(self.rate_limit_delay)

        logger.info(
            f"Category complete: {saved_count} saved, {skipped_before_crawl} skipped, {failed_count} failed"
        )
        return products

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean text"""
        if not text:
            return None
        return " ".join(text.split()).strip()

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text"""
        if not price_text:
            return None
        # Remove currency symbol and extract first number
        price_text = re.sub(r"[৳,]", "", price_text)
        match = re.search(r"\d+(?:\.\d+)?", price_text)
        if match:
            try:
                return float(match.group())
            except ValueError:
                return None
        return None
