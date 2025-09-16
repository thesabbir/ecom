import re
import asyncio
from typing import List, Optional, Dict
from datetime import datetime
from ..models.product import Product, PriceUnit
from playwright.async_api import async_playwright, Browser
from bs4 import BeautifulSoup
from ..storage.parquet_storage import ParquetStorage
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class RyansCrawler:
    """Optimized parallel crawler for Ryans.com"""

    def __init__(self, max_concurrent: int = 5, storage_path: str = "data"):
        self.base_url = "https://www.ryans.com"
        self.site_name = "Ryans"
        self.categories_url = "https://www.ryans.com/categories"
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

                # Navigate to page with increased timeout and retry
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
                f"{category_url}?page={page_num}"
                if "?" not in category_url
                else f"{category_url}&page={page_num}"
            )

            logger.info(f"Fetching page {page_num}: {page_url}")
            html = await self.fetch_with_new_page(
                page_url, wait_for_selector="div.category-single-product"
            )

            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            containers = soup.find_all("div", class_="category-single-product")

            for container in containers:
                link = container.find("a", href=True)
                if link:
                    href = link["href"]
                    if not href.startswith("http"):
                        href = self.base_url + href
                    if href not in product_urls:
                        product_urls.append(href)

            # Check for next page
            if not soup.find("a", text=re.compile(r"Next|→")):
                break

        logger.info(f"Found {len(product_urls)} products")
        return product_urls

    async def extract_product_data(self, product_url: str) -> Optional[Product]:
        """Extract product data from a product page"""
        html = await self.fetch_with_new_page(product_url, wait_for_selector="h1")

        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        # Extract product ID
        product_id_match = re.search(r"/([^/]+)$", product_url.rstrip('/'))
        product_id = (
            f"ryans_{product_id_match.group(1)}"
            if product_id_match
            else f"ryans_{hash(product_url)}"
        )

        # Extract title
        title_elem = soup.find("h1", class_=re.compile(r"product-title|title", re.I))
        if not title_elem:
            title_elem = soup.find("h1")
        title = self.clean_text(title_elem.get_text()) if title_elem else None

        if not title:
            return None

        # Extract price
        price = None
        original_price = None

        # Look for price in various formats
        price_elem = soup.find(["span", "div"], class_=re.compile(r"price-new|product-price|price", re.I))
        if price_elem:
            price = self.extract_price(price_elem.get_text())

        # Look for original price
        old_price_elem = soup.find(["span", "div"], class_=re.compile(r"price-old|regular-price|original", re.I))
        if old_price_elem:
            original_price = self.extract_price(old_price_elem.get_text())

        # Extract description
        description = None
        desc_elem = soup.find(["div", "section"], class_=re.compile(r"description|overview|product-info", re.I))
        if desc_elem:
            description = self.clean_text(desc_elem.get_text())[:1000]  # Limit to 1000 chars

        # Extract images
        images = []
        main_image = None

        # Look for product images
        img_container = soup.find(["div", "section"], class_=re.compile(r"product-image|gallery|photo", re.I))
        if img_container:
            img_tags = img_container.find_all("img")
        else:
            img_tags = soup.find_all("img", class_=re.compile(r"product", re.I))

        for img in img_tags[:10]:  # Limit to 10 images
            img_src = img.get("src") or img.get("data-src")
            if img_src and not "logo" in img_src.lower():
                if not img_src.startswith("http"):
                    img_src = self.base_url + img_src
                images.append(img_src)
                if not main_image:
                    main_image = img_src

        # Extract specifications
        specifications = {}

        # Method 1: Look for tabbed specification content (Ryans uses tabs)
        # Try different possible IDs/classes for specification tabs
        spec_tab_selectors = [
            {'id': 'pills-specification'},
            {'id': 'specification'},
            {'id': 'specs'},
            {'class_': 'specification-tab'},
            {'class_': 'product-specification'},
            {'class_': 'tab-pane', 'id': re.compile(r'spec', re.I)}
        ]

        spec_content = None
        for selector in spec_tab_selectors:
            spec_content = soup.find('div', **selector)
            if spec_content:
                break

        # If we found specification tab content, extract from it
        if spec_content:
            # Look for tables within the specification tab
            tables = spec_content.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = self.clean_text(cells[0].get_text())
                        value = self.clean_text(cells[1].get_text())
                        if key and value and not key.lower() in ['price', 'status', 'availability']:
                            specifications[key] = value

        # Method 2: Fallback to general specification tables
        if not specifications:
            spec_tables = soup.find_all("table", class_=re.compile(r"spec|detail|feature", re.I))
            for table in spec_tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = self.clean_text(cells[0].get_text())
                        value = self.clean_text(cells[1].get_text())
                        if key and value and not key.lower() in ['price', 'status', 'availability']:
                            specifications[key] = value

        # Method 3: Look for key-value pairs in product info sections
        # Ryans often has specs in the main product details area
        product_info = soup.find_all(['div', 'section'], class_=re.compile(r'product-info|product-detail|key-feature', re.I))
        for info_section in product_info:
            # Look for list items with specifications
            spec_items = info_section.find_all(['li', 'p'])
            for item in spec_items:
                text = item.get_text()
                # Try to parse key-value pairs separated by colon
                if ":" in text:
                    parts = text.split(":", 1)
                    if len(parts) == 2:
                        key = self.clean_text(parts[0])
                        value = self.clean_text(parts[1])
                        if key and value and key not in specifications:
                            specifications[key] = value

        # Method 4: Extract from meta tags (often contains structured data)
        meta_specs = soup.find_all('meta', {'property': re.compile(r'product:', re.I)})
        for meta in meta_specs:
            content = meta.get('content', '')
            prop = meta.get('property', '').replace('product:', '')
            if prop and content and prop not in ['price', 'availability']:
                specifications[prop] = content

        # Method 5: Parse specifications from meta description
        # Ryans often puts specs in meta tags
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc:
            desc_content = meta_desc.get('content', '')
            # Look for processor info
            proc_match = re.search(r'Processor\s*(?:Brand|Type|Model|Generation)?\s*[-:]?\s*([^,\n]+)', desc_content, re.I)
            if proc_match and 'Processor' not in specifications:
                specifications['Processor'] = self.clean_text(proc_match.group(1))

            # Parse key-value pairs from meta description
            lines = desc_content.split('\n')
            for line in lines:
                if ' - ' in line:
                    parts = line.split(' - ', 1)
                    if len(parts) == 2:
                        key = self.clean_text(parts[0])
                        value = self.clean_text(parts[1])
                        if key and value and key not in specifications:
                            specifications[key] = value

        # Method 6: Parse from title/description for common specs
        title_lower = title.lower()
        if "ram" in title_lower or "gb" in title_lower:
            ram_match = re.search(r"(\d+)\s*GB\s*RAM", title, re.I)
            if ram_match and "RAM" not in specifications:
                specifications["RAM"] = ram_match.group(0)

        if "ssd" in title_lower or "hdd" in title_lower:
            storage_match = re.search(r"(\d+)\s*(GB|TB)\s*(SSD|HDD)", title, re.I)
            if storage_match and "Storage" not in specifications:
                specifications["Storage"] = storage_match.group(0)

        # Extract processor info from title
        if any(cpu in title_lower for cpu in ['intel', 'amd', 'ryzen', 'core']):
            # Try to extract processor model
            proc_patterns = [
                r"(Intel\s+Core\s+i[357]-\d+\w*)",
                r"(AMD\s+Ryzen\s+\d+\s+\d+\w*)",
                r"(Intel\s+Celeron\s+\w+)",
                r"(Intel\s+Pentium\s+\w+)"
            ]
            for pattern in proc_patterns:
                proc_match = re.search(pattern, title, re.I)
                if proc_match and "Processor" not in specifications:
                    specifications["Processor"] = proc_match.group(1)
                    break

        # Extract brand from title
        brand = None
        brand_keywords = ["Asus", "HP", "Dell", "Lenovo", "Acer", "MSI", "Apple", "Samsung", "LG", "Sony", "Toshiba", "Xiaomi", "Huawei", "Realme", "OnePlus"]
        for brand_name in brand_keywords:
            if brand_name.lower() in title.lower():
                brand = brand_name
                break

        # Extract category from breadcrumb or URL
        category = None
        breadcrumb = soup.find(["ol", "ul", "nav"], class_=re.compile(r"breadcrumb", re.I))
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if len(links) > 1:
                category = self.clean_text(links[-2].get_text())  # Usually second to last

        # Fallback: extract from URL
        if not category and "/laptop" in product_url.lower():
            category = "Laptop"
        elif not category and "/desktop" in product_url.lower():
            category = "Desktop"

        # Check stock status
        in_stock = True
        stock_elem = soup.find(text=re.compile(r"out of stock|unavailable", re.I))
        if stock_elem:
            in_stock = False

        # Calculate discount percentage
        discount_percentage = None
        if price and original_price and original_price > price:
            discount_percentage = round((original_price - price) / original_price * 100, 1)

        return Product(
            product_id=product_id,
            url=product_url,
            site_name=self.site_name,
            title=title,
            description=description,
            price=price or 0.0,
            original_price=original_price,
            currency=PriceUnit.BDT,
            discount_percentage=discount_percentage,
            brand=brand,
            category=category,
            images=images,
            main_image=main_image,
            in_stock=in_stock,
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

    async def get_all_categories(self) -> List[Dict[str, str]]:
        """Extract all category URLs from the categories page"""
        categories = []
        html = await self.fetch_with_new_page(
            self.categories_url, wait_for_selector='div[class*="category"]'
        )

        if not html:
            logger.error("Failed to fetch categories page")
            return categories

        soup = BeautifulSoup(html, "lxml")

        # Find all category containers
        category_containers = soup.find_all(
            "div", class_=re.compile(r"category-item")
        ) or soup.find_all("a", href=re.compile(r"/category/"))

        for container in category_containers:
            try:
                if container.name == "a":
                    link = container
                else:
                    link = container.find("a")

                if link and link.get("href"):
                    href = link["href"]
                    if not href.startswith("http"):
                        href = self.base_url + href

                    name = self.clean_text(link.get_text())
                    if name and "/category/" in href:
                        categories.append({"name": name, "url": href})
            except Exception as e:
                logger.error(f"Error extracting category: {e}")

        logger.info(f"Found {len(categories)} categories")
        return categories

    async def crawl_all_categories(
        self,
        max_pages_per_category: int = 5,
        skip_duplicates: bool = True,
        skip_if_scraped_within_hours: Optional[int] = 24,
        overwrite: bool = False,
    ) -> List[Product]:
        """Crawl all products from all categories with skip options"""
        all_products = []
        total_saved = 0

        # Clear data if overwrite mode
        if overwrite:
            logger.info("Overwrite mode: Clearing existing data")
            # Clear CSV file
            import os

            if os.path.exists(self.storage.csv_path):
                os.remove(self.storage.csv_path)
            self.storage._init_storage()

        # Get all categories
        categories = await self.get_all_categories()

        if not categories:
            logger.error("No categories found")
            return all_products

        logger.info(f"Starting to crawl {len(categories)} categories")
        logger.info(
            f"Settings: skip_duplicates={skip_duplicates}, skip_recent={skip_if_scraped_within_hours}h, overwrite={overwrite}"
        )

        # Get storage stats before crawling
        stats_before = self.storage.get_stats()
        logger.info(f"Storage before: {stats_before.get('total_products', 0)} products")

        # Crawl each category
        for i, category in enumerate(categories, 1):
            logger.info(f"Crawling category {i}/{len(categories)}: {category['name']}")

            try:
                products = await self.crawl_category_parallel(
                    category["url"],
                    max_pages=max_pages_per_category,
                    skip_duplicates=skip_duplicates,
                    skip_if_scraped_within_hours=skip_if_scraped_within_hours,
                )

                # Add category name to products
                for product in products:
                    if not product.category:
                        product.category = category["name"]

                all_products.extend(products)
                total_saved += len(products)

            except Exception as e:
                logger.error(f"Error crawling category {category['name']}: {e}")
                continue

        # Get storage stats after crawling
        stats_after = self.storage.get_stats()
        logger.info(f"Storage after: {stats_after.get('total_products', 0)} products")
        logger.info(
            f"Crawl complete: {total_saved} products saved, {stats_after.get('total_products', 0) - stats_before.get('total_products', 0)} new"
        )

        return all_products

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean text"""
        if not text:
            return None
        return " ".join(text.split()).strip()

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text"""
        if not price_text:
            return None
        import re

        price_text = re.sub(r"[^\d.,]", "", price_text)
        price_text = price_text.replace(",", "")
        try:
            return float(price_text)
        except ValueError:
            return None
