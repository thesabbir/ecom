from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, Page, Browser
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime
import logging
import random
from ..models.product import Product

logger = logging.getLogger(__name__)


class PlaywrightCrawler(ABC):
    """Base crawler using Playwright for JavaScript-heavy sites with anti-bot protection"""

    def __init__(self, base_url: str, site_name: str):
        self.base_url = base_url
        self.site_name = site_name
        self.browser: Optional[Browser] = None
        self.pages: List[Page] = []  # Multiple pages for parallel processing
        self.page_semaphore = None  # Limit concurrent pages
        self.playwright = None
        self.rate_limit_delay = 0.3  # Reduced delay for faster crawling
        self.max_pages = 5  # Maximum concurrent browser pages

    async def __aenter__(self):
        await self.setup_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def setup_browser(self):
        """Initialize Playwright browser with anti-detection settings"""
        self.playwright = await async_playwright().start()

        # Launch browser with anti-detection settings
        self.browser = await self.playwright.chromium.launch(
            headless=True,  # Set to False for debugging
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
        )

        # Create context with anti-detection settings
        context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation'],
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )

        self.page = await context.new_page()

        # Add anti-detection scripts
        await self.page.add_init_script("""
            // Override the navigator.webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Override navigator.plugins to look more realistic
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Override navigator.languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)

    async def close(self):
        """Close browser and cleanup"""
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def fetch_page(self, url: str, wait_for_selector: Optional[str] = None) -> Optional[str]:
        """Fetch page content using Playwright"""
        try:
            # Minimal delay between requests
            await asyncio.sleep(self.rate_limit_delay)

            # Navigate to page (domcontentloaded is faster than networkidle)
            await self.page.goto(url, wait_until='domcontentloaded', timeout=15000)

            # Wait for specific selector if provided
            if wait_for_selector:
                await self.page.wait_for_selector(wait_for_selector, timeout=5000)

            # Quick scroll to trigger lazy loading
            await self.quick_scroll()

            # Get page content
            content = await self.page.content()
            logger.info(f"Successfully fetched {url}")
            return content

        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    async def quick_scroll(self):
        """Quick scroll to trigger lazy loading"""
        try:
            # Just 2 quick scrolls to trigger lazy loading
            await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
            await asyncio.sleep(0.1)
            await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.debug(f"Scroll error (non-critical): {e}")

    async def random_scroll(self):
        """Legacy method for compatibility"""
        await self.quick_scroll()

    async def click_and_wait(self, selector: str, wait_after: float = 1.0):
        """Click an element and wait"""
        try:
            await self.page.click(selector)
            await asyncio.sleep(wait_after)
        except Exception as e:
            logger.error(f"Error clicking {selector}: {e}")

    async def wait_for_element(self, selector: str, timeout: int = 10000) -> bool:
        """Wait for an element to appear"""
        try:
            await self.page.wait_for_selector(selector, timeout=timeout)
            return True
        except:
            return False

    async def extract_text(self, selector: str) -> Optional[str]:
        """Extract text from an element"""
        try:
            element = await self.page.query_selector(selector)
            if element:
                text = await element.text_content()
                return text.strip() if text else None
        except Exception as e:
            logger.debug(f"Error extracting text from {selector}: {e}")
        return None

    async def extract_attribute(self, selector: str, attribute: str) -> Optional[str]:
        """Extract attribute from an element"""
        try:
            element = await self.page.query_selector(selector)
            if element:
                return await element.get_attribute(attribute)
        except Exception as e:
            logger.debug(f"Error extracting attribute {attribute} from {selector}: {e}")
        return None

    async def handle_popup(self):
        """Handle common popups and modals"""
        try:
            # Common popup close button selectors
            close_selectors = [
                'button[aria-label*="close"]',
                'button[aria-label*="Close"]',
                '.modal-close',
                '.popup-close',
                '.close-button',
                '[data-dismiss="modal"]'
            ]

            for selector in close_selectors:
                if await self.page.query_selector(selector):
                    await self.page.click(selector)
                    await asyncio.sleep(0.5)
                    break
        except Exception as e:
            logger.debug(f"No popup to close or error: {e}")

    def parse_html(self, html: str) -> BeautifulSoup:
        """Parse HTML content"""
        return BeautifulSoup(html, 'lxml')

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text"""
        if not price_text:
            return None

        import re
        # Remove currency symbols and non-numeric characters except dots and commas
        price_text = re.sub(r'[^\d.,]', '', price_text)
        # Remove commas (thousands separator)
        price_text = price_text.replace(',', '')

        try:
            return float(price_text)
        except ValueError:
            return None

    def extract_rating(self, rating_text: str) -> Optional[float]:
        """Extract rating from text"""
        if not rating_text:
            return None

        import re
        match = re.search(r'(\d+\.?\d*)', rating_text)
        if match:
            try:
                rating = float(match.group(1))
                return min(rating, 5.0)  # Cap at 5.0
            except ValueError:
                pass
        return None

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean and normalize text"""
        if not text:
            return None
        return ' '.join(text.split()).strip()

    @abstractmethod
    async def extract_product_urls(self, category_url: str, max_pages: int = 1) -> List[str]:
        """Extract product URLs from category page"""
        pass

    @abstractmethod
    async def extract_product_data(self, product_url: str) -> Optional[Product]:
        """Extract product data from product page"""
        pass