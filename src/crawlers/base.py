from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import httpx
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime
import logging
from ..models.product import Product, PriceUnit, ProductCondition

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    def __init__(self, base_url: str, site_name: str):
        self.base_url = base_url
        self.site_name = site_name
        self.session = httpx.AsyncClient(
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            },
            timeout=30.0,
            follow_redirects=True
        )
        self.rate_limit_delay = 1.0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.session.aclose()

    async def fetch_page(self, url: str) -> Optional[str]:
        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await self.session.get(url)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def parse_html(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, 'lxml')

    @abstractmethod
    async def extract_product_urls(self, category_url: str, max_pages: int = 1) -> List[str]:
        pass

    @abstractmethod
    async def extract_product_data(self, product_url: str) -> Optional[Product]:
        pass

    async def crawl_products(self, category_url: str, max_pages: int = 1) -> List[Product]:
        products = []
        product_urls = await self.extract_product_urls(category_url, max_pages)

        for url in product_urls:
            product = await self.extract_product_data(url)
            if product:
                products.append(product)
                logger.info(f"Extracted: {product.title[:50]}...")

        return products

    def extract_price(self, price_text: str) -> Optional[float]:
        if not price_text:
            return None

        import re
        price_text = re.sub(r'[^\d.,]', '', price_text)
        price_text = price_text.replace(',', '')

        try:
            return float(price_text)
        except ValueError:
            return None

    def extract_rating(self, rating_text: str) -> Optional[float]:
        if not rating_text:
            return None

        import re
        match = re.search(r'(\d+\.?\d*)', rating_text)
        if match:
            try:
                rating = float(match.group(1))
                return min(rating, 5.0)
            except ValueError:
                pass
        return None

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        return ' '.join(text.split()).strip()


class GenericCrawler(BaseCrawler):
    def __init__(self, base_url: str):
        site_name = base_url.split('//')[1].split('/')[0].replace('www.', '')
        super().__init__(base_url, site_name)

    async def extract_product_urls(self, category_url: str, max_pages: int = 1) -> List[str]:
        product_urls = []
        html = await self.fetch_page(category_url)

        if not html:
            return product_urls

        soup = self.parse_html(html)

        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'product' in href.lower() or 'item' in href.lower():
                if not href.startswith('http'):
                    href = self.base_url + href
                product_urls.append(href)

        return list(set(product_urls))[:50]

    async def extract_product_data(self, product_url: str) -> Optional[Product]:
        html = await self.fetch_page(product_url)

        if not html:
            return None

        soup = self.parse_html(html)

        title = None
        for tag in ['h1', 'h2']:
            element = soup.find(tag)
            if element:
                title = self.clean_text(element.get_text())
                break

        if not title:
            return None

        price = None
        price_patterns = [
            {'class': re.compile(r'price', re.I)},
            {'itemprop': 'price'},
            {'data-price': True}
        ]

        for pattern in price_patterns:
            element = soup.find(attrs=pattern)
            if element:
                price_text = element.get_text()
                price = self.extract_price(price_text)
                if price:
                    break

        if not price:
            price = 0.0

        description = None
        desc_element = soup.find(attrs={'class': re.compile(r'description', re.I)})
        if desc_element:
            description = self.clean_text(desc_element.get_text())[:500]

        images = []
        for img in soup.find_all('img', src=True)[:10]:
            src = img['src']
            if not src.startswith('http'):
                src = self.base_url + src
            if 'product' in src.lower() or 'item' in src.lower():
                images.append(src)

        return Product(
            product_id=f"{self.site_name}_{hash(product_url)}",
            url=product_url,
            site_name=self.site_name,
            title=title,
            description=description,
            price=price,
            currency=PriceUnit.USD,
            images=images[:5] if images else [],
            main_image=images[0] if images else None,
            scraped_at=datetime.now()
        )