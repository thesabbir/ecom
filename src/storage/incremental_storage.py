import os
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from typing import List, Optional, Set
from ..models.product import Product
import logging

logger = logging.getLogger(__name__)


class IncrementalCSVStorage:
    """Storage that saves data immediately and supports skip options"""

    def __init__(self, base_path: str = "data"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.db_path = os.path.join(base_path, "products.duckdb")
        self.csv_path = os.path.join(base_path, "products.csv")
        self._init_storage()

    def _init_storage(self):
        """Initialize storage with CSV and DuckDB"""
        # Create CSV with headers if it doesn't exist
        if not os.path.exists(self.csv_path):
            df = pd.DataFrame(columns=[
                'product_id', 'url', 'site_name', 'title', 'price',
                'currency', 'brand', 'category', 'scraped_at'
            ])
            df.to_csv(self.csv_path, index=False)
            logger.info(f"Created new CSV file: {self.csv_path}")

    def get_existing_product_ids(self, site_name: Optional[str] = None) -> Set[str]:
        """Get set of existing product IDs for duplicate checking"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT product_id
                    FROM read_csv_auto('{self.csv_path}')
                """
                if site_name:
                    query += f" WHERE site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result}
        except Exception as e:
            logger.warning(f"Could not fetch existing IDs: {e}")
            return set()

    def get_existing_product_urls(self, site_name: Optional[str] = None) -> Set[str]:
        """Get set of existing product URLs for duplicate checking before crawling"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT url
                    FROM read_csv_auto('{self.csv_path}')
                """
                if site_name:
                    query += f" WHERE site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result}
        except Exception as e:
            logger.warning(f"Could not fetch existing URLs: {e}")
            return set()

    def get_products_after_date(self, cutoff_date: datetime, site_name: Optional[str] = None) -> Set[str]:
        """Get product IDs that were scraped after a certain date"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT product_id
                    FROM read_csv_auto('{self.csv_path}')
                    WHERE scraped_at >= '{cutoff_date.isoformat()}'
                """
                if site_name:
                    query += f" AND site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result}
        except Exception as e:
            logger.warning(f"Could not fetch products by date: {e}")
            return set()

    def get_urls_after_date(self, cutoff_date: datetime, site_name: Optional[str] = None) -> Set[str]:
        """Get product URLs that were scraped after a certain date"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT url
                    FROM read_csv_auto('{self.csv_path}')
                    WHERE scraped_at >= '{cutoff_date.isoformat()}'
                """
                if site_name:
                    query += f" AND site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result}
        except Exception as e:
            logger.warning(f"Could not fetch URLs by date: {e}")
            return set()

    def save_product_immediate(self, product: Product, skip_duplicates: bool = True,
                              skip_if_scraped_within_hours: Optional[int] = None) -> bool:
        """Save a single product immediately to CSV"""
        try:
            # Check skip conditions
            if skip_duplicates:
                existing_ids = self.get_existing_product_ids(product.site_name)
                if product.product_id in existing_ids:
                    logger.debug(f"Skipping duplicate: {product.product_id}")
                    return False

            if skip_if_scraped_within_hours:
                cutoff = datetime.now() - timedelta(hours=skip_if_scraped_within_hours)
                recent_ids = self.get_products_after_date(cutoff, product.site_name)
                if product.product_id in recent_ids:
                    logger.debug(f"Skipping recently scraped: {product.product_id}")
                    return False

            # Convert product to DataFrame row
            product_dict = {
                'product_id': product.product_id,
                'url': str(product.url),
                'site_name': product.site_name,
                'title': product.title,
                'description': product.description,
                'price': product.price,
                'original_price': product.original_price,
                'currency': product.currency.value,
                'discount_percentage': product.discount_percentage,
                'brand': product.brand,
                'category': product.category,
                'subcategory': product.subcategory,
                'tags': ','.join(product.tags) if product.tags else None,
                'main_image': str(product.main_image) if product.main_image else None,
                'rating': product.rating,
                'review_count': product.review_count,
                'in_stock': product.in_stock,
                'stock_quantity': product.stock_quantity,
                'sku': product.sku,
                'scraped_at': product.scraped_at.isoformat()
            }

            # Append to CSV
            df = pd.DataFrame([product_dict])
            df.to_csv(self.csv_path, mode='a', header=False, index=False)
            logger.info(f"Saved product: {product.title[:50]}...")
            return True

        except Exception as e:
            logger.error(f"Error saving product {product.product_id}: {e}")
            return False

    def save_products_batch(self, products: List[Product], skip_duplicates: bool = True,
                           skip_if_scraped_within_hours: Optional[int] = None) -> int:
        """Save multiple products, returning count of saved items"""
        saved_count = 0
        for product in products:
            if self.save_product_immediate(product, skip_duplicates, skip_if_scraped_within_hours):
                saved_count += 1
        return saved_count

    def clear_old_data(self, days_to_keep: int = 30):
        """Remove data older than specified days"""
        try:
            cutoff = datetime.now() - timedelta(days=days_to_keep)

            # Read current data
            df = pd.read_csv(self.csv_path)
            df['scraped_at'] = pd.to_datetime(df['scraped_at'])

            # Filter to keep only recent data
            df_filtered = df[df['scraped_at'] >= cutoff]

            # Save back
            df_filtered.to_csv(self.csv_path, index=False)

            removed = len(df) - len(df_filtered)
            logger.info(f"Removed {removed} old records")
            return removed

        except Exception as e:
            logger.error(f"Error clearing old data: {e}")
            return 0

    def get_stats(self) -> dict:
        """Get storage statistics"""
        try:
            with duckdb.connect(self.db_path) as conn:
                stats = conn.execute(f"""
                    SELECT
                        COUNT(*) as total_products,
                        COUNT(DISTINCT site_name) as total_sites,
                        COUNT(DISTINCT category) as total_categories,
                        MIN(scraped_at) as oldest_record,
                        MAX(scraped_at) as newest_record
                    FROM read_csv_auto('{self.csv_path}')
                """).fetchone()

                return {
                    'total_products': int(stats[0]) if stats[0] is not None else 0,
                    'total_sites': int(stats[1]) if stats[1] is not None else 0,
                    'total_categories': int(stats[2]) if stats[2] is not None else 0,
                    'oldest_record': stats[3],
                    'newest_record': stats[4]
                }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}