import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Set
from ..models.product import Product
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class ParquetStorage:
    """Storage using Parquet format with DuckDB for efficient querying"""

    def __init__(self, base_path: str = "data"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.parquet_path = os.path.join(base_path, "products.parquet")
        self.db_path = os.path.join(base_path, "products.duckdb")
        self._init_storage()

    def _init_storage(self):
        """Initialize storage with Parquet file"""
        if not os.path.exists(self.parquet_path):
            # Create empty Parquet file with schema
            df = pd.DataFrame(
                columns=[
                    "product_id",
                    "url",
                    "site_name",
                    "title",
                    "price",
                    "currency",
                    "brand",
                    "category",
                    "scraped_at",
                    "description",
                    "original_price",
                    "discount_percentage",
                    "subcategory",
                    "tags",
                    "images",
                    "main_image",
                    "rating",
                    "review_count",
                    "in_stock",
                    "stock_quantity",
                    "sku",
                    "upc",
                    "ean",
                    "condition",
                    "shipping_info",
                    "seller_name",
                    "seller_rating",
                    "specifications",
                    "variants",
                    "updated_at",
                ]
            )
            df.to_parquet(self.parquet_path, index=False)
            logger.info(f"Created new Parquet file: {self.parquet_path}")

    def get_existing_product_ids(self, site_name: Optional[str] = None) -> Set[str]:
        """Get set of existing product IDs for duplicate checking"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT product_id
                    FROM read_parquet('{self.parquet_path}')
                """
                if site_name:
                    query += f" WHERE site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result if row[0] is not None}
        except Exception as e:
            logger.warning(f"Could not fetch existing IDs: {e}")
            return set()

    def get_existing_product_urls(self, site_name: Optional[str] = None) -> Set[str]:
        """Get set of existing product URLs for duplicate checking before crawling"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT url
                    FROM read_parquet('{self.parquet_path}')
                """
                if site_name:
                    query += f" WHERE site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result if row[0] is not None}
        except Exception as e:
            logger.warning(f"Could not fetch existing URLs: {e}")
            return set()

    def get_products_after_date(
        self, cutoff_date: datetime, site_name: Optional[str] = None
    ) -> Set[str]:
        """Get product IDs that were scraped after a certain date"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT product_id
                    FROM read_parquet('{self.parquet_path}')
                    WHERE scraped_at >= '{cutoff_date.isoformat()}'
                """
                if site_name:
                    query += f" AND site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result if row[0] is not None}
        except Exception as e:
            logger.warning(f"Could not fetch products by date: {e}")
            return set()

    def get_urls_after_date(
        self, cutoff_date: datetime, site_name: Optional[str] = None
    ) -> Set[str]:
        """Get product URLs that were scraped after a certain date"""
        try:
            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT DISTINCT url
                    FROM read_parquet('{self.parquet_path}')
                    WHERE scraped_at >= '{cutoff_date.isoformat()}'
                """
                if site_name:
                    query += f" AND site_name = '{site_name}'"

                result = conn.execute(query).fetchall()
                return {row[0] for row in result if row[0] is not None}
        except Exception as e:
            logger.warning(f"Could not fetch URLs by date: {e}")
            return set()

    def save_product_immediate(
        self,
        product: Product,
        skip_duplicates: bool = True,
        skip_if_scraped_within_hours: Optional[int] = None,
    ) -> bool:
        """Save a single product immediately to Parquet"""
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
                "product_id": product.product_id,
                "url": str(product.url),
                "site_name": product.site_name,
                "title": product.title,
                "description": product.description,
                "price": product.price,
                "original_price": product.original_price,
                "currency": product.currency.value if product.currency else "BDT",
                "discount_percentage": product.discount_percentage,
                "brand": product.brand,
                "category": product.category,
                "subcategory": product.subcategory,
                "tags": product.tags if product.tags else [],
                "images": [str(img) for img in product.images]
                if product.images
                else [],
                "main_image": str(product.main_image) if product.main_image else None,
                "rating": product.rating,
                "review_count": product.review_count,
                "in_stock": product.in_stock,
                "stock_quantity": product.stock_quantity,
                "sku": product.sku,
                "upc": getattr(product, "upc", None),
                "ean": getattr(product, "ean", None),
                "condition": getattr(product, "condition", "new"),
                "shipping_info": getattr(product, "shipping_info", None),
                "seller_name": getattr(product, "seller_name", None),
                "seller_rating": getattr(product, "seller_rating", None),
                "specifications": product.specifications
                if hasattr(product, "specifications") and product.specifications
                else {},
                "variants": product.variants
                if hasattr(product, "variants") and product.variants
                else [],
                "scraped_at": product.scraped_at,
                "updated_at": getattr(product, "updated_at", product.scraped_at)
                if hasattr(product, "updated_at")
                else product.scraped_at,
            }

            # Read existing data, append new row, and save back
            if os.path.exists(self.parquet_path):
                df_existing = pd.read_parquet(self.parquet_path)
                df_new = pd.DataFrame([product_dict])
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = pd.DataFrame([product_dict])

            df_combined.to_parquet(self.parquet_path, index=False)
            logger.info(f"Saved product: {product.title[:50]}...")
            return True

        except Exception as e:
            logger.error(f"Error saving product {product.product_id}: {e}")
            return False

    def save_products_batch(
        self,
        products: List[Product],
        skip_duplicates: bool = True,
        skip_if_scraped_within_hours: Optional[int] = None,
    ) -> int:
        """Save multiple products efficiently in batch"""
        if not products:
            return 0

        try:
            # Filter products based on skip conditions
            products_to_save = []

            if skip_duplicates:
                existing_ids = self.get_existing_product_ids()
            else:
                existing_ids = set()

            if skip_if_scraped_within_hours:
                cutoff = datetime.now() - timedelta(hours=skip_if_scraped_within_hours)
                recent_ids = self.get_products_after_date(cutoff)
            else:
                recent_ids = set()

            for product in products:
                if product.product_id in existing_ids:
                    logger.debug(f"Skipping duplicate: {product.product_id}")
                    continue
                if product.product_id in recent_ids:
                    logger.debug(f"Skipping recently scraped: {product.product_id}")
                    continue
                products_to_save.append(product)

            if not products_to_save:
                return 0

            # Convert products to DataFrame
            product_dicts = []
            for product in products_to_save:
                product_dict = {
                    "product_id": product.product_id,
                    "url": str(product.url),
                    "site_name": product.site_name,
                    "title": product.title,
                    "description": product.description,
                    "price": product.price,
                    "original_price": product.original_price,
                    "currency": product.currency.value if product.currency else "BDT",
                    "discount_percentage": product.discount_percentage,
                    "brand": product.brand,
                    "category": product.category,
                    "subcategory": product.subcategory,
                    "tags": product.tags if product.tags else [],
                    "images": [str(img) for img in product.images]
                    if product.images
                    else [],
                    "main_image": str(product.main_image)
                    if product.main_image
                    else None,
                    "rating": product.rating,
                    "review_count": product.review_count,
                    "in_stock": product.in_stock,
                    "stock_quantity": product.stock_quantity,
                    "sku": product.sku,
                    "upc": getattr(product, "upc", None),
                    "ean": getattr(product, "ean", None),
                    "condition": getattr(product, "condition", "new"),
                    "shipping_info": getattr(product, "shipping_info", None),
                    "seller_name": getattr(product, "seller_name", None),
                    "seller_rating": getattr(product, "seller_rating", None),
                    "specifications": product.specifications
                    if hasattr(product, "specifications") and product.specifications
                    else {},
                    "variants": product.variants
                    if hasattr(product, "variants") and product.variants
                    else [],
                    "scraped_at": product.scraped_at,
                    "updated_at": getattr(product, "updated_at", product.scraped_at)
                    if hasattr(product, "updated_at")
                    else product.scraped_at,
                }
                product_dicts.append(product_dict)

            # Read existing data, append new rows, and save back
            if os.path.exists(self.parquet_path):
                df_existing = pd.read_parquet(self.parquet_path)
                df_new = pd.DataFrame(product_dicts)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = pd.DataFrame(product_dicts)

            df_combined.to_parquet(self.parquet_path, index=False)
            logger.info(f"Saved {len(products_to_save)} products in batch")
            return len(products_to_save)

        except Exception as e:
            logger.error(f"Error saving batch: {e}")
            return 0

    def clear_old_data(self, days_to_keep: int = 30):
        """Remove data older than specified days"""
        try:
            cutoff = datetime.now() - timedelta(days=days_to_keep)

            with duckdb.connect(self.db_path) as conn:
                # Create new Parquet file with filtered data
                conn.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{self.parquet_path}')
                        WHERE scraped_at >= '{cutoff.isoformat()}'
                    ) TO '{self.parquet_path}.tmp' (FORMAT PARQUET)
                """)

            # Replace old file with new
            os.replace(f"{self.parquet_path}.tmp", self.parquet_path)

            logger.info(f"Removed old records before {cutoff}")
            return True

        except Exception as e:
            logger.error(f"Error clearing old data: {e}")
            return False

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
                    FROM read_parquet('{self.parquet_path}')
                """).fetchone()

                return {
                    "total_products": int(stats[0]) if stats[0] is not None else 0,
                    "total_sites": int(stats[1]) if stats[1] is not None else 0,
                    "total_categories": int(stats[2]) if stats[2] is not None else 0,
                    "oldest_record": stats[3],
                    "newest_record": stats[4],
                }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

    def load_products(self) -> pd.DataFrame:
        """Load all products as a DataFrame"""
        try:
            if (
                os.path.exists(self.parquet_path)
                and os.path.getsize(self.parquet_path) > 0
            ):
                return pd.read_parquet(self.parquet_path)
            else:
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading products: {e}")
            return pd.DataFrame()

    def save_products(self, products: List[Product]) -> int:
        """Save a list of products (compatibility method)"""
        return self.save_products_batch(products)

    def search_products(
        self,
        query: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        category: Optional[str] = None,
        site_name: Optional[str] = None,
        in_stock_only: bool = False,
        min_rating: Optional[float] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Search products with various filters"""
        try:
            with duckdb.connect(self.db_path) as conn:
                sql = f"SELECT * FROM read_parquet('{self.parquet_path}') WHERE 1=1"

                if query:
                    sql += f" AND (LOWER(title) LIKE LOWER('%{query}%') OR LOWER(description) LIKE LOWER('%{query}%') OR LOWER(brand) LIKE LOWER('%{query}%'))"
                if min_price is not None:
                    sql += f" AND price >= {min_price}"
                if max_price is not None:
                    sql += f" AND price <= {max_price}"
                if category:
                    sql += f" AND LOWER(category) LIKE LOWER('%{category}%')"
                if site_name:
                    sql += f" AND site_name = '{site_name}'"
                if in_stock_only:
                    sql += " AND in_stock = true"
                if min_rating is not None:
                    sql += f" AND rating >= {min_rating}"

                sql += f" LIMIT {limit}"

                return conn.execute(sql).df()
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return pd.DataFrame()

    def get_insights(self) -> dict:
        """Get market insights and analytics"""
        try:
            with duckdb.connect(self.db_path) as conn:
                # Basic stats
                stats = conn.execute(f"""
                    SELECT
                        COUNT(*) as total_products,
                        AVG(price) as avg_price,
                        MIN(price) as min_price,
                        MAX(price) as max_price,
                        COUNT(DISTINCT site_name) as total_sites,
                        COUNT(DISTINCT category) as total_categories,
                        COUNT(DISTINCT brand) as total_brands
                    FROM read_parquet('{self.parquet_path}')
                """).fetchone()

                # Top categories
                top_categories = conn.execute(f"""
                    SELECT category, COUNT(*) as count
                    FROM read_parquet('{self.parquet_path}')
                    WHERE category IS NOT NULL
                    GROUP BY category
                    ORDER BY count DESC
                    LIMIT 10
                """).fetchall()

                # Top brands
                top_brands = conn.execute(f"""
                    SELECT brand, COUNT(*) as count
                    FROM read_parquet('{self.parquet_path}')
                    WHERE brand IS NOT NULL
                    GROUP BY brand
                    ORDER BY count DESC
                    LIMIT 10
                """).fetchall()

                # Price distribution
                price_dist = conn.execute(f"""
                    SELECT
                        CASE
                            WHEN price < 1000 THEN '0-1000'
                            WHEN price < 5000 THEN '1000-5000'
                            WHEN price < 10000 THEN '5000-10000'
                            WHEN price < 25000 THEN '10000-25000'
                            WHEN price < 50000 THEN '25000-50000'
                            ELSE '50000+'
                        END as price_range,
                        COUNT(*) as count
                    FROM read_parquet('{self.parquet_path}')
                    WHERE price IS NOT NULL
                    GROUP BY price_range
                    ORDER BY price_range
                """).fetchall()

                return {
                    "total_products": int(stats[0]) if stats[0] else 0,
                    "avg_price": float(stats[1]) if stats[1] else 0,
                    "price_range": {
                        "min": float(stats[2]) if stats[2] else 0,
                        "max": float(stats[3]) if stats[3] else 0,
                    },
                    "total_sites": int(stats[4]) if stats[4] else 0,
                    "total_categories": int(stats[5]) if stats[5] else 0,
                    "total_brands": int(stats[6]) if stats[6] else 0,
                    "top_categories": [
                        {"name": cat[0], "count": cat[1]} for cat in top_categories
                    ],
                    "top_brands": [
                        {"name": brand[0], "count": brand[1]} for brand in top_brands
                    ],
                    "price_distribution": [
                        {"range": pd[0], "count": pd[1]} for pd in price_dist
                    ],
                }
        except Exception as e:
            logger.error(f"Error getting insights: {e}")
            return {}

    def query_products(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query on the products data"""
        try:
            with duckdb.connect(self.db_path) as conn:
                # Replace 'products' table reference with parquet file
                modified_sql = sql.replace(
                    "products", f"read_parquet('{self.parquet_path}')"
                )
                return conn.execute(modified_sql).df()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def migrate_from_csv(self, csv_path: str):
        """Migrate data from CSV to Parquet format"""
        try:
            df = pd.read_csv(csv_path)

            # Convert string representations back to lists/dicts
            if "tags" in df.columns:
                df["tags"] = df["tags"].apply(
                    lambda x: x.split(",") if pd.notna(x) and x else []
                )
            if "images" in df.columns:
                df["images"] = df["images"].apply(
                    lambda x: x.split(",") if pd.notna(x) and x else []
                )
            if "specifications" in df.columns:
                df["specifications"] = df["specifications"].apply(
                    lambda x: {} if pd.isna(x) or x == "{}" else eval(x)
                )
            if "variants" in df.columns:
                df["variants"] = df["variants"].apply(
                    lambda x: [] if pd.isna(x) or x == "[]" else eval(x)
                )

            # Convert datetime columns
            if "scraped_at" in df.columns:
                df["scraped_at"] = pd.to_datetime(df["scraped_at"])
            if "updated_at" in df.columns:
                df["updated_at"] = pd.to_datetime(df["updated_at"])

            df.to_parquet(self.parquet_path, index=False)
            logger.info(f"Migrated {len(df)} records from CSV to Parquet")
            return len(df)
        except Exception as e:
            logger.error(f"Error migrating from CSV: {e}")
            return 0
