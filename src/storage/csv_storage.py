import pandas as pd
import duckdb
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import logging
from ..models.product import Product

logger = logging.getLogger(__name__)


class CSVStorage:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.products_file = self.data_dir / "products.csv"
        self.db_path = self.data_dir / "products.duckdb"

    def save_products(self, products: List[Product], append: bool = True) -> int:
        if not products:
            return 0

        df = pd.DataFrame([p.model_dump() for p in products])

        # Only process columns if they exist
        if 'images' in df.columns:
            df['images'] = df['images'].apply(json.dumps)
        if 'specifications' in df.columns:
            df['specifications'] = df['specifications'].apply(json.dumps)
        if 'variants' in df.columns:
            df['variants'] = df['variants'].apply(json.dumps)
        if 'shipping_info' in df.columns:
            df['shipping_info'] = df['shipping_info'].apply(
                lambda x: json.dumps(x) if x else None
            )

        if self.products_file.exists() and append:
            existing_df = pd.read_csv(self.products_file)
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['product_id'], keep='last')

        df.to_csv(self.products_file, index=False)
        logger.info(f"Saved {len(products)} products to {self.products_file}")
        return len(products)

    def load_products(self) -> pd.DataFrame:
        if not self.products_file.exists():
            return pd.DataFrame()

        df = pd.read_csv(self.products_file)

        for col in ['images', 'specifications', 'variants', 'shipping_info']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.loads(x) if pd.notna(x) else []
                )

        return df

    def query_products(self, sql: str) -> pd.DataFrame:
        conn = duckdb.connect(str(self.db_path))

        if self.products_file.exists():
            conn.execute(f"""
                CREATE OR REPLACE TABLE products AS
                SELECT * FROM read_csv_auto('{self.products_file}')
            """)

        result = conn.execute(sql).fetchdf()
        conn.close()
        return result

    def get_insights(self) -> Dict[str, Any]:
        if not self.products_file.exists():
            return {"error": "No data available"}

        conn = duckdb.connect(str(self.db_path))
        conn.execute(f"""
            CREATE OR REPLACE TABLE products AS
            SELECT * FROM read_csv_auto('{self.products_file}')
        """)

        insights = {}

        insights['total_products'] = conn.execute(
            "SELECT COUNT(*) as count FROM products"
        ).fetchone()[0]

        insights['unique_sites'] = conn.execute(
            "SELECT COUNT(DISTINCT site_name) as count FROM products"
        ).fetchone()[0]

        insights['avg_price_by_site'] = conn.execute("""
            SELECT site_name,
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price,
                   COUNT(*) as product_count
            FROM products
            GROUP BY site_name
            ORDER BY avg_price DESC
        """).fetchdf().to_dict('records')

        insights['top_categories'] = conn.execute("""
            SELECT category, COUNT(*) as count
            FROM products
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf().to_dict('records')

        insights['price_distribution'] = conn.execute("""
            SELECT
                CASE
                    WHEN price < 10 THEN 'Under $10'
                    WHEN price < 50 THEN '$10-50'
                    WHEN price < 100 THEN '$50-100'
                    WHEN price < 500 THEN '$100-500'
                    ELSE 'Over $500'
                END as price_range,
                COUNT(*) as count
            FROM products
            GROUP BY price_range
            ORDER BY
                CASE price_range
                    WHEN 'Under $10' THEN 1
                    WHEN '$10-50' THEN 2
                    WHEN '$50-100' THEN 3
                    WHEN '$100-500' THEN 4
                    ELSE 5
                END
        """).fetchdf().to_dict('records')

        insights['top_discounted'] = conn.execute("""
            SELECT title, site_name, price, original_price, discount_percentage
            FROM products
            WHERE discount_percentage IS NOT NULL
            ORDER BY discount_percentage DESC
            LIMIT 10
        """).fetchdf().to_dict('records')

        insights['top_rated'] = conn.execute("""
            SELECT title, site_name, rating, review_count
            FROM products
            WHERE rating IS NOT NULL
            ORDER BY rating DESC, review_count DESC
            LIMIT 10
        """).fetchdf().to_dict('records')

        insights['stock_status'] = conn.execute("""
            SELECT
                in_stock,
                COUNT(*) as count,
                AVG(price) as avg_price
            FROM products
            GROUP BY in_stock
        """).fetchdf().to_dict('records')

        insights['brands_overview'] = conn.execute("""
            SELECT
                brand,
                COUNT(*) as product_count,
                AVG(price) as avg_price,
                AVG(rating) as avg_rating
            FROM products
            WHERE brand IS NOT NULL
            GROUP BY brand
            ORDER BY product_count DESC
            LIMIT 20
        """).fetchdf().to_dict('records')

        conn.close()
        return insights

    def search_products(
        self,
        query: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        category: Optional[str] = None,
        site_name: Optional[str] = None,
        in_stock_only: bool = False,
        min_rating: Optional[float] = None,
        limit: int = 100
    ) -> pd.DataFrame:

        conn = duckdb.connect(str(self.db_path))

        if self.products_file.exists():
            conn.execute(f"""
                CREATE OR REPLACE TABLE products AS
                SELECT * FROM read_csv_auto('{self.products_file}')
            """)

        conditions = []

        if query:
            conditions.append(f"""
                (LOWER(title) LIKE LOWER('%{query}%') OR
                 LOWER(description) LIKE LOWER('%{query}%') OR
                 LOWER(brand) LIKE LOWER('%{query}%'))
            """)

        if min_price is not None:
            conditions.append(f"price >= {min_price}")

        if max_price is not None:
            conditions.append(f"price <= {max_price}")

        if category:
            conditions.append(f"LOWER(category) LIKE LOWER('%{category}%')")

        if site_name:
            conditions.append(f"LOWER(site_name) = LOWER('{site_name}')")

        if in_stock_only:
            conditions.append("in_stock = true")

        if min_rating is not None:
            conditions.append(f"rating >= {min_rating}")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            SELECT * FROM products
            WHERE {where_clause}
            ORDER BY scraped_at DESC
            LIMIT {limit}
        """

        result = conn.execute(sql).fetchdf()
        conn.close()
        return result