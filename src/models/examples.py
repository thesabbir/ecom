from typing import Dict, Any

CRAWL_REQUEST_EXAMPLES: Dict[str, Any] = {
    "ryans_laptops": {
        "summary": "Crawl Ryans laptops",
        "value": {
            "url": "https://www.ryans.com/categories/laptop",
            "site_adapter": "ryans",
            "max_pages": 2,
            "price_min": 50000,
            "price_max": 150000,
            "category_filter": "Laptop"
        }
    },
    "generic_site": {
        "summary": "Crawl generic e-commerce site",
        "value": {
            "url": "https://shop.example.com/category/electronics",
            "max_pages": 3
        }
    }
}

PRODUCT_EXAMPLE = {
    "product_id": "ryans_12345",
    "url": "https://www.ryans.com/product/12345",
    "site_name": "Ryans",
    "title": "Dell Inspiron 15 Laptop",
    "description": "High performance laptop with latest processor.",
    "price": 75000,
    "original_price": 80000,
    "currency": "BDT",
    "discount_percentage": 6.25,
    "brand": "Dell",
    "category": "Electronics",
    "subcategory": "Smart Home",
    "tags": ["smart speaker", "alexa", "voice assistant"],
    "images": [
        "https://www.ryans.com/images/product/12345.jpg"
    ],
    "main_image": "https://www.ryans.com/images/product/12345.jpg",
    "rating": 4.5,
    "review_count": 125432,
    "in_stock": True,
    "stock_quantity": 50,
    "sku": "B08N5WRWNW",
    "condition": "new",
    "specifications": {
        "Weight": "12 ounces",
        "Dimensions": "3.9 x 3.9 x 3.5 inches",
        "Connectivity": "Wi-Fi, Bluetooth"
    },
    "scraped_at": "2024-01-15T10:30:00Z"
}

SQL_QUERY_EXAMPLES = {
    "avg_price_by_site": {
        "summary": "Average price by site",
        "value": {
            "sql": "SELECT site_name, AVG(price) as avg_price, COUNT(*) as product_count FROM products GROUP BY site_name ORDER BY avg_price DESC"
        }
    },
    "top_discounted": {
        "summary": "Top discounted products",
        "value": {
            "sql": "SELECT title, site_name, price, original_price, discount_percentage FROM products WHERE discount_percentage IS NOT NULL ORDER BY discount_percentage DESC LIMIT 10"
        }
    },
    "category_analysis": {
        "summary": "Category performance",
        "value": {
            "sql": "SELECT category, COUNT(*) as count, AVG(price) as avg_price, AVG(rating) as avg_rating FROM products WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC"
        }
    }
}