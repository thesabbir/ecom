from pydantic import BaseModel, Field, HttpUrl, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class PriceUnit(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    BDT = "BDT"
    INR = "INR"
    OTHER = "OTHER"


class ProductCondition(str, Enum):
    NEW = "new"
    USED = "used"
    REFURBISHED = "refurbished"
    UNKNOWN = "unknown"


class Product(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "product_id": "ryans_12345",
                "url": "https://www.ryans.com/product/12345",
                "site_name": "Ryans",
                "title": "Echo Dot (4th Gen) Smart speaker",
                "price": 49.99,
                "currency": "USD",
                "rating": 4.5,
                "review_count": 125432,
                "in_stock": True,
                "scraped_at": "2024-01-15T10:30:00Z"
            }
        }
    )
    product_id: str = Field(..., description="Unique identifier for the product")
    url: HttpUrl = Field(..., description="Product page URL")
    site_name: str = Field(..., description="E-commerce site name")
    title: str = Field(..., description="Product title")
    description: Optional[str] = Field(None, description="Product description")
    price: float = Field(..., description="Current price")
    original_price: Optional[float] = Field(None, description="Original price before discount")
    currency: PriceUnit = Field(PriceUnit.USD, description="Price currency")
    discount_percentage: Optional[float] = Field(None, description="Discount percentage")

    brand: Optional[str] = Field(None, description="Brand name")
    category: Optional[str] = Field(None, description="Main category")
    subcategory: Optional[str] = Field(None, description="Subcategory")
    tags: List[str] = Field(default_factory=list, description="Product tags")

    images: List[HttpUrl] = Field(default_factory=list, description="Product image URLs")
    main_image: Optional[HttpUrl] = Field(None, description="Main product image")

    rating: Optional[float] = Field(None, ge=0, le=5, description="Average rating")
    review_count: Optional[int] = Field(None, ge=0, description="Number of reviews")

    in_stock: bool = Field(True, description="Stock availability")
    stock_quantity: Optional[int] = Field(None, description="Available quantity")

    sku: Optional[str] = Field(None, description="Stock keeping unit")
    upc: Optional[str] = Field(None, description="Universal product code")
    ean: Optional[str] = Field(None, description="European article number")

    condition: ProductCondition = Field(ProductCondition.NEW, description="Product condition")
    shipping_info: Optional[Dict[str, Any]] = Field(None, description="Shipping information")
    seller_name: Optional[str] = Field(None, description="Seller or merchant name")
    seller_rating: Optional[float] = Field(None, description="Seller rating")

    specifications: Dict[str, Any] = Field(default_factory=dict, description="Product specifications")
    variants: List[Dict[str, Any]] = Field(default_factory=list, description="Product variants")

    scraped_at: datetime = Field(default_factory=datetime.now, description="Scraping timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")


class ProductList(BaseModel):
    products: List[Product]
    total_count: int
    page: int = 1
    page_size: int = 50
    has_next: bool = False


class CrawlRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "url": "https://www.ryans.com/categories/laptop",
                "site_adapter": "ryans",
                "max_pages": 2,
                "skip_duplicates": True,
                "skip_if_scraped_within_hours": 24,
                "overwrite": False,
                "price_min": 500,
                "price_max": 1500
            }
        }
    )
    url: HttpUrl
    site_adapter: Optional[str] = Field(None, description="Specific site adapter to use")
    max_pages: int = Field(1, ge=1, le=100, description="Maximum pages to crawl")
    skip_duplicates: bool = Field(True, description="Skip products that already exist in storage")
    skip_if_scraped_within_hours: Optional[int] = Field(24, description="Skip products scraped within N hours (None to disable)")
    overwrite: bool = Field(False, description="Clear existing data before crawling")
    category_filter: Optional[str] = Field(None, description="Category to filter")
    price_min: Optional[float] = Field(None, ge=0, description="Minimum price filter")
    price_max: Optional[float] = Field(None, ge=0, description="Maximum price filter")


class CrawlResponse(BaseModel):
    job_id: str
    status: str
    products_found: int
    message: str
    started_at: datetime
    completed_at: Optional[datetime] = None