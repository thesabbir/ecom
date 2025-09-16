"""
Product data retrieval and search endpoints
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from pydantic import BaseModel, Field
import pandas as pd

from ..storage.parquet_storage import ParquetStorage
from ..utils.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api", tags=["Products"])
storage = ParquetStorage()


class SearchParams(BaseModel):
    query: Optional[str] = Field(None, description="Search query")
    site: Optional[str] = Field(None, description="Filter by site")
    category: Optional[str] = Field(None, description="Filter by category")
    brand: Optional[str] = Field(None, description="Filter by brand")
    min_price: Optional[float] = Field(None, ge=0, description="Minimum price")
    max_price: Optional[float] = Field(None, ge=0, description="Maximum price")
    in_stock: Optional[bool] = Field(None, description="Filter by stock availability")
    sort_by: Optional[str] = Field("price", description="Sort field")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=100, description="Items per page")


@router.get(
    "/products",
    summary="Get products",
    description="Retrieve products with pagination and optional filtering",
)
async def get_products(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    site: Optional[str] = None,
    category: Optional[str] = None,
    in_stock: Optional[bool] = None,
):
    """Get products with optional filtering"""
    try:
        df = storage.load_products()

        if df.empty:
            return {"products": [], "total": 0, "limit": limit, "offset": offset}

        # Apply filters
        if site:
            df = df[df["site_name"] == site]
        if category:
            df = df[df["category"].str.contains(category, case=False, na=False)]
        if in_stock is not None:
            df = df[df["in_stock"] == in_stock]

        total = len(df)

        # Apply pagination
        df = df.iloc[offset : offset + limit]

        # Convert complex types to JSON-serializable formats
        for col in df.columns:
            if col in ["tags", "images", "variants"]:
                df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])
            elif col in ["specifications"]:
                # Only keep non-null specification values
                df[col] = df[col].apply(lambda x: {k: v for k, v in (x if isinstance(x, dict) else {}).items() if v is not None})
            elif col in ["scraped_at", "updated_at"]:
                df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)

        # Convert to dict
        products = df.to_dict("records")

        return {
            "products": products,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error(f"Error getting products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/search",
    summary="Search products",
    description="Search products with multiple filter options",
)
async def search_products(params: SearchParams = Query()):
    """Search products with advanced filtering"""
    try:
        df = storage.load_products()

        if df.empty:
            return {"results": [], "total": 0}

        # Apply search filters
        if params.query:
            query_lower = params.query.lower()
            df = df[
                df["title"].str.lower().str.contains(query_lower, na=False)
                | df["brand"].str.lower().str.contains(query_lower, na=False)
                | df["category"].str.lower().str.contains(query_lower, na=False)
            ]

        if params.site:
            df = df[df["site_name"] == params.site]

        if params.category:
            df = df[df["category"].str.contains(params.category, case=False, na=False)]

        if params.brand:
            df = df[df["brand"].str.contains(params.brand, case=False, na=False)]

        if params.min_price is not None:
            df = df[df["price"] >= params.min_price]

        if params.max_price is not None:
            df = df[df["price"] <= params.max_price]

        if params.in_stock is not None:
            df = df[df["in_stock"] == params.in_stock]

        # Sort
        if params.sort_by:
            ascending = params.sort_by != "price_desc"
            sort_column = "price" if "price" in params.sort_by else params.sort_by
            df = df.sort_values(sort_column, ascending=ascending)

        total = len(df)

        # Pagination
        offset = (params.page - 1) * params.page_size
        df = df.iloc[offset : offset + params.page_size]

        # Convert complex types to JSON-serializable formats
        for col in df.columns:
            if col in ["tags", "images", "variants"]:
                df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])
            elif col in ["specifications"]:
                # Only keep non-null specification values
                df[col] = df[col].apply(lambda x: {k: v for k, v in (x if isinstance(x, dict) else {}).items() if v is not None})
            elif col in ["scraped_at", "updated_at"]:
                df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)

        return {
            "results": df.to_dict("records"),
            "total": total,
            "page": params.page,
            "page_size": params.page_size,
            "has_next": offset + params.page_size < total,
        }
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/insights",
    summary="Get market insights",
    description="Get aggregated insights about the crawled data",
)
async def get_insights():
    """Get market insights and statistics"""
    try:
        df = storage.load_products()

        if df.empty:
            return {"message": "No data available"}

        insights = {
            "total_products": len(df),
            "total_sites": df["site_name"].nunique(),
            "total_categories": df["category"].nunique(),
            "total_brands": df["brand"].nunique(),
            "average_price": float(df["price"].mean()),
            "price_range": {
                "min": float(df["price"].min()),
                "max": float(df["price"].max()),
            },
            "stock_availability": {
                "in_stock": int(df["in_stock"].sum()),
                "out_of_stock": int((~df["in_stock"]).sum()),
                "percentage_in_stock": float((df["in_stock"].sum() / len(df)) * 100),
            },
            "top_categories": df["category"].value_counts().head(10).to_dict(),
            "top_brands": df["brand"].value_counts().head(10).to_dict(),
            "price_distribution": {
                "under_500": int((df["price"] < 500).sum()),
                "500_to_1000": int(((df["price"] >= 500) & (df["price"] < 1000)).sum()),
                "1000_to_2000": int(
                    ((df["price"] >= 1000) & (df["price"] < 2000)).sum()
                ),
                "above_2000": int((df["price"] >= 2000).sum()),
            },
            "last_updated": df["scraped_at"].max() if "scraped_at" in df.columns else None,
        }

        return insights
    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))