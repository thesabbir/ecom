from fastapi import APIRouter, Query
from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ..storage.parquet_storage import ParquetStorage

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
storage = ParquetStorage()


@router.get("/price-trends")
async def get_price_trends(
    days: int = Query(30, ge=1, le=365),
    site: Optional[str] = None,
    category: Optional[str] = None,
):
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure numeric columns are properly typed
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    cutoff_date = datetime.now() - timedelta(days=days)
    df = df[df["scraped_at"] >= cutoff_date]

    if site:
        df = df[df["site_name"] == site]
    if category:
        df = df[df["category"].str.contains(category, case=False, na=False)]

    trends = (
        df.groupby(df["scraped_at"].dt.date)
        .agg({"price": ["mean", "min", "max", "std"], "product_id": "count"})
        .round(2)
    )

    trends.columns = [
        "avg_price",
        "min_price",
        "max_price",
        "price_std",
        "product_count",
    ]
    trends = trends.reset_index()
    trends["scraped_at"] = trends["scraped_at"].astype(str)

    return {
        "trends": trends.to_dict("records"),
        "summary": {
            "avg_price_overall": float(df["price"].mean()),
            "price_volatility": float(df["price"].std()),
            "total_products": len(df),
        },
    }


@router.get("/competitive-analysis")
async def competitive_analysis(category: Optional[str] = None):
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure numeric columns are properly typed
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["discount_percentage"] = pd.to_numeric(
        df["discount_percentage"], errors="coerce"
    ).fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0)

    if category:
        df = df[df["category"].str.contains(category, case=False, na=False)]

    analysis = (
        df.groupby("site_name")
        .agg(
            {
                "price": ["mean", "min", "max"],
                "discount_percentage": "mean",
                "rating": "mean",
                "review_count": "sum",
                "product_id": "count",
                "in_stock": lambda x: (x == True).sum() / len(x) * 100,
            }
        )
        .round(2)
    )

    analysis.columns = [
        "avg_price",
        "min_price",
        "max_price",
        "avg_discount",
        "avg_rating",
        "total_reviews",
        "product_count",
        "stock_availability",
    ]
    analysis = analysis.reset_index()

    market_share = df["site_name"].value_counts(normalize=True) * 100
    analysis["market_share"] = analysis["site_name"].map(market_share)

    return {
        "competitors": analysis.to_dict("records"),
        "market_leader": analysis.nlargest(1, "market_share").iloc[0]["site_name"],
        "price_leader": analysis.nsmallest(1, "avg_price").iloc[0]["site_name"],
        "quality_leader": analysis.nlargest(1, "avg_rating").iloc[0]["site_name"],
    }


@router.get("/demand-indicators")
async def get_demand_indicators(top_n: int = Query(20, ge=5, le=100)):
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure numeric columns are properly typed
    if "review_count" in df.columns:
        df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(
            0
        )
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    if "discount_percentage" in df.columns:
        df["discount_percentage"] = pd.to_numeric(
            df["discount_percentage"], errors="coerce"
        )
    if "original_price" in df.columns:
        df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce")

    # High demand products (based on review count)
    high_demand_df = df[df["review_count"] > 0]
    if not high_demand_df.empty:
        high_demand = high_demand_df.nlargest(
            min(top_n, len(high_demand_df)), "review_count"
        )[["title", "site_name", "category", "price", "rating", "review_count"]]
    else:
        high_demand = pd.DataFrame()

    # Out of stock categories
    out_of_stock = (
        df[df["in_stock"] == False]
        .groupby("category")
        .size()
        .sort_values(ascending=False)
        .head(10)
    )

    # Products with significant discounts
    discounted_df = df[
        df["discount_percentage"].notna() & (df["discount_percentage"] > 0)
    ]
    if not discounted_df.empty:
        price_drops = discounted_df.nlargest(
            min(top_n, len(discounted_df)), "discount_percentage"
        )[["title", "site_name", "price", "original_price", "discount_percentage"]]
    else:
        price_drops = pd.DataFrame()

    return {
        "high_demand_products": high_demand.to_dict("records")
        if not high_demand.empty
        else [],
        "stock_shortage_categories": out_of_stock.to_dict()
        if not out_of_stock.empty
        else {},
        "significant_discounts": price_drops.to_dict("records")
        if not price_drops.empty
        else [],
        "avg_discount_rate": float(df["discount_percentage"].mean())
        if "discount_percentage" in df and not df["discount_percentage"].isna().all()
        else 0,
    }


@router.get("/category-performance")
async def category_performance():
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure numeric columns are properly typed
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0)
    df["discount_percentage"] = pd.to_numeric(
        df["discount_percentage"], errors="coerce"
    ).fillna(0)

    category_stats = (
        df.groupby("category")
        .agg(
            {
                "price": ["mean", "median"],
                "rating": "mean",
                "review_count": "sum",
                "discount_percentage": "mean",
                "product_id": "count",
                "in_stock": lambda x: (x == True).sum() / len(x) * 100,
            }
        )
        .round(2)
    )

    category_stats.columns = [
        "avg_price",
        "median_price",
        "avg_rating",
        "total_reviews",
        "avg_discount",
        "product_count",
        "availability",
    ]
    category_stats = category_stats.reset_index()
    category_stats = category_stats.sort_values("product_count", ascending=False)

    return {
        "categories": category_stats.to_dict("records"),
        "top_category": category_stats.iloc[0]["category"]
        if not category_stats.empty
        else None,
        "most_expensive_category": category_stats.nlargest(1, "avg_price").iloc[0][
            "category"
        ]
        if not category_stats.empty
        else None,
        "best_rated_category": category_stats.nlargest(1, "avg_rating").iloc[0][
            "category"
        ]
        if not category_stats.empty
        else None,
    }


@router.get("/brand-analysis")
async def brand_analysis(min_products: int = Query(5, ge=1)):
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure numeric columns are properly typed
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0)
    df["discount_percentage"] = pd.to_numeric(
        df["discount_percentage"], errors="coerce"
    ).fillna(0)

    brand_df = df[df["brand"].notna()]

    brand_stats = (
        brand_df.groupby("brand")
        .agg(
            {
                "price": ["mean", "min", "max"],
                "rating": "mean",
                "review_count": "sum",
                "product_id": "count",
                "discount_percentage": "mean",
            }
        )
        .round(2)
    )

    brand_stats.columns = [
        "avg_price",
        "min_price",
        "max_price",
        "avg_rating",
        "total_reviews",
        "product_count",
        "avg_discount",
    ]
    brand_stats = brand_stats[brand_stats["product_count"] >= min_products]
    brand_stats = brand_stats.reset_index()
    brand_stats = brand_stats.sort_values("product_count", ascending=False)

    premium_brands = brand_stats.nlargest(10, "avg_price")["brand"].tolist()
    value_brands = brand_stats.nsmallest(10, "avg_price")["brand"].tolist()

    return {
        "brands": brand_stats.head(50).to_dict("records"),
        "premium_brands": premium_brands,
        "value_brands": value_brands,
        "total_brands": len(brand_stats),
        "brand_concentration": {
            "top_5_market_share": float(
                brand_df[brand_df["brand"].isin(brand_stats.head(5)["brand"])].shape[0]
                / len(brand_df)
                * 100
            )
        },
    }


@router.get("/pricing-opportunities")
async def find_pricing_opportunities(
    margin_threshold: float = Query(20.0, ge=0, le=100),
):
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure price column is properly typed
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    opportunities = []

    for category in df["category"].dropna().unique():
        cat_df = df[df["category"] == category]
        if len(cat_df) < 5:
            continue

        avg_price = cat_df["price"].mean()
        min_price = cat_df["price"].min()
        max_price = cat_df["price"].max()

        price_spread = (
            ((max_price - min_price) / avg_price * 100) if avg_price > 0 else 0
        )

        if price_spread > margin_threshold:
            opportunities.append(
                {
                    "category": category,
                    "avg_price": round(avg_price, 2),
                    "min_price": round(min_price, 2),
                    "max_price": round(max_price, 2),
                    "price_spread_pct": round(price_spread, 2),
                    "potential_margin": round(max_price - avg_price, 2),
                    "product_count": len(cat_df),
                }
            )

    opportunities = sorted(
        opportunities, key=lambda x: x["potential_margin"], reverse=True
    )

    return {
        "opportunities": opportunities[:20],
        "total_opportunities": len(opportunities),
        "avg_potential_margin": round(
            np.mean([o["potential_margin"] for o in opportunities]), 2
        )
        if opportunities
        else 0,
    }


@router.get("/customer-preferences")
async def analyze_customer_preferences():
    df = storage.load_products()

    if df.empty:
        return {"error": "No data available"}

    # Ensure numeric columns are properly typed
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0)
    df["discount_percentage"] = pd.to_numeric(
        df["discount_percentage"], errors="coerce"
    ).fillna(0)

    high_rated = df[df["rating"] >= 4.0] if "rating" in df else pd.DataFrame()

    preferences = {
        "price_sensitivity": {
            "preferred_price_range": {
                "min": float(df["price"].quantile(0.25)),
                "max": float(df["price"].quantile(0.75)),
            },
            "discount_importance": float(
                df[df["discount_percentage"] > 0]["review_count"].sum()
                / df["review_count"].sum()
                * 100
            )
            if df["review_count"].sum() > 0
            else 0,
        },
        "quality_indicators": {
            "min_acceptable_rating": 3.5,
            "avg_rating_purchased": float(
                df[df["review_count"] > df["review_count"].median()]["rating"].mean()
            )
            if "rating" in df
            else None,
            "review_threshold": int(df["review_count"].median())
            if "review_count" in df
            else 0,
        },
        "popular_categories": df.groupby("category")["review_count"]
        .sum()
        .nlargest(10)
        .to_dict(),
        "popular_brands": df.groupby("brand")["review_count"]
        .sum()
        .nlargest(10)
        .to_dict()
        if "brand" in df
        else {},
        "high_rated_characteristics": {
            "avg_price": float(high_rated["price"].mean())
            if not high_rated.empty
            else None,
            "common_categories": high_rated["category"].value_counts().head(5).to_dict()
            if not high_rated.empty
            else {},
        },
    }

    return preferences
