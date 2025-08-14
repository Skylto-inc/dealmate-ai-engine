use axum::{
    extract::{Extension, Query},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::services::real_time_deals::{
    RealTimeDealsService, RealTimeDeal, DealFilter, DealAlert, AlertType, PricePoint
};

#[derive(Debug, Deserialize)]
pub struct GetDealsQuery {
    pub categories: Option<String>, // comma-separated
    pub platforms: Option<String>,  // comma-separated
    pub min_discount: Option<f64>,
    pub max_price: Option<f64>,
    pub brands: Option<String>,     // comma-separated
    pub include_bank_offers: Option<bool>,
    pub include_coupons: Option<bool>,
    pub flash_sales_only: Option<bool>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct GetDealsResponse {
    pub deals: Vec<RealTimeDeal>,
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateAlertRequest {
    pub user_id: String,
    pub product_name: String,
    pub target_price: Option<f64>,
    pub min_discount: Option<f64>,
    pub platforms: Vec<String>,
    pub alert_type: AlertType,
}

#[derive(Debug, Deserialize)]
pub struct PriceHistoryQuery {
    pub platform: String,
    pub product_name: String,
}

pub fn real_time_deals_routes(pool: PgPool, redis_client: redis::Client) -> Router {
    let service = Arc::new(RealTimeDealsService::new(pool, redis_client));
    
    // Start background tasks
    let bg_service = service.clone();
    tokio::spawn(async move {
        bg_service.start_background_tasks().await;
    });
    
    Router::new()
        .route("/", get(get_deals))
        .route("/alerts", post(create_alert))
        .route("/price-history", get(get_price_history))
        .route("/trending", get(get_trending_deals))
        .route("/flash-sales", get(get_flash_sales))
        .layer(Extension(service))
}

async fn get_deals(
    Extension(service): Extension<Arc<RealTimeDealsService>>,
    Query(params): Query<GetDealsQuery>,
) -> Result<Json<GetDealsResponse>, StatusCode> {
    let filter = DealFilter {
        categories: params.categories.map(|c| c.split(',').map(String::from).collect()),
        platforms: params.platforms.map(|p| p.split(',').map(String::from).collect()),
        min_discount: params.min_discount,
        max_price: params.max_price.map(|p| BigDecimal::from(p as i64)),
        brands: params.brands.map(|b| b.split(',').map(String::from).collect()),
        include_bank_offers: params.include_bank_offers.unwrap_or(true),
        include_coupons: params.include_coupons.unwrap_or(true),
        flash_sales_only: params.flash_sales_only.unwrap_or(false),
    };
    
    let limit = params.limit.unwrap_or(20).min(100);
    let offset = params.offset.unwrap_or(0);
    
    match service.get_real_time_deals(filter, limit, offset).await {
        Ok(deals) => {
            let total = deals.len();
            Ok(Json(GetDealsResponse { deals, total }))
        }
        Err(e) => {
            tracing::error!("Failed to get deals: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn create_alert(
    Extension(service): Extension<Arc<RealTimeDealsService>>,
    Json(payload): Json<CreateAlertRequest>,
) -> Result<Json<DealAlert>, StatusCode> {
    let alert = DealAlert {
        id: Uuid::new_v4(),
        user_id: payload.user_id,
        product_name: payload.product_name,
        target_price: payload.target_price.map(|p| BigDecimal::from(p as i64)),
        min_discount: payload.min_discount,
        platforms: payload.platforms,
        alert_type: payload.alert_type,
        created_at: chrono::Utc::now(),
        last_triggered: None,
    };
    
    match service.create_price_alert(alert.clone()).await {
        Ok(_) => Ok(Json(alert)),
        Err(e) => {
            tracing::error!("Failed to create alert: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_price_history(
    Extension(service): Extension<Arc<RealTimeDealsService>>,
    Query(params): Query<PriceHistoryQuery>,
) -> Result<Json<Vec<PricePoint>>, StatusCode> {
    match service.get_price_history(&params.platform, &params.product_name).await {
        Ok(history) => Ok(Json(history)),
        Err(e) => {
            tracing::error!("Failed to get price history: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_trending_deals(
    Extension(service): Extension<Arc<RealTimeDealsService>>,
) -> Result<Json<GetDealsResponse>, StatusCode> {
    // Get deals with high discount percentages
    let filter = DealFilter {
        categories: None,
        platforms: None,
        min_discount: Some(30.0),
        max_price: None,
        brands: None,
        include_bank_offers: true,
        include_coupons: true,
        flash_sales_only: false,
    };
    
    match service.get_real_time_deals(filter, 10, 0).await {
        Ok(deals) => {
            let total = deals.len();
            Ok(Json(GetDealsResponse { deals, total }))
        }
        Err(e) => {
            tracing::error!("Failed to get trending deals: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_flash_sales(
    Extension(service): Extension<Arc<RealTimeDealsService>>,
) -> Result<Json<GetDealsResponse>, StatusCode> {
    let filter = DealFilter {
        categories: None,
        platforms: None,
        min_discount: None,
        max_price: None,
        brands: None,
        include_bank_offers: true,
        include_coupons: true,
        flash_sales_only: true,
    };
    
    match service.get_real_time_deals(filter, 20, 0).await {
        Ok(deals) => {
            let total = deals.len();
            Ok(Json(GetDealsResponse { deals, total }))
        }
        Err(e) => {
            tracing::error!("Failed to get flash sales: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
