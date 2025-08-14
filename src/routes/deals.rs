use axum::{
    extract::{Extension, Path, Query},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use sqlx::PgPool;
use serde::Deserialize;
use std::sync::Arc;
use crate::shared_models::deal::{
    CreateDealRequest, Deal, DealSearchRequest,
};
use crate::kafka::{KafkaProducer, DealEvent, DealEventType};
use crate::lazy_db::LazyDbService;

#[derive(Deserialize)]
pub struct DealsQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub search: Option<String>,
    pub category: Option<String>,
    pub merchant: Option<String>,
}

pub fn deals_routes(pool: PgPool) -> Router {
    let lazy_db = Arc::new(LazyDbService::new(pool.clone()));
    
    Router::new()
        .route("/", post(create_deal).get(search_deals_lazy))
        .route("/:id", get(get_deal_lazy))
        .route("/merchant/:merchant", get(get_coupons_by_merchant))
        .route("/submit", post(submit_coupon))
        .layer(Extension(pool))
        .layer(Extension(lazy_db))
}

async fn create_deal(
    Extension(pool): Extension<PgPool>,
    Json(payload): Json<CreateDealRequest>,
) -> Result<Json<Deal>, StatusCode> {
    match Deal::create(&pool, payload).await {
        Ok(deal) => {
            // Publish deal created event to Kafka
            if let Ok(kafka_producer) = KafkaProducer::new() {
                let deal_event = DealEvent {
                    deal_id: deal.id.to_string(),
                    event_type: DealEventType::DealCreated,
                    product_id: deal.external_id.clone(),
                    retailer: deal.merchant.clone(),
                    category: deal.category.clone().unwrap_or_else(|| "general".to_string()),
                    original_price: deal.original_price.to_string().parse().unwrap_or(0.0),
                    discounted_price: deal.discounted_price.as_ref()
                        .map(|d| d.to_string().parse().unwrap_or(0.0))
                        .unwrap_or(deal.original_price.to_string().parse().unwrap_or(0.0)),
                    currency: deal.currency.clone(),
                    ..Default::default()
                };
                
                let _ = kafka_producer.publish_deal_event(deal_event).await;
            }
            
            Ok(Json(deal))
        },
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_coupons_by_merchant(
    Extension(pool): Extension<PgPool>,
    Path(merchant): Path<String>,
) -> Result<Json<Vec<Deal>>, StatusCode> {
    let search_request = DealSearchRequest {
        query: None,
        category: None,
        merchant: Some(merchant),
        min_discount: None,
        max_price: None,
        currency: None,
        only_active: Some(true),
        only_verified: Some(true),
        limit: Some(100),
        offset: Some(0),
    };

    match Deal::search(&pool, search_request).await {
        Ok(response) => Ok(Json(response.deals)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn submit_coupon(
    Extension(pool): Extension<PgPool>,
    Json(payload): Json<CreateDealRequest>,
) -> Result<Json<Deal>, StatusCode> {
    // In a real application, you'd have more validation and security here.
    // For example, you might want to check if the user is logged in,
    // and you might want to have a system to prevent spam.
    match Deal::create(&pool, payload).await {
        Ok(deal) => {
            // Publish user-submitted deal event to Kafka
            if let Ok(kafka_producer) = KafkaProducer::new() {
                let deal_event = DealEvent {
                    deal_id: deal.id.to_string(),
                    event_type: DealEventType::UserSubmitted,
                    product_id: deal.external_id.clone(),
                    retailer: deal.merchant.clone(),
                    category: deal.category.clone().unwrap_or_else(|| "general".to_string()),
                    original_price: deal.original_price.to_string().parse().unwrap_or(0.0),
                    discounted_price: deal.discounted_price.as_ref()
                        .map(|d| d.to_string().parse().unwrap_or(0.0))
                        .unwrap_or(deal.original_price.to_string().parse().unwrap_or(0.0)),
                    currency: deal.currency.clone(),
                    source: "user_submission".to_string(),
                    ..Default::default()
                };
                
                let _ = kafka_producer.publish_deal_event(deal_event).await;
            }
            
            Ok(Json(deal))
        },
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// Lazy loading implementations
async fn search_deals_lazy(
    Extension(lazy_db): Extension<Arc<LazyDbService>>,
    Query(params): Query<DealsQuery>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    let limit = params.limit.unwrap_or(20).min(100); // Limit max results
    let offset = params.offset.unwrap_or(0);
    let search_filter = params.search.as_deref();
    
    match lazy_db.get_deals_lazy(limit, offset, search_filter).await {
        Ok(deals) => Ok(Json(deals)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_deal_lazy(
    Extension(lazy_db): Extension<Arc<LazyDbService>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match lazy_db.get_user_lazy(&id).await {
        Ok(Some(deal)) => Ok(Json(deal)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
