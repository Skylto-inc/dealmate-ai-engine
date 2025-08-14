use axum::{routing::{get, post}, Router, Json};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/deals", get(get_deals))
        .route("/deals/search", get(search_deals))
        .route("/deals/trending", get(trending_deals))
        .route("/coupons", get(get_coupons))
        .route("/coupons/test", post(test_coupons))
        .route("/coupons/validate", post(validate_coupon))
        .route("/stacksmart", post(optimize_deals))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8001").await.unwrap();
    println!("💰 Deal Service running on port 8001");
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<Value> {
    Json(json!({"status": "healthy", "service": "deal-service", "features": ["deals", "coupons", "stacksmart"]}))
}

async fn get_deals() -> Json<Value> {
    Json(json!({
        "deals": [
            {"id": "deal_1", "title": "50% off Laptops", "discount": 50, "store": "TechStore"},
            {"id": "deal_2", "title": "Buy 2 Get 1 Free", "discount": 33, "store": "BookStore"}
        ],
        "service": "deal-service"
    }))
}

async fn search_deals() -> Json<Value> {
    Json(json!({
        "results": [
            {"id": "deal_1", "title": "Laptop Deal", "discount": 50, "relevance": 0.9}
        ],
        "query": "laptop",
        "service": "deal-service"
    }))
}

async fn trending_deals() -> Json<Value> {
    Json(json!({
        "trending": [
            {"id": "deal_1", "title": "Hot Laptop Deal", "popularity": 95}
        ],
        "service": "deal-service"
    }))
}

async fn get_coupons() -> Json<Value> {
    Json(json!({
        "coupons": [
            {"code": "SAVE20", "discount": 20, "type": "percentage"},
            {"code": "FLAT50", "discount": 50, "type": "fixed"}
        ],
        "service": "deal-service"
    }))
}

async fn test_coupons() -> Json<Value> {
    Json(json!({
        "valid": true,
        "discount": 20,
        "message": "Coupon tested by Deal Service",
        "service": "deal-service"
    }))
}

async fn validate_coupon() -> Json<Value> {
    Json(json!({
        "valid": true,
        "discount": 15,
        "message": "Coupon validated by Deal Service"
    }))
}

async fn optimize_deals() -> Json<Value> {
    Json(json!({
        "optimized_deals": [
            {"combination": ["SAVE20", "FREESHIP"], "total_discount": 25}
        ],
        "message": "StackSmart optimization by Deal Service"
    }))
}
