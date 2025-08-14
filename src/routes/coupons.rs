use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use sqlx::PgPool;

use crate::models::coupon::{
    Coupon, CouponSearchQuery, CouponTestRequest, CouponTestResult, 
    NewCoupon, NewCouponTest, NewMerchant, Merchant
};

#[derive(Debug)]
pub enum CouponError {
    NotFound,
    DatabaseError(sqlx::Error),
    ValidationError(String),
}

impl From<sqlx::Error> for CouponError {
    fn from(err: sqlx::Error) -> Self {
        CouponError::DatabaseError(err)
    }
}

impl IntoResponse for CouponError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            CouponError::NotFound => (StatusCode::NOT_FOUND, "Resource not found"),
            CouponError::ValidationError(ref msg) => (StatusCode::BAD_REQUEST, msg.as_str()),
            CouponError::DatabaseError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"),
        };
        (status, Json(json!({ "error": error_message }))).into_response()
    }
}

pub async fn search_coupons(
    State(pool): State<PgPool>,
    Query(query): Query<CouponSearchQuery>,
) -> Result<Json<Vec<Coupon>>, CouponError> {
    let mut sql = "SELECT c.* FROM coupons c JOIN merchants m ON c.merchant_id = m.id WHERE 1=1".to_string();
    let mut conditions = Vec::new();

    if let Some(domain) = &query.merchant_domain {
        conditions.push(format!("m.domain = '{}'", domain));
    }
    if let Some(discount_type) = &query.discount_type {
        conditions.push(format!("c.discount_type = '{}'", discount_type));
    }
    if query.active_only.unwrap_or(true) {
        conditions.push("c.is_active = true AND (c.valid_until IS NULL OR c.valid_until > NOW())".to_string());
    }

    if !conditions.is_empty() {
        sql.push_str(" AND ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY c.created_at DESC");

    let coupons = sqlx::query_as::<_, Coupon>(&sql)
        .fetch_all(&pool)
        .await?;

    Ok(Json(coupons))
}

pub async fn create_merchant(
    State(pool): State<PgPool>,
    Json(payload): Json<NewMerchant>,
) -> Result<impl IntoResponse, CouponError> {
    let merchant = sqlx::query_as!(
        Merchant,
        r#"INSERT INTO merchants (name, domain, affiliate_network, commission_rate) 
           VALUES ($1, $2, $3, $4) RETURNING *"#,
        payload.name,
        payload.domain,
        payload.affiliate_network,
        payload.commission_rate
    )
    .fetch_one(&pool)
    .await?;

    Ok((StatusCode::CREATED, Json(merchant)))
}

pub async fn create_coupon(
    State(pool): State<PgPool>,
    Json(payload): Json<NewCoupon>,
) -> Result<impl IntoResponse, CouponError> {
    let coupon = sqlx::query_as!(
        Coupon,
        r#"INSERT INTO coupons (merchant_id, code, title, description, discount_type, 
           discount_value, minimum_order, maximum_discount, valid_from, valid_until, 
           usage_limit, source, affiliate_network) 
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING *"#,
        payload.merchant_id,
        payload.code,
        payload.title,
        payload.description,
        payload.discount_type,
        payload.discount_value,
        payload.minimum_order,
        payload.maximum_discount,
        payload.valid_from,
        payload.valid_until,
        payload.usage_limit,
        payload.source,
        payload.affiliate_network
    )
    .fetch_one(&pool)
    .await?;

    Ok((StatusCode::CREATED, Json(coupon)))
}

pub async fn test_coupons(
    State(pool): State<PgPool>,
    Json(payload): Json<CouponTestRequest>,
) -> Result<Json<Vec<CouponTestResult>>, CouponError> {
    let mut results = Vec::new();
    
    for code in payload.coupon_codes {
        let coupon = sqlx::query_as!(
            Coupon,
            r#"SELECT c.* FROM coupons c 
               JOIN merchants m ON c.merchant_id = m.id 
               WHERE c.code = $1 AND m.domain = $2 AND c.is_active = true"#,
            code,
            payload.merchant_domain
        )
        .fetch_optional(&pool)
        .await?;

        let result = if let Some(coupon) = coupon {
            let discount = calculate_discount(&coupon, &payload.order_value);
            let discount_amount = discount.clone().unwrap_or_default();
            let final_price = &payload.order_value - &discount_amount;
            
            // Record test result
            let test_record = NewCouponTest {
                coupon_id: coupon.id,
                is_valid: discount.is_some(),
                error_message: None,
                discount_applied: discount.clone(),
                test_order_value: Some(payload.order_value.clone()),
            };
            
            let _ = sqlx::query!(
                r#"INSERT INTO coupon_tests (coupon_id, is_valid, error_message, discount_applied, test_order_value)
                   VALUES ($1, $2, $3, $4, $5)"#,
                test_record.coupon_id,
                test_record.is_valid,
                test_record.error_message,
                test_record.discount_applied,
                test_record.test_order_value
            )
            .execute(&pool)
            .await;

            CouponTestResult {
                code: code.clone(),
                is_valid: discount.is_some(),
                discount_applied: discount,
                final_price: Some(final_price),
                error_message: None,
            }
        } else {
            CouponTestResult {
                code: code.clone(),
                is_valid: false,
                discount_applied: None,
                final_price: None,
                error_message: Some("Coupon not found or expired".to_string()),
            }
        };
        
        results.push(result);
    }

    Ok(Json(results))
}

fn calculate_discount(coupon: &Coupon, order_value: &bigdecimal::BigDecimal) -> Option<bigdecimal::BigDecimal> {
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    // Check minimum order requirement
    if let Some(min_order) = &coupon.minimum_order {
        if order_value < min_order {
            return None;
        }
    }

    let discount = match coupon.discount_type.as_str() {
        "percentage" => {
            if let Some(discount_value) = &coupon.discount_value {
                let discount = order_value * discount_value / BigDecimal::from_str("100").unwrap();
                if let Some(max_discount) = &coupon.maximum_discount {
                    Some(discount.min(max_discount.clone()))
                } else {
                    Some(discount)
                }
            } else {
                None
            }
        },
        "fixed" => coupon.discount_value.clone(),
        "free_shipping" => Some(BigDecimal::from_str("10").unwrap()), // Assume $10 shipping
        _ => None,
    };

    discount
}