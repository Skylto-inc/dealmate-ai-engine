use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use bigdecimal::BigDecimal;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Merchant {
    pub id: Uuid,
    pub name: String,
    pub domain: String,
    pub affiliate_network: Option<String>,
    pub commission_rate: Option<BigDecimal>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewMerchant {
    pub name: String,
    pub domain: String,
    pub affiliate_network: Option<String>,
    pub commission_rate: Option<BigDecimal>,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Coupon {
    pub id: Uuid,
    pub merchant_id: Uuid,
    pub code: String,
    pub title: String,
    pub description: Option<String>,
    pub discount_type: String,
    pub discount_value: Option<BigDecimal>,
    pub minimum_order: Option<BigDecimal>,
    pub maximum_discount: Option<BigDecimal>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub usage_limit: Option<i32>,
    pub usage_count: Option<i32>,
    pub is_active: Option<bool>,
    pub source: String,
    pub affiliate_network: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewCoupon {
    pub merchant_id: Uuid,
    pub code: String,
    pub title: String,
    pub description: Option<String>,
    pub discount_type: String,
    pub discount_value: Option<BigDecimal>,
    pub minimum_order: Option<BigDecimal>,
    pub maximum_discount: Option<BigDecimal>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub usage_limit: Option<i32>,
    pub source: String,
    pub affiliate_network: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct CouponTest {
    pub id: Uuid,
    pub coupon_id: Uuid,
    pub test_date: DateTime<Utc>,
    pub is_valid: bool,
    pub error_message: Option<String>,
    pub discount_applied: Option<BigDecimal>,
    pub test_order_value: Option<BigDecimal>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewCouponTest {
    pub coupon_id: Uuid,
    pub is_valid: bool,
    pub error_message: Option<String>,
    pub discount_applied: Option<BigDecimal>,
    pub test_order_value: Option<BigDecimal>,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct AffiliateNetwork {
    pub id: Uuid,
    pub name: String,
    pub api_endpoint: Option<String>,
    pub api_key_encrypted: Option<String>,
    pub commission_rate: Option<BigDecimal>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CouponSearchQuery {
    pub merchant_domain: Option<String>,
    pub discount_type: Option<String>,
    pub minimum_discount: Option<BigDecimal>,
    pub active_only: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CouponTestRequest {
    pub coupon_codes: Vec<String>,
    pub merchant_domain: String,
    pub order_value: BigDecimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CouponTestResult {
    pub code: String,
    pub is_valid: bool,
    pub discount_applied: Option<BigDecimal>,
    pub final_price: Option<BigDecimal>,
    pub error_message: Option<String>,
}