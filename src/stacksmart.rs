use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use reqwest;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum DealType {
    #[serde(rename = "coupon")]
    Coupon,
    #[serde(rename = "cashback")]
    Cashback,
    #[serde(rename = "discount")]
    Discount,
    #[serde(rename = "card_offer")]
    CardOffer,
    #[serde(rename = "wallet_offer")]
    WalletOffer,
    #[serde(rename = "membership")]
    Membership,
    #[serde(rename = "referral")]
    Referral,
    #[serde(rename = "bundle")]
    Bundle,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Deal {
    pub id: String,
    pub title: String,
    pub description: String,
    pub deal_type: DealType,
    pub value: f64,
    pub value_type: String,
    pub code: Option<String>,
    pub min_purchase: Option<f64>,
    pub max_discount: Option<f64>,
    pub platform: String,
    pub confidence: f64,
    pub stackable: bool,
    pub terms: Vec<String>,
    pub priority: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StackedDealResult {
    pub deals: Vec<Deal>,
    pub total_savings: f64,
    pub final_price: f64,
    pub original_price: f64,
    pub confidence: f64,
    pub application_order: Vec<String>,
    pub warnings: Vec<String>,
    pub processing_time: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StackDealsRequest {
    pub deals: Vec<Deal>,
    pub base_price: f64,
    pub user_context: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidateStackRequest {
    pub deals: Vec<Deal>,
    pub base_price: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidateStackResponse {
    pub valid: bool,
    pub total_savings: Option<f64>,
    pub final_price: Option<f64>,
    pub confidence: Option<f64>,
    pub warnings: Vec<String>,
    pub error: Option<String>,
}

pub struct StackSmartEngine;

impl StackSmartEngine {
    pub fn new() -> Self {
        StackSmartEngine
    }

    pub async fn optimize_deals(&self, request: StackDealsRequest) -> StackedDealResult {
        let client = reqwest::Client::new();
        let res = client
            .post("http://localhost:8001/optimize-deals")
            .json(&request)
            .send()
            .await
            .unwrap()
            .json::<StackedDealResult>()
            .await
            .unwrap();
        res
    }

    pub async fn validate_deal_stack(&self, request: ValidateStackRequest) -> ValidateStackResponse {
        // This is a placeholder for the validation logic.
        let final_price = request.base_price * 0.9; // a dummy 10% discount
        let total_savings = request.base_price - final_price;

        ValidateStackResponse {
            valid: true,
            total_savings: Some(total_savings),
            final_price: Some(final_price),
            confidence: Some(0.9),
            warnings: vec![],
            error: None,
        }
    }
}
