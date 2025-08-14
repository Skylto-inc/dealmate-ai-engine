use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::models::coupon::NewCoupon;

#[derive(Debug, Serialize, Deserialize)]
pub struct AffiliateApiResponse {
    pub coupons: Vec<AffiliateCoupon>,
    pub total: i32,
    pub page: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AffiliateCoupon {
    pub code: String,
    pub title: String,
    pub description: Option<String>,
    pub discount_type: String,
    pub discount_value: Option<f64>,
    pub minimum_order: Option<f64>,
    pub valid_until: Option<String>,
    pub merchant_name: String,
    pub merchant_domain: String,
}

pub struct CouponAggregator {
    _client: Client,
    pool: PgPool,
}

impl CouponAggregator {
    pub fn new(pool: PgPool) -> Self {
        Self {
            _client: Client::new(),
            pool,
        }
    }

    pub async fn aggregate_from_affiliate_networks(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Simulate affiliate network APIs
        let networks = vec![
            ("impact", "https://api.impact.com/coupons"),
            ("cj", "https://api.cj.com/coupons"),
            ("awin", "https://api.awin.com/coupons"),
        ];

        for (network_name, _api_url) in networks {
            println!("Aggregating coupons from {}", network_name);
            
            // For demo purposes, create sample coupons
            let sample_coupons = self.generate_sample_coupons(network_name);
            
            for coupon_data in sample_coupons {
                if let Err(e) = self.store_coupon(coupon_data, network_name).await {
                    eprintln!("Error storing coupon: {}", e);
                }
            }
            
            sleep(Duration::from_secs(1)).await; // Rate limiting
        }

        Ok(())
    }

    pub async fn scrape_merchant_sites(&self) -> Result<(), Box<dyn std::error::Error>> {
        let merchants = vec![
            ("amazon.com", "https://amazon.com/coupons"),
            ("target.com", "https://target.com/promotions"),
            ("walmart.com", "https://walmart.com/coupons"),
        ];

        for (domain, _url) in merchants {
            println!("Scraping coupons from {}", domain);
            
            // For demo purposes, generate sample scraped coupons
            let scraped_coupons = self.generate_sample_scraped_coupons(domain);
            
            for coupon_data in scraped_coupons {
                if let Err(e) = self.store_coupon(coupon_data, "scraping").await {
                    eprintln!("Error storing scraped coupon: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn store_coupon(&self, coupon_data: AffiliateCoupon, source: &str) -> Result<(), sqlx::Error> {
        // First, ensure merchant exists
        let merchant_id = self.ensure_merchant_exists(&coupon_data.merchant_name, &coupon_data.merchant_domain).await?;
        
        // Check if coupon already exists
        let existing = sqlx::query!(
            "SELECT id FROM coupons WHERE merchant_id = $1 AND code = $2",
            merchant_id,
            coupon_data.code
        )
        .fetch_optional(&self.pool)
        .await?;

        if existing.is_some() {
            return Ok(()); // Skip if already exists
        }

        let valid_until = coupon_data.valid_until
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));

        let new_coupon = NewCoupon {
            merchant_id,
            code: coupon_data.code,
            title: coupon_data.title,
            description: coupon_data.description,
            discount_type: coupon_data.discount_type,
            discount_value: coupon_data.discount_value.map(|v| bigdecimal::BigDecimal::from(v as i32)),
            minimum_order: coupon_data.minimum_order.map(|v| bigdecimal::BigDecimal::from(v as i32)),
            maximum_discount: None,
            valid_from: None,
            valid_until,
            usage_limit: None,
            source: source.to_string(),
            affiliate_network: Some(source.to_string()),
        };

        sqlx::query!(
            r#"INSERT INTO coupons (merchant_id, code, title, description, discount_type, 
               discount_value, minimum_order, maximum_discount, valid_from, valid_until, 
               usage_limit, source, affiliate_network) 
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#,
            new_coupon.merchant_id,
            new_coupon.code,
            new_coupon.title,
            new_coupon.description,
            new_coupon.discount_type,
            new_coupon.discount_value,
            new_coupon.minimum_order,
            new_coupon.maximum_discount,
            new_coupon.valid_from,
            new_coupon.valid_until,
            new_coupon.usage_limit,
            new_coupon.source,
            new_coupon.affiliate_network
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn ensure_merchant_exists(&self, name: &str, domain: &str) -> Result<Uuid, sqlx::Error> {
        let existing = sqlx::query!(
            "SELECT id FROM merchants WHERE domain = $1",
            domain
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(merchant) = existing {
            return Ok(merchant.id);
        }

        let new_merchant = sqlx::query!(
            "INSERT INTO merchants (name, domain) VALUES ($1, $2) RETURNING id",
            name,
            domain
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(new_merchant.id)
    }

    fn generate_sample_coupons(&self, network: &str) -> Vec<AffiliateCoupon> {
        vec![
            AffiliateCoupon {
                code: format!("{}10OFF", network.to_uppercase()),
                title: "10% Off Everything".to_string(),
                description: Some("Get 10% off your entire order".to_string()),
                discount_type: "percentage".to_string(),
                discount_value: Some(10.0),
                minimum_order: Some(50.0),
                valid_until: Some("2024-12-31T23:59:59Z".to_string()),
                merchant_name: "Sample Store".to_string(),
                merchant_domain: "samplestore.com".to_string(),
            },
            AffiliateCoupon {
                code: format!("{}SHIP", network.to_uppercase()),
                title: "Free Shipping".to_string(),
                description: Some("Free shipping on all orders".to_string()),
                discount_type: "free_shipping".to_string(),
                discount_value: None,
                minimum_order: Some(25.0),
                valid_until: Some("2024-12-31T23:59:59Z".to_string()),
                merchant_name: "Sample Store".to_string(),
                merchant_domain: "samplestore.com".to_string(),
            },
        ]
    }

    fn generate_sample_scraped_coupons(&self, domain: &str) -> Vec<AffiliateCoupon> {
        vec![
            AffiliateCoupon {
                code: "SAVE20".to_string(),
                title: "$20 Off $100+".to_string(),
                description: Some("Save $20 on orders over $100".to_string()),
                discount_type: "fixed".to_string(),
                discount_value: Some(20.0),
                minimum_order: Some(100.0),
                valid_until: Some("2024-12-31T23:59:59Z".to_string()),
                merchant_name: domain.replace(".com", "").to_string(),
                merchant_domain: domain.to_string(),
            },
        ]
    }

    pub async fn cleanup_expired_coupons(&self) -> Result<i64, sqlx::Error> {
        let result = sqlx::query!(
            "UPDATE coupons SET is_active = false WHERE valid_until < NOW() AND is_active = true"
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }
}
