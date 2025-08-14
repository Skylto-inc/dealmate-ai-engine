//! High-performance coupon aggregation engine
//! 
//! This module provides the core Rust components for efficient coupon aggregation,
//! including concurrent HTTP requests, HTML/JSON parsing, rate limiting, and data validation.

pub mod scraper;
pub mod parser;
pub mod validator;
pub mod deduplicator;
pub mod rate_limiter;
pub mod proxy_manager;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Core coupon data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawCoupon {
    pub code: String,
    pub title: String,
    pub description: Option<String>,
    pub discount_type: DiscountType,
    pub discount_value: Option<f64>,
    pub minimum_order: Option<f64>,
    pub maximum_discount: Option<f64>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub merchant_name: String,
    pub merchant_domain: String,
    pub source_url: String,
    pub source_type: SourceType,
    pub metadata: serde_json::Value,
    pub scraped_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DiscountType {
    Percentage,
    Fixed,
    FreeShipping,
    Bogo,
    CashBack,
    Points,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    AffiliateApi,
    WebScraping,
    UserSubmitted,
    PartnerApi,
}

/// Configuration for the coupon engine
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub max_concurrent_requests: usize,
    pub request_timeout_secs: u64,
    pub retry_attempts: u32,
    pub rate_limit_per_domain: u32,
    pub proxy_rotation_enabled: bool,
    pub user_agent_rotation: bool,
    pub cache_duration_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 100,
            request_timeout_secs: 30,
            retry_attempts: 3,
            rate_limit_per_domain: 10,
            proxy_rotation_enabled: true,
            user_agent_rotation: true,
            cache_duration_secs: 3600,
        }
    }
}

/// Main coupon aggregation engine
pub struct CouponEngine {
    config: EngineConfig,
    scraper: Arc<scraper::Scraper>,
    parser: Arc<parser::Parser>,
    validator: Arc<validator::Validator>,
    deduplicator: Arc<deduplicator::Deduplicator>,
    rate_limiter: Arc<rate_limiter::RateLimiter>,
    _proxy_manager: Option<Arc<proxy_manager::ProxyManager>>,
}

impl CouponEngine {
    pub fn new(config: EngineConfig) -> Self {
        let proxy_manager = if config.proxy_rotation_enabled {
            Some(Arc::new(proxy_manager::ProxyManager::new()))
        } else {
            None
        };

        Self {
            scraper: Arc::new(scraper::Scraper::new(config.clone())),
            parser: Arc::new(parser::Parser::new()),
            validator: Arc::new(validator::Validator::new()),
            deduplicator: Arc::new(deduplicator::Deduplicator::new()),
            rate_limiter: Arc::new(rate_limiter::RateLimiter::new(config.rate_limit_per_domain)),
            _proxy_manager: proxy_manager,
            config,
        }
    }

    /// Process a batch of URLs for coupon extraction
    pub async fn process_batch(&self, urls: Vec<String>) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let mut all_coupons = Vec::new();
        
        // Process URLs concurrently with rate limiting
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent_requests));
        let mut tasks: Vec<tokio::task::JoinHandle<Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>>>> = Vec::new();

        for url in urls {
            let sem = semaphore.clone();
            let scraper = self.scraper.clone();
            let parser = self.parser.clone();
            let validator = self.validator.clone();
            let rate_limiter = self.rate_limiter.clone();
            
            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                
                // Apply rate limiting per domain
                if let Ok(domain) = Self::extract_domain(&url) {
                    rate_limiter.wait_if_needed(&domain).await;
                }
                
                // Scrape content
                match scraper.fetch_content(&url).await {
                    Ok(content) => {
                        // Parse coupons from content
                        match parser.extract_coupons(&content, &url).await {
                            Ok(coupons) => {
                                // Validate each coupon
                                let mut valid_coupons = Vec::new();
                                for coupon in coupons {
                                    if validator.is_valid(&coupon).await {
                                        valid_coupons.push(coupon);
                                    }
                                }
                                Ok(valid_coupons)
                            }
                            Err(e) => {
                                eprintln!("Failed to parse {}: {}", url, e);
                                Ok(Vec::new())
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to fetch {}: {}", url, e);
                        Ok(Vec::new())
                    }
                }
            });
            
            tasks.push(task);
        }

        // Collect results
        for task in tasks {
            if let Ok(Ok(coupons)) = task.await {
                all_coupons.extend(coupons);
            }
        }

        // Deduplicate coupons
        let unique_coupons = self.deduplicator.deduplicate(all_coupons).await?;
        
        Ok(unique_coupons)
    }

    /// Extract domain from URL
    fn extract_domain(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let parsed = url::Url::parse(url)?;
        Ok(parsed.host_str().unwrap_or("").to_string())
    }
}

/// Python interop functions (currently disabled - add "python" feature in Cargo.toml to enable)
#[cfg(feature = "python")]
#[allow(dead_code)]
pub mod python_bindings {
    use super::*;
    use pyo3::prelude::*;

    #[pyclass]
    pub struct PyCouponEngine {
        engine: Arc<CouponEngine>,
    }

    #[pymethods]
    impl PyCouponEngine {
        #[new]
        pub fn new(config: Option<&str>) -> PyResult<Self> {
            let config = if let Some(config_str) = config {
                serde_json::from_str(config_str)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
            } else {
                EngineConfig::default()
            };

            Ok(Self {
                engine: Arc::new(CouponEngine::new(config)),
            })
        }

        pub fn process_urls(&self, urls: Vec<String>) -> PyResult<String> {
            let engine = self.engine.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let coupons = rt.block_on(async move {
                engine.process_batch(urls).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let json = serde_json::to_string(&coupons)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
            
            Ok(json)
        }
    }

    /// Python module initialization
    #[pymodule]
    fn dealpal_coupon_engine(_py: Python, m: &PyModule) -> PyResult<()> {
        m.add_class::<PyCouponEngine>()?;
        Ok(())
    }
}
