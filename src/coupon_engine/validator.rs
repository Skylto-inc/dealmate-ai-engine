//! Coupon validation module for verifying coupon data quality and validity

use crate::coupon_engine::{RawCoupon, DiscountType};
use chrono::Utc;
use regex::Regex;
use std::collections::HashSet;
use lazy_static::lazy_static;

lazy_static! {
    static ref VALID_CODE_PATTERN: Regex = Regex::new(r"^[A-Z0-9]{3,50}$").unwrap();
    static ref SPAM_KEYWORDS: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("TEST");
        set.insert("DEMO");
        set.insert("EXAMPLE");
        set.insert("FAKE");
        set.insert("INVALID");
        set
    };
}

pub struct Validator {
    min_discount_value: f64,
    max_discount_percentage: f64,
    max_future_days: i64,
}

impl Validator {
    pub fn new() -> Self {
        Self {
            min_discount_value: 1.0,
            max_discount_percentage: 99.0,
            max_future_days: 365,
        }
    }

    pub async fn is_valid(&self, coupon: &RawCoupon) -> bool {
        // Basic validation checks
        if !self.validate_code(&coupon.code) {
            return false;
        }

        if !self.validate_discount(&coupon.discount_type, coupon.discount_value) {
            return false;
        }

        if !self.validate_dates(coupon) {
            return false;
        }

        if !self.validate_merchant(coupon) {
            return false;
        }

        true
    }

    fn validate_code(&self, code: &str) -> bool {
        // Check if code matches valid pattern
        if !VALID_CODE_PATTERN.is_match(code) {
            return false;
        }

        // Check for spam keywords
        let code_upper = code.to_uppercase();
        for keyword in SPAM_KEYWORDS.iter() {
            if code_upper.contains(keyword) {
                return false;
            }
        }

        // Check for repetitive patterns
        if self.has_repetitive_pattern(code) {
            return false;
        }

        true
    }

    fn validate_discount(&self, discount_type: &DiscountType, value: Option<f64>) -> bool {
        match discount_type {
            DiscountType::Percentage => {
                if let Some(v) = value {
                    v >= self.min_discount_value && v <= self.max_discount_percentage
                } else {
                    false
                }
            }
            DiscountType::Fixed => {
                if let Some(v) = value {
                    v >= self.min_discount_value && v <= 10000.0 // Max $10,000 discount
                } else {
                    false
                }
            }
            DiscountType::FreeShipping | DiscountType::Bogo => true,
            DiscountType::CashBack => {
                if let Some(v) = value {
                    v >= self.min_discount_value && v <= 100.0
                } else {
                    false
                }
            }
            DiscountType::Points => {
                if let Some(v) = value {
                    v >= 1.0 && v <= 100000.0
                } else {
                    false
                }
            }
            DiscountType::Unknown => false,
        }
    }

    fn validate_dates(&self, coupon: &RawCoupon) -> bool {
        let now = Utc::now();

        // Check if coupon has already expired
        if let Some(valid_until) = coupon.valid_until {
            if valid_until < now {
                return false;
            }

            // Check if expiry date is too far in the future
            let days_diff = (valid_until - now).num_days();
            if days_diff > self.max_future_days {
                return false;
            }
        }

        // Check if valid_from is in the past (if specified)
        if let Some(valid_from) = coupon.valid_from {
            if valid_from > now {
                // Coupon not yet active
                return false;
            }

            // Check logical date ordering
            if let Some(valid_until) = coupon.valid_until {
                if valid_from >= valid_until {
                    return false;
                }
            }
        }

        true
    }

    fn validate_merchant(&self, coupon: &RawCoupon) -> bool {
        // Check merchant name length
        if coupon.merchant_name.is_empty() || coupon.merchant_name.len() > 100 {
            return false;
        }

        // Check merchant domain
        if coupon.merchant_domain.is_empty() || !self.is_valid_domain(&coupon.merchant_domain) {
            return false;
        }

        true
    }

    fn is_valid_domain(&self, domain: &str) -> bool {
        // Basic domain validation
        if domain.len() < 4 || domain.len() > 253 {
            return false;
        }

        // Check for valid characters
        let domain_pattern = Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?(\.[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?)*$").unwrap();
        domain_pattern.is_match(domain)
    }

    fn has_repetitive_pattern(&self, code: &str) -> bool {
        // Check for patterns like AAAA, 1111, ABAB
        if code.len() < 4 {
            return false;
        }

        // Check if all characters are the same
        let first_char = code.chars().next().unwrap();
        if code.chars().all(|c| c == first_char) {
            return true;
        }

        // Check for alternating patterns (ABAB)
        if code.len() >= 4 {
            let chars: Vec<char> = code.chars().collect();
            if chars.len() >= 4 && chars[0] == chars[2] && chars[1] == chars[3] {
                // Check if the entire string follows this pattern
                let mut follows_pattern = true;
                for i in (4..chars.len()).step_by(2) {
                    if i < chars.len() && chars[i] != chars[0] {
                        follows_pattern = false;
                        break;
                    }
                    if i + 1 < chars.len() && chars[i + 1] != chars[1] {
                        follows_pattern = false;
                        break;
                    }
                }
                if follows_pattern {
                    return true;
                }
            }
        }

        false
    }

    /// Batch validation with detailed results
    pub async fn validate_batch(&self, coupons: Vec<RawCoupon>) -> Vec<ValidationResult> {
        let mut results = Vec::new();

        for coupon in coupons {
            let is_valid = self.is_valid(&coupon).await;
            let reasons = if !is_valid {
                self.get_validation_errors(&coupon)
            } else {
                Vec::new()
            };

            results.push(ValidationResult {
                coupon,
                is_valid,
                validation_errors: reasons,
            });
        }

        results
    }

    fn get_validation_errors(&self, coupon: &RawCoupon) -> Vec<String> {
        let mut errors = Vec::new();

        if !self.validate_code(&coupon.code) {
            errors.push(format!("Invalid coupon code: {}", coupon.code));
        }

        if !self.validate_discount(&coupon.discount_type, coupon.discount_value) {
            errors.push("Invalid discount value".to_string());
        }

        if !self.validate_dates(coupon) {
            errors.push("Invalid or expired dates".to_string());
        }

        if !self.validate_merchant(coupon) {
            errors.push("Invalid merchant information".to_string());
        }

        errors
    }
}

#[derive(Debug)]
pub struct ValidationResult {
    pub coupon: RawCoupon,
    pub is_valid: bool,
    pub validation_errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coupon_engine::SourceType;

    #[tokio::test]
    async fn test_valid_coupon() {
        let validator = Validator::new();
        let coupon = RawCoupon {
            code: "SAVE20".to_string(),
            title: "20% Off".to_string(),
            description: None,
            discount_type: DiscountType::Percentage,
            discount_value: Some(20.0),
            minimum_order: None,
            maximum_discount: None,
            valid_from: None,
            valid_until: Some(Utc::now() + chrono::Duration::days(30)),
            merchant_name: "Test Store".to_string(),
            merchant_domain: "teststore.com".to_string(),
            source_url: "https://teststore.com".to_string(),
            source_type: SourceType::WebScraping,
            metadata: serde_json::json!({}),
            scraped_at: Utc::now(),
        };

        assert!(validator.is_valid(&coupon).await);
    }

    #[tokio::test]
    async fn test_invalid_code_pattern() {
        let validator = Validator::new();
        let coupon = RawCoupon {
            code: "AAAA".to_string(), // Repetitive pattern
            title: "Test".to_string(),
            description: None,
            discount_type: DiscountType::Percentage,
            discount_value: Some(10.0),
            minimum_order: None,
            maximum_discount: None,
            valid_from: None,
            valid_until: Some(Utc::now() + chrono::Duration::days(30)),
            merchant_name: "Test Store".to_string(),
            merchant_domain: "teststore.com".to_string(),
            source_url: "https://teststore.com".to_string(),
            source_type: SourceType::WebScraping,
            metadata: serde_json::json!({}),
            scraped_at: Utc::now(),
        };

        assert!(!validator.is_valid(&coupon).await);
    }
}
