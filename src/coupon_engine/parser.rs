//! High-performance coupon parser for HTML, JSON, and CSV content

use crate::coupon_engine::{RawCoupon, DiscountType, SourceType};
use chrono::{DateTime, Utc};
use regex::Regex;
use scraper::{Html, Selector};
use serde_json::Value;
use std::collections::HashMap;

pub struct Parser {
    html_parsers: HashMap<String, HtmlParser>,
    json_parsers: HashMap<String, JsonParser>,
    regex_patterns: RegexPatterns,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            html_parsers: Self::init_html_parsers(),
            json_parsers: Self::init_json_parsers(),
            regex_patterns: RegexPatterns::new(),
        }
    }

    pub async fn extract_coupons(
        &self,
        content: &str,
        source_url: &str,
    ) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let content_type = crate::coupon_engine::scraper::detect_content_type(content);
        let domain = Self::extract_domain(source_url)?;

        match content_type {
            crate::coupon_engine::scraper::ContentType::Html => {
                self.parse_html(content, source_url, &domain).await
            }
            crate::coupon_engine::scraper::ContentType::Json => {
                self.parse_json(content, source_url, &domain).await
            }
            crate::coupon_engine::scraper::ContentType::Csv => {
                self.parse_csv(content, source_url, &domain).await
            }
            _ => {
                // Try to extract coupons using regex patterns
                self.parse_with_regex(content, source_url, &domain).await
            }
        }
    }

    async fn parse_html(
        &self,
        content: &str,
        source_url: &str,
        domain: &str,
    ) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let mut coupons = Vec::new();
        let document = Html::parse_document(content);

        // Try domain-specific parser first
        if let Some(parser) = self.html_parsers.get(domain) {
            coupons.extend(parser.parse(&document, source_url)?);
        }

        // Generic coupon extraction
        let generic_parser = &self.html_parsers["generic"];
        coupons.extend(generic_parser.parse(&document, source_url)?);

        // Extract using regex patterns on text content
        let text_content = document.root_element().text().collect::<String>();
        coupons.extend(self.extract_from_text(&text_content, source_url, domain)?);

        Ok(coupons)
    }

    async fn parse_json(
        &self,
        content: &str,
        source_url: &str,
        domain: &str,
    ) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let value: Value = serde_json::from_str(content)?;
        
        // Try domain-specific parser
        if let Some(parser) = self.json_parsers.get(domain) {
            return Ok(parser.parse(&value, source_url)?);
        }

        // Generic JSON parsing
        self.json_parsers["generic"].parse(&value, source_url)
    }

    async fn parse_csv(
        &self,
        content: &str,
        source_url: &str,
        domain: &str,
    ) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let mut coupons = Vec::new();
        let mut reader = csv::Reader::from_reader(content.as_bytes());

        for result in reader.records() {
            let record = result?;
            if let Some(coupon) = self.parse_csv_record(&record, source_url, domain) {
                coupons.push(coupon);
            }
        }

        Ok(coupons)
    }

    async fn parse_with_regex(
        &self,
        content: &str,
        source_url: &str,
        domain: &str,
    ) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        self.extract_from_text(content, source_url, domain)
    }

    fn extract_from_text(
        &self,
        text: &str,
        source_url: &str,
        domain: &str,
    ) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let mut coupons = Vec::new();

        // Extract coupon codes
        for cap in self.regex_patterns.code_pattern.captures_iter(text) {
            if let Some(code) = cap.get(1) {
                let code_str = code.as_str().to_uppercase();
                
                // Find associated discount info
                let discount_info = self.find_discount_info(text, code.start(), code.end());
                
                let coupon = RawCoupon {
                    code: code_str.clone(),
                    title: discount_info.title.unwrap_or_else(|| format!("Coupon Code: {}", code_str)),
                    description: discount_info.description,
                    discount_type: discount_info.discount_type,
                    discount_value: discount_info.discount_value,
                    minimum_order: discount_info.minimum_order,
                    maximum_discount: None,
                    valid_from: None,
                    valid_until: discount_info.expiry_date,
                    merchant_name: domain.to_string(),
                    merchant_domain: domain.to_string(),
                    source_url: source_url.to_string(),
                    source_type: SourceType::WebScraping,
                    metadata: serde_json::json!({}),
                    scraped_at: Utc::now(),
                };
                
                coupons.push(coupon);
            }
        }

        Ok(coupons)
    }

    fn find_discount_info(&self, text: &str, code_start: usize, code_end: usize) -> DiscountInfo {
        let context_range = 200; // Look 200 chars before and after
        let start = code_start.saturating_sub(context_range);
        let end = (code_end + context_range).min(text.len());
        let context = &text[start..end];

        let mut info = DiscountInfo::default();

        // Extract percentage discount
        if let Some(cap) = self.regex_patterns.percentage_pattern.captures(context) {
            if let Some(value) = cap.get(1) {
                info.discount_type = DiscountType::Percentage;
                info.discount_value = value.as_str().parse().ok();
                info.title = Some(format!("{}% Off", value.as_str()));
            }
        }

        // Extract fixed discount
        if info.discount_value.is_none() {
            if let Some(cap) = self.regex_patterns.fixed_pattern.captures(context) {
                if let Some(value) = cap.get(1) {
                    info.discount_type = DiscountType::Fixed;
                    info.discount_value = value.as_str().parse().ok();
                    info.title = Some(format!("${} Off", value.as_str()));
                }
            }
        }

        // Extract minimum order
        if let Some(cap) = self.regex_patterns.minimum_pattern.captures(context) {
            if let Some(value) = cap.get(1) {
                info.minimum_order = value.as_str().parse().ok();
            }
        }

        // Extract description
        info.description = Some(context.trim().to_string());

        info
    }

    fn parse_csv_record(
        &self,
        record: &csv::StringRecord,
        source_url: &str,
        domain: &str,
    ) -> Option<RawCoupon> {
        // Assuming standard CSV format with columns: code, title, discount_type, discount_value, expiry
        if record.len() < 2 {
            return None;
        }

        let code = record.get(0)?.trim().to_uppercase();
        let title = record.get(1).map(|s| s.trim().to_string())
            .unwrap_or_else(|| format!("Coupon: {}", code));

        let discount_type = record.get(2)
            .and_then(|s| match s.trim().to_lowercase().as_str() {
                "percentage" | "percent" | "%" => Some(DiscountType::Percentage),
                "fixed" | "amount" | "$" => Some(DiscountType::Fixed),
                "free_shipping" | "shipping" => Some(DiscountType::FreeShipping),
                _ => None,
            })
            .unwrap_or(DiscountType::Unknown);

        let discount_value = record.get(3)
            .and_then(|s| s.trim().parse().ok());

        Some(RawCoupon {
            code,
            title,
            description: None,
            discount_type,
            discount_value,
            minimum_order: None,
            maximum_discount: None,
            valid_from: None,
            valid_until: None,
            merchant_name: domain.to_string(),
            merchant_domain: domain.to_string(),
            source_url: source_url.to_string(),
            source_type: SourceType::WebScraping,
            metadata: serde_json::json!({}),
            scraped_at: Utc::now(),
        })
    }

    fn init_html_parsers() -> HashMap<String, HtmlParser> {
        let mut parsers = HashMap::new();
        
        // Generic parser
        parsers.insert("generic".to_string(), HtmlParser::generic());
        
        // Domain-specific parsers
        parsers.insert("retailmenot.com".to_string(), HtmlParser::retailmenot());
        parsers.insert("coupons.com".to_string(), HtmlParser::coupons_com());
        
        parsers
    }

    fn init_json_parsers() -> HashMap<String, JsonParser> {
        let mut parsers = HashMap::new();
        
        parsers.insert("generic".to_string(), JsonParser::generic());
        
        parsers
    }

    fn extract_domain(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let parsed = url::Url::parse(url)?;
        Ok(parsed.host_str().unwrap_or("").to_string())
    }
}

struct HtmlParser {
    selectors: Vec<(Selector, CouponExtractor)>,
}

impl HtmlParser {
    fn generic() -> Self {
        Self {
            selectors: vec![
                (
                    Selector::parse("[class*='coupon-code']").unwrap(),
                    CouponExtractor::generic(),
                ),
                (
                    Selector::parse("[data-coupon-code]").unwrap(),
                    CouponExtractor::data_attribute(),
                ),
                (
                    Selector::parse(".promo-code, .discount-code").unwrap(),
                    CouponExtractor::generic(),
                ),
            ],
        }
    }

    fn retailmenot() -> Self {
        Self {
            selectors: vec![
                (
                    Selector::parse("[data-clipboard-text]").unwrap(),
                    CouponExtractor::retailmenot(),
                ),
            ],
        }
    }

    fn coupons_com() -> Self {
        Self {
            selectors: vec![
                (
                    Selector::parse(".coupon-item").unwrap(),
                    CouponExtractor::coupons_com(),
                ),
            ],
        }
    }

    fn parse(&self, document: &Html, source_url: &str) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let mut coupons = Vec::new();
        
        for (selector, extractor) in &self.selectors {
            for element in document.select(selector) {
                if let Some(coupon) = extractor.extract(&element, source_url) {
                    coupons.push(coupon);
                }
            }
        }
        
        Ok(coupons)
    }
}

struct JsonParser;

impl JsonParser {
    fn generic() -> Self {
        Self
    }

    fn parse(&self, value: &Value, source_url: &str) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        let mut coupons = Vec::new();
        
        // Try to find coupon arrays in common patterns
        if let Some(arr) = value.as_array() {
            for item in arr {
                if let Some(coupon) = self.extract_coupon_from_json(item, source_url) {
                    coupons.push(coupon);
                }
            }
        } else if let Some(obj) = value.as_object() {
            // Look for common keys that might contain coupons
            for key in &["coupons", "deals", "offers", "promotions", "data", "results"] {
                if let Some(Value::Array(arr)) = obj.get(*key) {
                    for item in arr {
                        if let Some(coupon) = self.extract_coupon_from_json(item, source_url) {
                            coupons.push(coupon);
                        }
                    }
                }
            }
        }
        
        Ok(coupons)
    }

    fn extract_coupon_from_json(&self, value: &Value, source_url: &str) -> Option<RawCoupon> {
        let obj = value.as_object()?;
        
        let code = obj.get("code")
            .or(obj.get("couponCode"))
            .or(obj.get("promoCode"))
            .and_then(|v| v.as_str())?
            .to_uppercase();

        let title = obj.get("title")
            .or(obj.get("name"))
            .or(obj.get("description"))
            .and_then(|v| v.as_str())
            .unwrap_or("Coupon")
            .to_string();

        Some(RawCoupon {
            code,
            title,
            description: obj.get("description").and_then(|v| v.as_str()).map(String::from),
            discount_type: DiscountType::Unknown,
            discount_value: obj.get("discountValue").and_then(|v| v.as_f64()),
            minimum_order: obj.get("minimumOrder").and_then(|v| v.as_f64()),
            maximum_discount: None,
            valid_from: None,
            valid_until: None,
            merchant_name: "Unknown".to_string(),
            merchant_domain: Parser::extract_domain(source_url).unwrap_or_default(),
            source_url: source_url.to_string(),
            source_type: SourceType::AffiliateApi,
            metadata: value.clone(),
            scraped_at: Utc::now(),
        })
    }
}

struct CouponExtractor;

impl CouponExtractor {
    fn generic() -> Self {
        Self
    }

    fn data_attribute() -> Self {
        Self
    }

    fn retailmenot() -> Self {
        Self
    }

    fn coupons_com() -> Self {
        Self
    }

    fn extract(&self, element: &scraper::ElementRef, source_url: &str) -> Option<RawCoupon> {
        // Extract code from various attributes or text
        let code = if let Some(attr_code) = element.value().attr("data-coupon-code")
            .or(element.value().attr("data-clipboard-text")) {
            attr_code.to_uppercase()
        } else {
            let text = element.text().collect::<String>();
            text.trim().split_whitespace().next()?.to_uppercase()
        };

        if code.len() < 3 || code.len() > 50 {
            return None; // Invalid code length
        }

        let title = element.value().attr("data-title")
            .or(element.value().attr("title"))
            .unwrap_or("Coupon Code")
            .to_string();

        Some(RawCoupon {
            code,
            title,
            description: None,
            discount_type: DiscountType::Unknown,
            discount_value: None,
            minimum_order: None,
            maximum_discount: None,
            valid_from: None,
            valid_until: None,
            merchant_name: "Unknown".to_string(),
            merchant_domain: Parser::extract_domain(source_url).unwrap_or_default(),
            source_url: source_url.to_string(),
            source_type: SourceType::WebScraping,
            metadata: serde_json::json!({}),
            scraped_at: Utc::now(),
        })
    }
}

#[derive(Default)]
struct DiscountInfo {
    title: Option<String>,
    description: Option<String>,
    discount_type: DiscountType,
    discount_value: Option<f64>,
    minimum_order: Option<f64>,
    expiry_date: Option<DateTime<Utc>>,
}

impl Default for DiscountType {
    fn default() -> Self {
        DiscountType::Unknown
    }
}

struct RegexPatterns {
    code_pattern: Regex,
    percentage_pattern: Regex,
    fixed_pattern: Regex,
    minimum_pattern: Regex,
}

impl RegexPatterns {
    fn new() -> Self {
        Self {
            code_pattern: Regex::new(r"(?i)(?:code|coupon|promo)[\s:]*([A-Z0-9]{3,20})").unwrap(),
            percentage_pattern: Regex::new(r"(\d+)\s*%\s*off").unwrap(),
            fixed_pattern: Regex::new(r"\$(\d+(?:\.\d{2})?)\s*off").unwrap(),
            minimum_pattern: Regex::new(r"(?i)minimum\s*(?:order|purchase)[\s:]*\$?(\d+(?:\.\d{2})?)").unwrap(),
        }
    }
}
