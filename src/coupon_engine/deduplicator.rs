//! Efficient coupon deduplication using multiple strategies

use crate::coupon_engine::RawCoupon;
use std::collections::{HashMap, HashSet};
use sha2::{Sha256, Digest};

pub struct Deduplicator {
    strategy: DeduplicationStrategy,
}

#[derive(Clone)]
pub enum DeduplicationStrategy {
    /// Exact code match within same merchant
    CodeAndMerchant,
    /// Fuzzy matching based on similarity
    Fuzzy { threshold: f64 },
    /// Hash-based deduplication
    HashBased,
    /// Combined strategy
    Combined,
}

impl Deduplicator {
    pub fn new() -> Self {
        Self {
            strategy: DeduplicationStrategy::Combined,
        }
    }

    pub fn with_strategy(strategy: DeduplicationStrategy) -> Self {
        Self { strategy }
    }

    pub async fn deduplicate(&self, coupons: Vec<RawCoupon>) -> Result<Vec<RawCoupon>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.strategy {
            DeduplicationStrategy::CodeAndMerchant => {
                Ok(self.deduplicate_by_code_and_merchant(coupons))
            }
            DeduplicationStrategy::Fuzzy { threshold } => {
                Ok(self.deduplicate_fuzzy(coupons, *threshold))
            }
            DeduplicationStrategy::HashBased => {
                Ok(self.deduplicate_by_hash(coupons))
            }
            DeduplicationStrategy::Combined => {
                Ok(self.deduplicate_combined(coupons))
            }
        }
    }

    fn deduplicate_by_code_and_merchant(&self, coupons: Vec<RawCoupon>) -> Vec<RawCoupon> {
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut unique_coupons = Vec::new();

        for coupon in coupons {
            let key = (coupon.code.clone(), coupon.merchant_domain.clone());
            if seen.insert(key) {
                unique_coupons.push(coupon);
            }
        }

        unique_coupons
    }

    fn deduplicate_by_hash(&self, coupons: Vec<RawCoupon>) -> Vec<RawCoupon> {
        let mut seen_hashes: HashSet<String> = HashSet::new();
        let mut unique_coupons = Vec::new();

        for coupon in coupons {
            let hash = self.compute_coupon_hash(&coupon);
            if seen_hashes.insert(hash) {
                unique_coupons.push(coupon);
            }
        }

        unique_coupons
    }

    fn deduplicate_fuzzy(&self, coupons: Vec<RawCoupon>, threshold: f64) -> Vec<RawCoupon> {
        let mut unique_coupons = Vec::new();
        
        for coupon in coupons {
            let is_duplicate = unique_coupons.iter().any(|existing| {
                self.similarity_score(existing, &coupon) > threshold
            });

            if !is_duplicate {
                unique_coupons.push(coupon);
            }
        }

        unique_coupons
    }

    fn deduplicate_combined(&self, coupons: Vec<RawCoupon>) -> Vec<RawCoupon> {
        // First pass: exact code and merchant matching
        let coupons = self.deduplicate_by_code_and_merchant(coupons);
        
        // Second pass: fuzzy matching within same merchant
        let mut merchant_groups: HashMap<String, Vec<RawCoupon>> = HashMap::new();
        for coupon in coupons {
            merchant_groups
                .entry(coupon.merchant_domain.clone())
                .or_insert_with(Vec::new)
                .push(coupon);
        }

        let mut final_coupons = Vec::new();
        for (_, group) in merchant_groups {
            let deduped_group = self.deduplicate_fuzzy(group, 0.85);
            final_coupons.extend(deduped_group);
        }

        // Third pass: remove duplicates based on hash
        self.deduplicate_by_hash(final_coupons)
    }

    fn compute_coupon_hash(&self, coupon: &RawCoupon) -> String {
        let mut hasher = Sha256::new();
        
        // Include key fields in hash
        hasher.update(&coupon.code);
        hasher.update(&coupon.merchant_domain);
        hasher.update(&coupon.discount_type.to_string());
        
        if let Some(value) = coupon.discount_value {
            hasher.update(value.to_string());
        }

        format!("{:x}", hasher.finalize())
    }

    fn similarity_score(&self, coupon1: &RawCoupon, coupon2: &RawCoupon) -> f64 {
        let mut score = 0.0;
        let mut weight_total = 0.0;

        // Code similarity (highest weight)
        let code_similarity = self.levenshtein_similarity(&coupon1.code, &coupon2.code);
        score += code_similarity * 0.4;
        weight_total += 0.4;

        // Title similarity
        let title_similarity = self.levenshtein_similarity(&coupon1.title, &coupon2.title);
        score += title_similarity * 0.3;
        weight_total += 0.3;

        // Discount type and value
        if coupon1.discount_type == coupon2.discount_type {
            score += 0.2;
            
            if let (Some(v1), Some(v2)) = (coupon1.discount_value, coupon2.discount_value) {
                if (v1 - v2).abs() < 0.01 {
                    score += 0.1;
                }
            }
        }
        weight_total += 0.3;

        score / weight_total
    }

    fn levenshtein_similarity(&self, s1: &str, s2: &str) -> f64 {
        let distance = self.levenshtein_distance(s1, s2);
        let max_len = s1.len().max(s2.len()) as f64;
        
        if max_len == 0.0 {
            1.0
        } else {
            1.0 - (distance as f64 / max_len)
        }
    }

    fn levenshtein_distance(&self, s1: &str, s2: &str) -> usize {
        let len1 = s1.len();
        let len2 = s2.len();
        let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

        for i in 0..=len1 {
            matrix[i][0] = i;
        }

        for j in 0..=len2 {
            matrix[0][j] = j;
        }

        for (i, c1) in s1.chars().enumerate() {
            for (j, c2) in s2.chars().enumerate() {
                let cost = if c1 == c2 { 0 } else { 1 };
                matrix[i + 1][j + 1] = std::cmp::min(
                    matrix[i][j] + cost,
                    std::cmp::min(
                        matrix[i + 1][j] + 1,
                        matrix[i][j + 1] + 1,
                    ),
                );
            }
        }

        matrix[len1][len2]
    }

    /// Get statistics about deduplication
    pub fn get_deduplication_stats(&self, original: &[RawCoupon], deduplicated: &[RawCoupon]) -> DeduplicationStats {
        let original_count = original.len();
        let deduplicated_count = deduplicated.len();
        let removed_count = original_count - deduplicated_count;
        
        let mut merchant_stats = HashMap::new();
        for coupon in original {
            *merchant_stats.entry(coupon.merchant_domain.clone()).or_insert(0) += 1;
        }
        
        let mut deduplicated_merchant_stats = HashMap::new();
        for coupon in deduplicated {
            *deduplicated_merchant_stats.entry(coupon.merchant_domain.clone()).or_insert(0) += 1;
        }

        DeduplicationStats {
            original_count,
            deduplicated_count,
            removed_count,
            deduplication_rate: (removed_count as f64 / original_count as f64) * 100.0,
            merchant_stats,
            deduplicated_merchant_stats,
        }
    }
}

impl DiscountType {
    fn to_string(&self) -> String {
        match self {
            DiscountType::Percentage => "percentage",
            DiscountType::Fixed => "fixed",
            DiscountType::FreeShipping => "free_shipping",
            DiscountType::Bogo => "bogo",
            DiscountType::CashBack => "cash_back",
            DiscountType::Points => "points",
            DiscountType::Unknown => "unknown",
        }.to_string()
    }
}

#[derive(Debug)]
pub struct DeduplicationStats {
    pub original_count: usize,
    pub deduplicated_count: usize,
    pub removed_count: usize,
    pub deduplication_rate: f64,
    pub merchant_stats: HashMap<String, usize>,
    pub deduplicated_merchant_stats: HashMap<String, usize>,
}

use crate::coupon_engine::DiscountType;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coupon_engine::SourceType;
    use chrono::Utc;

    fn create_test_coupon(code: &str, merchant: &str) -> RawCoupon {
        RawCoupon {
            code: code.to_string(),
            title: format!("{} Discount", code),
            description: None,
            discount_type: DiscountType::Percentage,
            discount_value: Some(10.0),
            minimum_order: None,
            maximum_discount: None,
            valid_from: None,
            valid_until: None,
            merchant_name: merchant.to_string(),
            merchant_domain: format!("{}.com", merchant.to_lowercase()),
            source_url: format!("https://{}.com", merchant.to_lowercase()),
            source_type: SourceType::WebScraping,
            metadata: serde_json::json!({}),
            scraped_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_exact_duplicate_removal() {
        let deduplicator = Deduplicator::new();
        let coupons = vec![
            create_test_coupon("SAVE10", "Amazon"),
            create_test_coupon("SAVE10", "Amazon"), // Duplicate
            create_test_coupon("SAVE10", "Target"), // Same code, different merchant
            create_test_coupon("SAVE20", "Amazon"),
        ];

        let result = deduplicator.deduplicate(coupons).await.unwrap();
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_fuzzy_deduplication() {
        let deduplicator = Deduplicator::with_strategy(DeduplicationStrategy::Fuzzy { threshold: 0.8 });
        let coupons = vec![
            create_test_coupon("SAVE10", "Amazon"),
            create_test_coupon("SAVE1O", "Amazon"), // Similar code (O instead of 0)
            create_test_coupon("DISCOUNT20", "Amazon"),
        ];

        let result = deduplicator.deduplicate(coupons).await.unwrap();
        assert_eq!(result.len(), 2); // SAVE10 and SAVE1O should be considered similar
    }
}
