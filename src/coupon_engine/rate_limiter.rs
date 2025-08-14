//! Rate limiting module for controlling request frequency per domain

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

pub struct RateLimiter {
    limits: Arc<Mutex<HashMap<String, DomainLimit>>>,
    default_rate: u32,
}

struct DomainLimit {
    max_requests: u32,
    window_duration: Duration,
    request_times: Vec<Instant>,
}

impl RateLimiter {
    pub fn new(default_rate_per_minute: u32) -> Self {
        Self {
            limits: Arc::new(Mutex::new(HashMap::new())),
            default_rate: default_rate_per_minute,
        }
    }

    pub async fn wait_if_needed(&self, domain: &str) {
        let mut limits = self.limits.lock().await;
        
        let limit = limits.entry(domain.to_string()).or_insert_with(|| {
            DomainLimit {
                max_requests: self.default_rate,
                window_duration: Duration::from_secs(60),
                request_times: Vec::new(),
            }
        });

        // Clean up old request times
        let now = Instant::now();
        limit.request_times.retain(|&time| now.duration_since(time) < limit.window_duration);

        // Check if we need to wait
        if limit.request_times.len() >= limit.max_requests as usize {
            // Calculate wait time
            if let Some(&oldest) = limit.request_times.first() {
                let elapsed = now.duration_since(oldest);
                if elapsed < limit.window_duration {
                    let wait_time = limit.window_duration - elapsed + Duration::from_millis(100);
                    drop(limits); // Release lock while waiting
                    sleep(wait_time).await;
                    
                    // Re-acquire lock and clean up
                    let mut limits = self.limits.lock().await;
                    if let Some(limit) = limits.get_mut(domain) {
                        let now = Instant::now();
                        limit.request_times.retain(|&time| now.duration_since(time) < limit.window_duration);
                    }
                }
            }
        }

        // Record this request
        let mut limits = self.limits.lock().await;
        if let Some(limit) = limits.get_mut(domain) {
            limit.request_times.push(Instant::now());
        }
    }

    pub async fn set_domain_limit(&self, domain: &str, max_requests_per_minute: u32) {
        let mut limits = self.limits.lock().await;
        limits.insert(
            domain.to_string(),
            DomainLimit {
                max_requests: max_requests_per_minute,
                window_duration: Duration::from_secs(60),
                request_times: Vec::new(),
            },
        );
    }

    pub async fn get_current_rate(&self, domain: &str) -> Option<usize> {
        let limits = self.limits.lock().await;
        limits.get(domain).map(|limit| {
            let now = Instant::now();
            limit.request_times.iter()
                .filter(|&&time| now.duration_since(time) < limit.window_duration)
                .count()
        })
    }

    pub async fn reset_domain(&self, domain: &str) {
        let mut limits = self.limits.lock().await;
        if let Some(limit) = limits.get_mut(domain) {
            limit.request_times.clear();
        }
    }

    /// Advanced rate limiting with burst support
    pub fn with_burst_support(default_rate_per_minute: u32, burst_size: u32) -> BurstRateLimiter {
        BurstRateLimiter::new(default_rate_per_minute, burst_size)
    }
}

/// Token bucket implementation for burst rate limiting
pub struct BurstRateLimiter {
    buckets: Arc<Mutex<HashMap<String, TokenBucket>>>,
    default_rate: u32,
    default_burst: u32,
}

struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl BurstRateLimiter {
    pub fn new(default_rate_per_minute: u32, default_burst: u32) -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
            default_rate: default_rate_per_minute,
            default_burst: default_burst,
        }
    }

    pub async fn acquire(&self, domain: &str, tokens: f64) -> Result<(), RateLimitError> {
        let mut buckets = self.buckets.lock().await;
        
        let bucket = buckets.entry(domain.to_string()).or_insert_with(|| {
            TokenBucket {
                capacity: self.default_burst as f64,
                tokens: self.default_burst as f64,
                refill_rate: self.default_rate as f64 / 60.0, // per second
                last_refill: Instant::now(),
            }
        });

        // Refill tokens
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * bucket.refill_rate).min(bucket.capacity);
        bucket.last_refill = now;

        // Check if we have enough tokens
        if bucket.tokens >= tokens {
            bucket.tokens -= tokens;
            Ok(())
        } else {
            // Calculate wait time
            let needed = tokens - bucket.tokens;
            let wait_seconds = needed / bucket.refill_rate;
            Err(RateLimitError::InsufficientTokens {
                available: bucket.tokens,
                requested: tokens,
                wait_time: Duration::from_secs_f64(wait_seconds),
            })
        }
    }

    pub async fn acquire_or_wait(&self, domain: &str, tokens: f64) {
        loop {
            match self.acquire(domain, tokens).await {
                Ok(()) => break,
                Err(RateLimitError::InsufficientTokens { wait_time, .. }) => {
                    sleep(wait_time + Duration::from_millis(10)).await;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum RateLimitError {
    InsufficientTokens {
        available: f64,
        requested: f64,
        wait_time: Duration,
    },
}

/// Distributed rate limiter for multi-instance deployments
pub struct DistributedRateLimiter {
    redis_client: Option<redis::Client>,
    local_limiter: RateLimiter,
}

impl DistributedRateLimiter {
    pub fn new(redis_url: Option<&str>, default_rate: u32) -> Self {
        let redis_client = redis_url.and_then(|url| redis::Client::open(url).ok());
        
        Self {
            redis_client,
            local_limiter: RateLimiter::new(default_rate),
        }
    }

    pub async fn wait_if_needed(&self, domain: &str) {
        if let Some(client) = &self.redis_client {
            // Try Redis-based rate limiting
            if let Ok(mut con) = client.get_connection() {
                let key = format!("rate_limit:{}", domain);
                let window = 60; // seconds
                
                // Use Redis INCR with TTL
                let pipeline = redis::pipe()
                    .atomic()
                    .incr(&key, 1)
                    .expire(&key, window)
                    .query::<Vec<i32>>(&mut con);
                
                if let Ok(results) = pipeline {
                    if let Some(&count) = results.first() {
                        if count > self.local_limiter.default_rate as i32 {
                            let wait_time = Duration::from_secs(1);
                            sleep(wait_time).await;
                        }
                    }
                }
                return;
            }
        }
        
        // Fallback to local rate limiting
        self.local_limiter.wait_if_needed(domain).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_rate_limiting() {
        let limiter = RateLimiter::new(10); // 10 requests per minute
        let domain = "example.com";

        // Should allow initial requests
        for _ in 0..10 {
            limiter.wait_if_needed(domain).await;
        }

        // Check current rate
        let rate = limiter.get_current_rate(domain).await.unwrap();
        assert_eq!(rate, 10);
    }

    #[tokio::test]
    async fn test_burst_rate_limiting() {
        let limiter = BurstRateLimiter::new(60, 10); // 60/min, burst of 10
        let domain = "example.com";

        // Should allow burst
        for _ in 0..10 {
            assert!(limiter.acquire(domain, 1.0).await.is_ok());
        }

        // Next request should fail
        assert!(limiter.acquire(domain, 1.0).await.is_err());
    }
}
