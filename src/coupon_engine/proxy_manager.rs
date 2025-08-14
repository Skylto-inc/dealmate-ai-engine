//! Proxy management module for rotating proxies and handling failures

use reqwest::Proxy;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub proxy_type: ProxyType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProxyType {
    Http,
    Https,
    Socks5,
}

pub struct ProxyManager {
    proxies: Arc<Mutex<VecDeque<ProxyState>>>,
    failed_proxies: Arc<Mutex<Vec<FailedProxy>>>,
    config: ProxyManagerConfig,
}

struct ProxyState {
    config: ProxyConfig,
    last_used: Option<Instant>,
    success_count: u32,
    failure_count: u32,
}

struct FailedProxy {
    config: ProxyConfig,
    failed_at: Instant,
    _reason: String,
}

pub struct ProxyManagerConfig {
    pub rotation_interval: Duration,
    pub max_failures: u32,
    pub retry_after: Duration,
}

impl Default for ProxyManagerConfig {
    fn default() -> Self {
        Self {
            rotation_interval: Duration::from_secs(60),
            max_failures: 3,
            retry_after: Duration::from_secs(300),
        }
    }
}

impl ProxyManager {
    pub fn new() -> Self {
        Self::with_config(ProxyManagerConfig::default())
    }

    pub fn with_config(config: ProxyManagerConfig) -> Self {
        Self {
            proxies: Arc::new(Mutex::new(VecDeque::new())),
            failed_proxies: Arc::new(Mutex::new(Vec::new())),
            config,
        }
    }

    pub async fn add_proxy(&self, proxy_config: ProxyConfig) {
        let mut proxies = self.proxies.lock().await;
        proxies.push_back(ProxyState {
            config: proxy_config,
            last_used: None,
            success_count: 0,
            failure_count: 0,
        });
    }

    pub async fn add_proxies(&self, proxy_configs: Vec<ProxyConfig>) {
        let mut proxies = self.proxies.lock().await;
        for config in proxy_configs {
            proxies.push_back(ProxyState {
                config,
                last_used: None,
                success_count: 0,
                failure_count: 0,
            });
        }
    }

    pub async fn get_next_proxy(&self) -> Option<ProxyConfig> {
        // First, check if any failed proxies can be retried
        self.recover_failed_proxies().await;

        let mut proxies = self.proxies.lock().await;
        
        if proxies.is_empty() {
            return None;
        }

        // Rotate to find a proxy that hasn't been used recently
        let now = Instant::now();
        let mut rotations = 0;
        
        loop {
            if rotations >= proxies.len() {
                // All proxies have been used recently, use the oldest one
                break;
            }

            let front = proxies.front()?;
            
            let should_use = match front.last_used {
                None => true,
                Some(last_used) => now.duration_since(last_used) >= self.config.rotation_interval,
            };

            if should_use {
                let mut proxy_state = proxies.pop_front()?;
                proxy_state.last_used = Some(now);
                let config = proxy_state.config.clone();
                proxies.push_back(proxy_state);
                return Some(config);
            }

            // Rotate to next proxy
            let proxy = proxies.pop_front()?;
            proxies.push_back(proxy);
            rotations += 1;
        }

        // Use the least recently used proxy
        let mut proxy_state = proxies.pop_front()?;
        proxy_state.last_used = Some(now);
        let config = proxy_state.config.clone();
        proxies.push_back(proxy_state);
        
        Some(config)
    }

    pub async fn mark_success(&self, proxy_url: &str) {
        let mut proxies = self.proxies.lock().await;
        
        for proxy in proxies.iter_mut() {
            if proxy.config.url == proxy_url {
                proxy.success_count += 1;
                proxy.failure_count = 0; // Reset failure count on success
                break;
            }
        }
    }

    pub async fn mark_failure(&self, proxy_url: &str, reason: &str) {
        let mut proxies = self.proxies.lock().await;
        let mut failed_proxies = self.failed_proxies.lock().await;
        
        let mut index_to_remove = None;
        
        for (i, proxy) in proxies.iter_mut().enumerate() {
            if proxy.config.url == proxy_url {
                proxy.failure_count += 1;
                
                if proxy.failure_count >= self.config.max_failures {
                    index_to_remove = Some(i);
                }
                break;
            }
        }

        // Move to failed proxies if exceeded max failures
        if let Some(index) = index_to_remove {
            if let Some(proxy_state) = proxies.remove(index) {
                failed_proxies.push(FailedProxy {
                    config: proxy_state.config,
                    failed_at: Instant::now(),
                    _reason: reason.to_string(),
                });
            }
        }
    }

    async fn recover_failed_proxies(&self) {
        let mut failed_proxies = self.failed_proxies.lock().await;
        let mut proxies = self.proxies.lock().await;
        
        let now = Instant::now();
        let mut recovered = Vec::new();
        
        // Find proxies that can be retried
        failed_proxies.retain(|failed_proxy| {
            if now.duration_since(failed_proxy.failed_at) >= self.config.retry_after {
                recovered.push(failed_proxy.config.clone());
                false
            } else {
                true
            }
        });

        // Add recovered proxies back to rotation
        for config in recovered {
            proxies.push_back(ProxyState {
                config,
                last_used: None,
                success_count: 0,
                failure_count: 0,
            });
        }
    }

    pub async fn get_stats(&self) -> ProxyStats {
        let proxies = self.proxies.lock().await;
        let failed_proxies = self.failed_proxies.lock().await;
        
        let total_success: u32 = proxies.iter().map(|p| p.success_count).sum();
        let total_failures: u32 = proxies.iter().map(|p| p.failure_count).sum();
        
        ProxyStats {
            active_proxies: proxies.len(),
            failed_proxies: failed_proxies.len(),
            total_success,
            total_failures,
            success_rate: if total_success + total_failures > 0 {
                (total_success as f64 / (total_success + total_failures) as f64) * 100.0
            } else {
                0.0
            },
        }
    }

    pub async fn to_reqwest_proxy(&self, config: &ProxyConfig) -> Result<Proxy, Box<dyn std::error::Error>> {
        let proxy = match config.proxy_type {
            ProxyType::Http => Proxy::http(&config.url)?,
            ProxyType::Https => Proxy::https(&config.url)?,
            ProxyType::Socks5 => {
                // Reqwest doesn't directly support SOCKS5 in the same way
                // You might need to use a different approach or library
                return Err("SOCKS5 proxy not directly supported by reqwest".into());
            }
        };

        let proxy = if let (Some(username), Some(password)) = (&config.username, &config.password) {
            proxy.basic_auth(username, password)
        } else {
            proxy
        };

        Ok(proxy)
    }

    /// Load proxies from a file or external source
    pub async fn load_from_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = tokio::fs::read_to_string(path).await?;
        let proxy_configs: Vec<ProxyConfig> = serde_json::from_str(&content)?;
        self.add_proxies(proxy_configs).await;
        Ok(())
    }

    /// Load free proxies from public sources (for testing/development)
    pub async fn load_free_proxies(&self) -> Result<(), Box<dyn std::error::Error>> {
        // This is a placeholder - in production, you'd fetch from actual proxy sources
        let test_proxies = vec![
            ProxyConfig {
                url: "http://proxy1.example.com:8080".to_string(),
                username: None,
                password: None,
                proxy_type: ProxyType::Http,
            },
            ProxyConfig {
                url: "http://proxy2.example.com:8080".to_string(),
                username: None,
                password: None,
                proxy_type: ProxyType::Http,
            },
        ];
        
        self.add_proxies(test_proxies).await;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct ProxyStats {
    pub active_proxies: usize,
    pub failed_proxies: usize,
    pub total_success: u32,
    pub total_failures: u32,
    pub success_rate: f64,
}

/// Proxy validator to test proxy connectivity
pub struct ProxyValidator;

impl ProxyValidator {
    pub async fn validate(proxy_config: &ProxyConfig) -> bool {
        // Try to make a simple request through the proxy
        let client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(10));

        let proxy_manager = ProxyManager::new();
        
        let client = match proxy_manager.to_reqwest_proxy(proxy_config).await {
            Ok(proxy) => client_builder.proxy(proxy).build(),
            Err(_) => return false,
        };

        if let Ok(client) = client {
            // Test with a simple HTTP request
            match client.get("http://httpbin.org/ip").send().await {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            }
        } else {
            false
        }
    }

    pub async fn validate_batch(proxies: Vec<ProxyConfig>) -> Vec<(ProxyConfig, bool)> {
        let mut results = Vec::new();
        
        for proxy in proxies {
            let is_valid = Self::validate(&proxy).await;
            results.push((proxy, is_valid));
        }
        
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_proxy_rotation() {
        let manager = ProxyManager::new();
        
        // Add test proxies
        for i in 1..=3 {
            manager.add_proxy(ProxyConfig {
                url: format!("http://proxy{}.test.com:8080", i),
                username: None,
                password: None,
                proxy_type: ProxyType::Http,
            }).await;
        }

        // Get proxies in rotation
        let proxy1 = manager.get_next_proxy().await.unwrap();
        let proxy2 = manager.get_next_proxy().await.unwrap();
        
        assert_ne!(proxy1.url, proxy2.url);
    }

    #[tokio::test]
    async fn test_proxy_failure_handling() {
        let config = ProxyManagerConfig {
            rotation_interval: Duration::from_secs(1),
            max_failures: 2,
            retry_after: Duration::from_secs(5),
        };
        
        let manager = ProxyManager::with_config(config);
        
        let proxy_config = ProxyConfig {
            url: "http://test.proxy.com:8080".to_string(),
            username: None,
            password: None,
            proxy_type: ProxyType::Http,
        };
        
        manager.add_proxy(proxy_config.clone()).await;
        
        // Mark failures
        manager.mark_failure(&proxy_config.url, "Connection timeout").await;
        manager.mark_failure(&proxy_config.url, "Connection refused").await;
        
        // Proxy should be moved to failed list
        let stats = manager.get_stats().await;
        assert_eq!(stats.active_proxies, 0);
        assert_eq!(stats.failed_proxies, 1);
    }
}
