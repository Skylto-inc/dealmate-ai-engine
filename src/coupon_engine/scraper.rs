//! High-performance web scraper with proxy support and error recovery

use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;
use rand::seq::SliceRandom;
use crate::coupon_engine::EngineConfig;

pub struct Scraper {
    config: EngineConfig,
    clients: Vec<Client>,
    user_agents: Vec<String>,
}

impl Scraper {
    pub fn new(config: EngineConfig) -> Self {
        let user_agents = vec![
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36".to_string(),
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36".to_string(),
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36".to_string(),
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0".to_string(),
        ];

        let mut clients = Vec::new();
        
        // Create clients with different configurations
        for _ in 0..5 {
            let mut client_builder = Client::builder()
                .timeout(Duration::from_secs(config.request_timeout_secs))
                .gzip(true)
                .deflate(true)
                .brotli(true);

            // Add headers
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8".parse().unwrap());
            headers.insert("Accept-Language", "en-US,en;q=0.9".parse().unwrap());
            headers.insert("Accept-Encoding", "gzip, deflate, br".parse().unwrap());
            headers.insert("DNT", "1".parse().unwrap());
            headers.insert("Connection", "keep-alive".parse().unwrap());
            headers.insert("Upgrade-Insecure-Requests", "1".parse().unwrap());
            
            client_builder = client_builder.default_headers(headers);
            
            if let Ok(client) = client_builder.build() {
                clients.push(client);
            }
        }

        // Ensure at least one client
        if clients.is_empty() {
            clients.push(Client::new());
        }

        Self {
            config,
            clients,
            user_agents,
        }
    }

    pub async fn fetch_content(&self, url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut last_error = None;
        
        for attempt in 0..self.config.retry_attempts {
            if attempt > 0 {
                // Exponential backoff
                sleep(Duration::from_millis(1000 * 2_u64.pow(attempt))).await;
            }

            // Select random client and user agent
            let client = self.clients.choose(&mut rand::thread_rng()).unwrap();
            let user_agent = if self.config.user_agent_rotation {
                self.user_agents.choose(&mut rand::thread_rng()).unwrap().clone()
            } else {
                self.user_agents[0].clone()
            };

            match self.fetch_with_client(client, url, &user_agent).await {
                Ok(content) => return Ok(content),
                Err(e) => {
                    last_error = Some(e);
                    eprintln!("Attempt {} failed for {}: {:?}", attempt + 1, url, last_error);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "All retry attempts failed".into()))
    }

    async fn fetch_with_client(
        &self,
        client: &Client,
        url: &str,
        user_agent: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let response = client
            .get(url)
            .header("User-Agent", user_agent)
            .send()
            .await?;

        // Check status
        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        // Read content
        let content = response.text().await?;
        
        // Basic validation
        if content.is_empty() {
            return Err("Empty response content".into());
        }

        Ok(content)
    }
}

/// Content type detection
pub fn detect_content_type(content: &str) -> ContentType {
    let trimmed = content.trim_start();
    
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        ContentType::Json
    } else if trimmed.starts_with('<') {
        ContentType::Html
    } else if trimmed.contains('\t') || trimmed.lines().all(|line| line.contains(',')) {
        ContentType::Csv
    } else {
        ContentType::Unknown
    }
}

#[derive(Debug, Clone)]
pub enum ContentType {
    Html,
    Json,
    Csv,
    Unknown,
}
