use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use reqwest::{Client, Url};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::bundle::{FileSummary, GetOutcome, PutOutcome};
use crate::config::Config;
use crate::error::{bail, Context, Result};
use crate::http::{
    AddPeerRequest, FetchRequest, FileDetailsResponse, PutFileRequest, StatusResponse,
    TicketResponse,
};
use crate::metadata::PeerRecord;
use crate::metrics::NodeHealth;

#[derive(Clone)]
pub struct ApiClient {
    base: Url,
    http: Client,
}

impl ApiClient {
    pub fn from_config(config: &Config, api: Option<SocketAddr>) -> Result<Self> {
        let addr = resolve_api_addr(config, api)?;
        Self::new(addr)
    }

    pub fn new(addr: SocketAddr) -> Result<Self> {
        let base = Url::parse(&format!("http://{addr}"))
            .with_context(|| format!("invalid API address {addr}"))?;
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { base, http })
    }

    pub async fn health(&self) -> Result<NodeHealth> {
        self.get_json(self.url(&["health"])?).await
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get_json(self.url(&["status"])?).await
    }

    pub async fn list_files(&self) -> Result<Vec<FileSummary>> {
        self.get_json(self.url(&["files"])?).await
    }

    pub async fn file_details(&self, file_id: &str) -> Result<FileDetailsResponse> {
        self.get_json(self.url(&["files", file_id])?).await
    }

    pub async fn ticket(&self, file_id: &str) -> Result<TicketResponse> {
        self.get_json(self.url(&["files", file_id, "ticket"])?)
            .await
    }

    pub async fn list_peers(&self) -> Result<Vec<PeerRecord>> {
        self.get_json(self.url(&["peers"])?).await
    }

    pub async fn put_file(&self, path: &Path) -> Result<PutOutcome> {
        let path = std::path::absolute(path)
            .with_context(|| format!("failed to resolve {}", path.display()))?;
        self.post_json(self.url(&["files"])?, &PutFileRequest { path })
            .await
    }

    pub async fn fetch_file(&self, target: &str, output: &Path) -> Result<GetOutcome> {
        let output = std::path::absolute(output)
            .with_context(|| format!("failed to resolve {}", output.display()))?;
        self.post_json(
            self.url(&["fetch"])?,
            &FetchRequest {
                target: target.to_string(),
                output,
            },
        )
        .await
    }

    pub async fn add_peer(&self, endpoint: iroh::EndpointAddr) -> Result<PeerRecord> {
        self.post_json(self.url(&["peers"])?, &AddPeerRequest { endpoint })
            .await
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        let mut path = match url.path_segments_mut() {
            Ok(path) => path,
            Err(_) => bail!("API base URL cannot be used as a base"),
        };
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }

    async fn get_json<T: DeserializeOwned>(&self, url: Url) -> Result<T> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to reach node at {}", self.base))?;
        decode(response).await
    }

    async fn post_json<T: DeserializeOwned, B: Serialize>(&self, url: Url, body: &B) -> Result<T> {
        let response = self
            .http
            .post(url)
            .json(body)
            .send()
            .await
            .with_context(|| format!("failed to reach node at {}", self.base))?;
        decode(response).await
    }
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let message = serde_json::from_str::<ErrorResponse>(&body)
            .map(|response| response.error)
            .unwrap_or(body);
        bail!("node returned {status}: {message}");
    }

    response
        .json()
        .await
        .context("failed to decode node response")
}

#[derive(serde::Deserialize)]
struct ErrorResponse {
    error: String,
}

pub fn resolve_api_addr(config: &Config, api: Option<SocketAddr>) -> Result<SocketAddr> {
    if let Some(addr) = api {
        return Ok(addr);
    }

    let path = config.api_path();
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "no running node found for {}; start `p2p node --data-dir {}`",
                config.data_dir().display(),
                config.data_dir().display()
            )
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read API address {}", path.display()))
        }
    };

    contents
        .trim()
        .parse()
        .with_context(|| format!("invalid API address in {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn reads_api_address_from_data_dir() {
        let dir = TempDir::new().unwrap();
        let config = Config::new(dir.path());
        std::fs::write(config.api_path(), "127.0.0.1:7001\n").unwrap();

        let addr = resolve_api_addr(&config, None).unwrap();
        assert_eq!(addr, "127.0.0.1:7001".parse().unwrap());
    }

    #[test]
    fn explicit_api_address_wins() {
        let dir = TempDir::new().unwrap();
        let config = Config::new(dir.path());
        std::fs::write(config.api_path(), "127.0.0.1:7001\n").unwrap();

        let override_addr = "127.0.0.1:9000".parse().unwrap();
        let addr = resolve_api_addr(&config, Some(override_addr)).unwrap();
        assert_eq!(addr, override_addr);
    }

    #[test]
    fn missing_api_file_has_actionable_error() {
        let dir = TempDir::new().unwrap();
        let config = Config::new(dir.path());
        let error = resolve_api_addr(&config, None).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("p2p node --data-dir"));
    }
}
