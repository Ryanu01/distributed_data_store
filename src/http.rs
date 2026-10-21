use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::bundle::{self, FileSummary, GetOutcome, PutOutcome};
use crate::manifest::Manifest;
use crate::metadata::{FileRecord, Metadata, PeerRecord};
use crate::metrics::{Metrics, NodeHealth};

#[derive(Clone)]
pub struct AppState {
    pub endpoint: Endpoint,
    pub store: FsStore,
    pub metadata: Arc<Metadata>,
    pub metrics: Metrics,
    pub api_addr: SocketAddr,
    pub started_at: Instant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub node_id: EndpointId,
    pub files: u64,
    pub chunks: u64,
    pub peers: u64,
    pub api_addr: SocketAddr,
    pub uptime_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutFileRequest {
    pub path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchFileRequest {
    pub output: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileDetailsResponse {
    pub file_id: Hash,
    pub record: Option<FileRecord>,
    pub manifest: Manifest,
    pub replicas: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TicketResponse {
    pub ticket: BlobTicket,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddPeerRequest {
    pub endpoint: EndpointAddr,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/files", get(list_files).post(put_file))
        .route("/files/{id}", get(file_details))
        .route("/files/{id}/fetch", post(fetch_file))
        .route("/files/{id}/ticket", get(file_ticket))
        .route("/peers", get(list_peers).post(add_peer))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<NodeHealth> {
    Json(state.metrics.snapshot())
}

async fn status(State(state): State<AppState>) -> ApiResult<Json<StatusResponse>> {
    let files = bundle::list_files(state.store.as_ref(), &state.metadata).await?;
    let chunks = files.iter().map(|file| file.chunk_count as u64).sum();
    let peers = state.metadata.list_peers()?.len() as u64;

    Ok(Json(StatusResponse {
        node_id: state.endpoint.id(),
        files: files.len() as u64,
        chunks,
        peers,
        api_addr: state.api_addr,
        uptime_secs: state.started_at.elapsed().as_secs(),
    }))
}

async fn list_files(State(state): State<AppState>) -> ApiResult<Json<Vec<FileSummary>>> {
    let files = bundle::list_files(state.store.as_ref(), &state.metadata).await?;
    Ok(Json(files))
}

async fn put_file(
    State(state): State<AppState>,
    Json(request): Json<PutFileRequest>,
) -> ApiResult<Json<PutOutcome>> {
    if !request.path.is_file() {
        return Err(ApiError::bad_request(format!(
            "not a readable file: {}",
            request.path.display()
        )));
    }

    let guard = state.metrics.begin_operation();
    let outcome = bundle::put_file(state.store.as_ref(), &request.path).await?;

    state.metadata.record_file(&FileRecord {
        file_id: outcome.file_id,
        file_name: outcome.file_name.clone(),
        file_size: outcome.file_size,
        file_hash: outcome.file_hash,
        chunk_count: outcome.chunk_count,
        created_at_unix: outcome.created_at_unix,
    })?;
    state
        .metadata
        .add_replica(outcome.file_id, &state.endpoint.id().to_string())?;

    guard.finish_success(outcome.file_size);
    Ok(Json(outcome))
}

async fn file_details(
    State(state): State<AppState>,
    UrlPath(id): UrlPath<String>,
) -> ApiResult<Json<FileDetailsResponse>> {
    let file_id = resolve_local_file(&state, &id).await?;
    let (manifest, _) = bundle::load_manifest(state.store.as_ref(), file_id)
        .await
        .map_err(ApiError::not_found)?;
    let record = state.metadata.get_file(file_id)?;
    let replicas = state.metadata.replicas(file_id)?;

    Ok(Json(FileDetailsResponse {
        file_id,
        record,
        manifest,
        replicas,
    }))
}

async fn fetch_file(
    State(state): State<AppState>,
    UrlPath(id): UrlPath<String>,
    Json(request): Json<FetchFileRequest>,
) -> ApiResult<Json<GetOutcome>> {
    let file_id = resolve_local_file(&state, &id).await?;
    let guard = state.metrics.begin_operation();
    let outcome = bundle::get_file(state.store.as_ref(), file_id, &request.output)
        .await
        .map_err(ApiError::not_found)?;
    guard.finish_success(outcome.file_size);
    Ok(Json(outcome))
}

async fn file_ticket(
    State(state): State<AppState>,
    UrlPath(id): UrlPath<String>,
) -> ApiResult<Json<TicketResponse>> {
    let file_id = resolve_local_file(&state, &id).await?;
    bundle::load_manifest(state.store.as_ref(), file_id)
        .await
        .map_err(ApiError::not_found)?;

    Ok(Json(TicketResponse {
        ticket: crate::retrieval::ticket(&state.endpoint, file_id),
    }))
}

async fn list_peers(State(state): State<AppState>) -> ApiResult<Json<Vec<PeerRecord>>> {
    Ok(Json(state.metadata.list_peers()?))
}

async fn add_peer(
    State(state): State<AppState>,
    Json(request): Json<AddPeerRequest>,
) -> ApiResult<(StatusCode, Json<PeerRecord>)> {
    let peer = PeerRecord {
        endpoint_id: request.endpoint.id,
        addr: request.endpoint,
        last_seen_unix: now_unix(),
        online: true,
    };
    state.metadata.upsert_peer(&peer)?;
    Ok((StatusCode::CREATED, Json(peer)))
}

async fn resolve_local_file(state: &AppState, input: &str) -> ApiResult<Hash> {
    if let Some(file_id) = state.metadata.resolve_name(input)? {
        return Ok(file_id);
    }

    bundle::resolve_file_id(state.store.as_ref(), input)
        .await
        .map_err(ApiError::not_found)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

type ApiResult<T> = std::result::Result<T, ApiError>;

pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(anyhow::Error),
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn not_found(error: impl Into<anyhow::Error>) -> Self {
        Self::NotFound(format!("{:#}", error.into()))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Internal(error) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}
