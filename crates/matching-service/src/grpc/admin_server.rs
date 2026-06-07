pub mod proto {
    tonic::include_proto!("matching");
}

use proto::matching_admin_service_server::MatchingAdminService;
use proto::{
    HealthCheckRequest, HealthCheckResponse, RuntimeStatusRequest, RuntimeStatusResponse,
    TriggerSnapshotRequest, TriggerSnapshotResponse,
};
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

pub struct RuntimeView {
    pub last_input_seq: u64,
    pub order_book_checksum: u64,
    pub state: String,
}

pub struct AdminServer {
    symbol: String,
    view: Arc<RwLock<RuntimeView>>,
}

impl AdminServer {
    pub fn new(symbol: String, view: Arc<RwLock<RuntimeView>>) -> Self {
        AdminServer { symbol, view }
    }
}

#[tonic::async_trait]
impl MatchingAdminService for AdminServer {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: "OK".into(),
        }))
    }

    async fn runtime_status(
        &self,
        request: Request<RuntimeStatusRequest>,
    ) -> Result<Response<RuntimeStatusResponse>, Status> {
        if request.into_inner().symbol != self.symbol {
            return Err(Status::not_found("symbol not found"));
        }

        let view = self
            .view
            .read()
            .map_err(|_| Status::internal("runtime view lock poisoned"))?;
        Ok(Response::new(RuntimeStatusResponse {
            symbol: self.symbol.clone(),
            last_input_seq: view.last_input_seq,
            order_book_checksum: view.order_book_checksum,
            state: view.state.clone(),
        }))
    }

    async fn trigger_snapshot(
        &self,
        _request: Request<TriggerSnapshotRequest>,
    ) -> Result<Response<TriggerSnapshotResponse>, Status> {
        Ok(Response::new(TriggerSnapshotResponse {
            success: false,
            message: "snapshot trigger must be executed by SymbolRuntime at a safe point".into(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::matching_admin_service_server::MatchingAdminService;
    use proto::{HealthCheckRequest, RuntimeStatusRequest, TriggerSnapshotRequest};
    use std::sync::{Arc, RwLock};
    use tonic::Request;

    #[tokio::test]
    async fn health_check_returns_ok() {
        let server = AdminServer::new(
            "BTCUSDT".into(),
            Arc::new(RwLock::new(RuntimeView {
                last_input_seq: 7,
                order_book_checksum: 42,
                state: "RUNNING".into(),
            })),
        );

        let response = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.status, "OK");
    }

    #[tokio::test]
    async fn runtime_status_reads_shared_view() {
        let server = AdminServer::new(
            "BTCUSDT".into(),
            Arc::new(RwLock::new(RuntimeView {
                last_input_seq: 7,
                order_book_checksum: 42,
                state: "RUNNING".into(),
            })),
        );

        let response = server
            .runtime_status(Request::new(RuntimeStatusRequest {
                symbol: "BTCUSDT".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.last_input_seq, 7);
        assert_eq!(response.order_book_checksum, 42);
        assert_eq!(response.state, "RUNNING");
    }

    #[tokio::test]
    async fn trigger_snapshot_is_explicitly_deferred_to_runtime_safe_point() {
        let server = AdminServer::new(
            "BTCUSDT".into(),
            Arc::new(RwLock::new(RuntimeView {
                last_input_seq: 7,
                order_book_checksum: 42,
                state: "RUNNING".into(),
            })),
        );

        let response = server
            .trigger_snapshot(Request::new(TriggerSnapshotRequest {
                symbol: "BTCUSDT".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!response.success);
        assert!(response.message.contains("safe point"));
    }
}
