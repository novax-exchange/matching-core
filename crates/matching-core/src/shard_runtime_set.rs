use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::matching_runtime::{
    MatchingRuntimeRunOnceReport, MatchingRuntimeRunReport, MatchingRuntimeShardRunOnceReport,
    MatchingRuntimeShardRunReport, MatchingRuntimeShardStatus, MatchingRuntimeSymbolStatus,
};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeShardId};
use crate::runtime_topology::RuntimeTopologyError;
use crate::shard_execution_core::{ShardExecutionCoreError, SymbolRuntimeStatus};
use crate::shard_runtime::{
    ShardRuntime, ShardRuntimeError, ShardRuntimeRunLimit, ShardRuntimeRunOnceLimits,
    ShardRuntimeRunOnceReport, ShardRuntimeRunReport,
};
use crate::types::Symbol;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

pub trait InputHandoffWriter {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWritePlan>, ShardRuntimeSetError>;
    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeSetError>;
    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError>;
    fn can_write_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), ShardRuntimeSetError>;
}

pub trait ShardRuntimeSet: InputHandoffWriter {
    fn shard_count(&self) -> usize;
    fn shard_ids(&self) -> Vec<RuntimeShardId>;
    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]>;
    fn shard_statuses(&self) -> Result<Vec<MatchingRuntimeShardStatus>, ShardRuntimeSetError>;
    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, ShardRuntimeSetError>;
    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, ShardRuntimeSetError>;
    fn shutdown(&mut self) -> Result<ShardRuntimeSetShutdownReport, ShardRuntimeSetError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardRuntimeSetError {
    ShardRuntime(ShardRuntimeError),
    ShardRuntimeUnavailable(RuntimeShardId),
    ShardRuntimeWorkerRequestFailed(RuntimeShardId),
    ShardRuntimeWorkerResponseFailed(RuntimeShardId),
    ShardRuntimeWorkerUnexpectedRequest {
        shard_id: RuntimeShardId,
        expected: &'static str,
    },
    ShardRuntimeWorkerUnexpectedResponse {
        shard_id: RuntimeShardId,
        expected: &'static str,
    },
    ShardRuntimeWorkerThreadPanicked(RuntimeShardId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputHandoffWritePlan {
    WriteInputs {
        shard_id: RuntimeShardId,
        entries: Vec<JournalInputEntry>,
    },
}

pub struct ThreadPerShardRuntimeSet {
    worker_handles: Vec<ShardRuntimeWorkerHandle>,
}

pub type ShardRuntimeOutputWriter = Box<dyn JournalOutputAppender + Send>;

enum ShardRuntimeWorkerHandle {
    Inline(InlineShardRuntimeWorkerHandle),
    Threaded(ThreadedShardRuntimeWorkerHandle),
}

struct InlineShardRuntimeWorkerHandle {
    worker: ShardRuntimeWorker,
}

struct ThreadedShardRuntimeWorkerHandle {
    shard_id: RuntimeShardId,
    symbols: Vec<Symbol>,
    request_sender: Sender<ShardRuntimeWorkerRequest>,
    response_receiver: Receiver<ShardRuntimeWorkerResponse>,
    join_handle: Option<JoinHandle<()>>,
}

struct ShardRuntimeWorker {
    runtime: ShardRuntime,
    owned_output: Option<ShardRuntimeWorkerOwnedOutput>,
}

struct ShardRuntimeWorkerOwnedOutput {
    journal_client: OutputJournalClient,
    output: ShardRuntimeOutputWriter,
}

struct ShardRuntimeWorkerRequest {
    shard_id: RuntimeShardId,
    payload: ShardRuntimeWorkerRequestPayload,
}

struct ShardRuntimeWorkerRunContext<'a> {
    journal_client: &'a mut OutputJournalClient,
    output: &'a mut dyn JournalOutputAppender,
}

enum ShardRuntimeWorkerRequestPayload {
    WriteInputs(Vec<JournalInputEntry>),
    RunOnce(ShardRuntimeRunOnceLimits),
    RunLimited {
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    },
    Status,
    Shutdown,
}

struct ShardRuntimeWorkerResponse {
    shard_id: RuntimeShardId,
    payload: ShardRuntimeWorkerResponsePayload,
}

enum ShardRuntimeWorkerResponsePayload {
    WriteInputs(Result<usize, ShardRuntimeSetError>),
    RunOnce(Result<ShardRuntimeRunOnceReport, ShardRuntimeSetError>),
    RunLimited(Result<ShardRuntimeRunReport, ShardRuntimeSetError>),
    Status(Result<Vec<MatchingRuntimeSymbolStatus>, ShardRuntimeSetError>),
    Shutdown,
}

impl ShardRuntimeWorkerHandle {
    fn new(runtime: ShardRuntime) -> Self {
        Self::Inline(InlineShardRuntimeWorkerHandle {
            worker: ShardRuntimeWorker::new(runtime),
        })
    }

    fn new_with_owned_output(
        runtime: ShardRuntime,
        journal_client: OutputJournalClient,
        output: ShardRuntimeOutputWriter,
    ) -> Self {
        let shard_id = runtime.shard_id();
        let symbols = runtime.symbols().to_vec();
        let worker = ShardRuntimeWorker::new_with_owned_output(runtime, journal_client, output);
        let (request_sender, request_receiver) = mpsc::channel();
        let (response_sender, response_receiver) = mpsc::channel();
        let join_handle = thread::spawn(move || {
            run_shard_runtime_worker_loop(worker, request_receiver, response_sender);
        });

        Self::Threaded(ThreadedShardRuntimeWorkerHandle {
            shard_id,
            symbols,
            request_sender,
            response_receiver,
            join_handle: Some(join_handle),
        })
    }

    fn shard_id(&self) -> RuntimeShardId {
        match self {
            Self::Inline(handle) => handle.worker.shard_id(),
            Self::Threaded(handle) => handle.shard_id,
        }
    }

    fn symbols(&self) -> &[Symbol] {
        match self {
            Self::Inline(handle) => handle.worker.symbols(),
            Self::Threaded(handle) => handle.symbols.as_slice(),
        }
    }

    fn has_symbol(&self, symbol: &Symbol) -> bool {
        self.symbols().contains(symbol)
    }

    fn available_input_capacity(&self, symbol: &Symbol) -> Result<usize, ShardRuntimeSetError> {
        match self {
            Self::Inline(handle) => handle.worker.available_input_capacity(symbol),
            Self::Threaded(_) => {
                let response = self.dispatch_status_request(ShardRuntimeWorkerRequest {
                    shard_id: self.shard_id(),
                    payload: ShardRuntimeWorkerRequestPayload::Status,
                })?;
                let status = response
                    .into_shard_status()?
                    .symbol_statuses
                    .into_iter()
                    .find(|status| status.symbol == *symbol)
                    .ok_or(ShardRuntimeSetError::ShardRuntime(
                        ShardRuntimeError::MissingHandoff(symbol.clone()),
                    ))?;

                Ok(status
                    .pending_input_capacity
                    .saturating_sub(status.pending_input_len))
            }
        }
    }

    fn dispatch_status_request(
        &self,
        request: ShardRuntimeWorkerRequest,
    ) -> Result<ShardRuntimeWorkerResponse, ShardRuntimeSetError> {
        match self {
            Self::Inline(handle) => Ok(handle.worker.handle_status_request(request)),
            Self::Threaded(_) => self.dispatch_threaded_request(request),
        }
    }

    fn dispatch_request(
        &mut self,
        request: ShardRuntimeWorkerRequest,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
    ) -> Result<ShardRuntimeWorkerResponse, ShardRuntimeSetError> {
        match self {
            Self::Inline(handle) => Ok(handle.worker.handle_request(request, run_context)),
            Self::Threaded(_) => self.dispatch_threaded_request(request),
        }
    }

    fn send_threaded_request(
        &self,
        request: ShardRuntimeWorkerRequest,
    ) -> Result<(), ShardRuntimeSetError> {
        let shard_id = request.shard_id;

        match self {
            Self::Threaded(handle) => handle
                .request_sender
                .send(request)
                .map_err(|_| ShardRuntimeSetError::ShardRuntimeWorkerRequestFailed(shard_id)),
            Self::Inline(_) => Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedRequest {
                shard_id,
                expected: "threaded worker request",
            }),
        }
    }

    fn receive_threaded_response(
        &self,
    ) -> Result<ShardRuntimeWorkerResponse, ShardRuntimeSetError> {
        match self {
            Self::Threaded(handle) => handle.response_receiver.recv().map_err(|_| {
                ShardRuntimeSetError::ShardRuntimeWorkerResponseFailed(handle.shard_id)
            }),
            Self::Inline(handle) => {
                Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                    shard_id: handle.worker.shard_id(),
                    expected: "threaded worker response",
                })
            }
        }
    }

    fn dispatch_threaded_request(
        &self,
        request: ShardRuntimeWorkerRequest,
    ) -> Result<ShardRuntimeWorkerResponse, ShardRuntimeSetError> {
        let shard_id = request.shard_id;
        self.send_threaded_request(request)?;
        self.receive_threaded_response()
            .map_err(|_| ShardRuntimeSetError::ShardRuntimeWorkerResponseFailed(shard_id))
    }

    fn is_threaded(&self) -> bool {
        matches!(self, Self::Threaded(_))
    }

    fn join_threaded_worker(&mut self) -> Result<(), ShardRuntimeSetError> {
        match self {
            Self::Threaded(handle) => {
                if let Some(join_handle) = handle.join_handle.take() {
                    join_handle.join().map_err(|_| {
                        ShardRuntimeSetError::ShardRuntimeWorkerThreadPanicked(handle.shard_id)
                    })?;
                }

                Ok(())
            }
            Self::Inline(_) => Ok(()),
        }
    }
}

fn run_shard_runtime_worker_loop(
    mut worker: ShardRuntimeWorker,
    request_receiver: Receiver<ShardRuntimeWorkerRequest>,
    response_sender: Sender<ShardRuntimeWorkerResponse>,
) {
    while let Ok(request) = request_receiver.recv() {
        let should_shutdown =
            matches!(&request.payload, ShardRuntimeWorkerRequestPayload::Shutdown);
        let response = worker.handle_request(request, None);

        if response_sender.send(response).is_err() {
            break;
        }

        if should_shutdown {
            break;
        }
    }
}

impl ShardRuntimeWorkerResponse {
    fn into_write_inputs_result(self) -> Result<usize, ShardRuntimeSetError> {
        match self.payload {
            ShardRuntimeWorkerResponsePayload::WriteInputs(result) => result,
            _ => Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: self.shard_id,
                expected: "write-input response",
            }),
        }
    }

    fn into_shard_status(self) -> Result<MatchingRuntimeShardStatus, ShardRuntimeSetError> {
        match self.payload {
            ShardRuntimeWorkerResponsePayload::Status(result) => Ok(MatchingRuntimeShardStatus {
                shard_id: self.shard_id,
                symbol_statuses: result?,
            }),
            _ => Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: self.shard_id,
                expected: "status response",
            }),
        }
    }

    fn into_shard_run_once_report(
        self,
    ) -> Result<MatchingRuntimeShardRunOnceReport, ShardRuntimeSetError> {
        match self.payload {
            ShardRuntimeWorkerResponsePayload::RunOnce(result) => {
                Ok(MatchingRuntimeShardRunOnceReport {
                    shard_id: self.shard_id,
                    run_once_report: result?,
                })
            }
            _ => Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: self.shard_id,
                expected: "run-once response",
            }),
        }
    }

    fn into_shard_run_report(self) -> Result<MatchingRuntimeShardRunReport, ShardRuntimeSetError> {
        match self.payload {
            ShardRuntimeWorkerResponsePayload::RunLimited(result) => {
                Ok(MatchingRuntimeShardRunReport {
                    shard_id: self.shard_id,
                    run_report: result?,
                })
            }
            _ => Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: self.shard_id,
                expected: "run-limited response",
            }),
        }
    }

    fn into_shutdown_shard_id(self) -> Result<RuntimeShardId, ShardRuntimeSetError> {
        match self.payload {
            ShardRuntimeWorkerResponsePayload::Shutdown => Ok(self.shard_id),
            _ => Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: self.shard_id,
                expected: "shutdown response",
            }),
        }
    }
}

impl ShardRuntimeWorker {
    fn new(runtime: ShardRuntime) -> Self {
        Self {
            runtime,
            owned_output: None,
        }
    }

    fn new_with_owned_output(
        runtime: ShardRuntime,
        journal_client: OutputJournalClient,
        output: ShardRuntimeOutputWriter,
    ) -> Self {
        Self {
            runtime,
            owned_output: Some(ShardRuntimeWorkerOwnedOutput {
                journal_client,
                output,
            }),
        }
    }

    fn shard_id(&self) -> RuntimeShardId {
        self.runtime.shard_id()
    }

    fn symbols(&self) -> &[Symbol] {
        self.runtime.symbols()
    }

    fn available_input_capacity(&self, symbol: &Symbol) -> Result<usize, ShardRuntimeSetError> {
        let pending_input_status = self.runtime.pending_input_status(symbol).ok_or_else(|| {
            ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(symbol.clone()))
        })?;

        Ok(pending_input_status
            .capacity
            .saturating_sub(pending_input_status.len))
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError> {
        self.runtime
            .enqueue_inputs(entries)
            .map_err(ShardRuntimeSetError::from)
    }

    fn symbol_statuses(&self) -> Result<Vec<MatchingRuntimeSymbolStatus>, ShardRuntimeSetError> {
        let mut symbol_statuses = Vec::new();

        for symbol in self.runtime.symbols() {
            let pending_input_status =
                self.runtime.pending_input_status(symbol).ok_or_else(|| {
                    ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                        symbol.clone(),
                    ))
                })?;

            let runtime_status =
                self.runtime
                    .symbol_status(symbol)
                    .ok_or(ShardRuntimeSetError::ShardRuntime(
                        ShardRuntimeError::ShardExecutionCore(
                            ShardExecutionCoreError::UnknownSymbol,
                        ),
                    ))?;

            symbol_statuses.push(symbol_status_from_runtime_status(
                symbol.clone(),
                pending_input_status.len,
                pending_input_status.capacity,
                pending_input_status.full,
                runtime_status,
            ));
        }

        Ok(symbol_statuses)
    }

    fn run_once_with_context(
        &mut self,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<ShardRuntimeRunOnceReport, ShardRuntimeSetError> {
        if let Some(owned_output) = self.owned_output.as_mut() {
            return self
                .runtime
                .run_once(
                    &mut owned_output.journal_client,
                    owned_output.output.as_mut(),
                    limits,
                )
                .map_err(ShardRuntimeSetError::from);
        }

        let Some(ShardRuntimeWorkerRunContext {
            journal_client,
            output,
        }) = run_context
        else {
            return Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedRequest {
                shard_id: self.shard_id(),
                expected: "run-once context",
            });
        };

        self.runtime
            .run_once(journal_client, output, limits)
            .map_err(ShardRuntimeSetError::from)
    }

    fn run_limited_with_context(
        &mut self,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<ShardRuntimeRunReport, ShardRuntimeSetError> {
        if let Some(owned_output) = self.owned_output.as_mut() {
            return self
                .runtime
                .run_limited(
                    &mut owned_output.journal_client,
                    owned_output.output.as_mut(),
                    limits,
                    limit,
                )
                .map_err(ShardRuntimeSetError::from);
        }

        let Some(ShardRuntimeWorkerRunContext {
            journal_client,
            output,
        }) = run_context
        else {
            return Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedRequest {
                shard_id: self.shard_id(),
                expected: "run-limited context",
            });
        };

        self.runtime
            .run_limited(journal_client, output, limits, limit)
            .map_err(ShardRuntimeSetError::from)
    }

    fn shutdown(&mut self) -> RuntimeShardId {
        self.shard_id()
    }

    fn handle_write_inputs_command(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> ShardRuntimeWorkerResponse {
        ShardRuntimeWorkerResponse {
            shard_id: self.shard_id(),
            payload: ShardRuntimeWorkerResponsePayload::WriteInputs(self.write_inputs(entries)),
        }
    }

    fn handle_run_once_command(
        &mut self,
        limits: ShardRuntimeRunOnceLimits,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
    ) -> ShardRuntimeWorkerResponse {
        ShardRuntimeWorkerResponse {
            shard_id: self.shard_id(),
            payload: ShardRuntimeWorkerResponsePayload::RunOnce(
                self.run_once_with_context(run_context, limits),
            ),
        }
    }

    fn handle_run_limited_command(
        &mut self,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
    ) -> ShardRuntimeWorkerResponse {
        ShardRuntimeWorkerResponse {
            shard_id: self.shard_id(),
            payload: ShardRuntimeWorkerResponsePayload::RunLimited(self.run_limited_with_context(
                run_context,
                limits,
                limit,
            )),
        }
    }

    fn handle_status_command(&self) -> ShardRuntimeWorkerResponse {
        ShardRuntimeWorkerResponse {
            shard_id: self.shard_id(),
            payload: ShardRuntimeWorkerResponsePayload::Status(self.symbol_statuses()),
        }
    }

    fn handle_shutdown_command(&mut self) -> ShardRuntimeWorkerResponse {
        ShardRuntimeWorkerResponse {
            shard_id: self.shutdown(),
            payload: ShardRuntimeWorkerResponsePayload::Shutdown,
        }
    }

    fn handle_status_request(
        &self,
        request: ShardRuntimeWorkerRequest,
    ) -> ShardRuntimeWorkerResponse {
        debug_assert_eq!(request.shard_id, self.shard_id());

        match request.payload {
            ShardRuntimeWorkerRequestPayload::Status => self.handle_status_command(),
            _ => ShardRuntimeWorkerResponse {
                shard_id: self.shard_id(),
                payload: ShardRuntimeWorkerResponsePayload::Status(Err(
                    ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedRequest {
                        shard_id: request.shard_id,
                        expected: "status request",
                    },
                )),
            },
        }
    }

    fn handle_request(
        &mut self,
        request: ShardRuntimeWorkerRequest,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
    ) -> ShardRuntimeWorkerResponse {
        debug_assert_eq!(request.shard_id, self.shard_id());

        match request.payload {
            ShardRuntimeWorkerRequestPayload::WriteInputs(entries) => {
                self.handle_write_inputs_command(entries)
            }
            ShardRuntimeWorkerRequestPayload::RunOnce(limits) => {
                self.handle_run_once_command(limits, run_context)
            }
            ShardRuntimeWorkerRequestPayload::RunLimited { limits, limit } => {
                self.handle_run_limited_command(limits, limit, run_context)
            }
            ShardRuntimeWorkerRequestPayload::Status => self.handle_status_command(),
            ShardRuntimeWorkerRequestPayload::Shutdown => self.handle_shutdown_command(),
        }
    }
}

impl ThreadPerShardRuntimeSet {
    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeTopologyError> {
        let worker_runtimes = ShardRuntime::from_symbols_with_config(symbols, config)?;
        let worker_handles = worker_runtimes
            .into_iter()
            .map(ShardRuntimeWorkerHandle::new)
            .collect();

        Ok(Self { worker_handles })
    }

    pub fn from_symbols_with_config_and_output_factory<F>(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
        mut output_factory: F,
    ) -> Result<Self, RuntimeTopologyError>
    where
        F: FnMut(RuntimeShardId) -> ShardRuntimeOutputWriter,
    {
        let worker_runtimes = ShardRuntime::from_symbols_with_config(symbols, config)?;
        let worker_handles = worker_runtimes
            .into_iter()
            .map(|runtime| {
                let shard_id = runtime.shard_id();
                ShardRuntimeWorkerHandle::new_with_owned_output(
                    runtime,
                    OutputJournalClient::new(),
                    output_factory(shard_id),
                )
            })
            .collect();

        Ok(Self { worker_handles })
    }

    pub fn worker_count(&self) -> usize {
        self.worker_handles.len()
    }

    pub fn worker_symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.worker_handles
            .iter()
            .find(|worker| worker.shard_id() == shard_id)
            .map(|worker| worker.symbols())
    }

    fn worker_index_for_symbol(&self, symbol: &Symbol) -> Option<usize> {
        self.worker_handles
            .iter()
            .position(|worker| worker.has_symbol(symbol))
    }

    fn validate_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<HashMap<Symbol, usize>, ShardRuntimeSetError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();
        let mut owner_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let worker_index = self.worker_index_for_symbol(&symbol).ok_or_else(|| {
                ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::UnregisteredHandoff(
                    symbol.clone(),
                ))
            })?;

            *requested_by_symbol.entry(symbol.clone()).or_insert(0) += 1;
            owner_by_symbol.insert(symbol, worker_index);
        }

        let mut requested_symbols: Vec<Symbol> = requested_by_symbol.keys().cloned().collect();
        requested_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in requested_symbols {
            let requested_count = requested_by_symbol
                .get(&symbol)
                .expect("requested symbol should have a requested count");
            let worker_index = owner_by_symbol
                .get(&symbol)
                .expect("requested symbol should have an owning worker");
            let available_capacity =
                self.worker_handles[*worker_index].available_input_capacity(&symbol)?;

            if available_capacity < *requested_count {
                return Err(ShardRuntimeSetError::ShardRuntime(
                    ShardRuntimeError::InputHandoffFull(symbol),
                ));
            }
        }

        Ok(owner_by_symbol)
    }

    fn plan_worker_write_requests(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<ShardRuntimeWorkerRequest>, ShardRuntimeSetError> {
        let write_plans = self.plan_writes(entries)?;

        Ok(write_plans
            .into_iter()
            .map(|plan| match plan {
                InputHandoffWritePlan::WriteInputs { shard_id, entries } => {
                    ShardRuntimeWorkerRequest {
                        shard_id,
                        payload: ShardRuntimeWorkerRequestPayload::WriteInputs(entries),
                    }
                }
            })
            .collect())
    }

    fn plan_worker_run_once_requests(
        &self,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Vec<ShardRuntimeWorkerRequest> {
        self.worker_handles
            .iter()
            .map(|worker| ShardRuntimeWorkerRequest {
                shard_id: worker.shard_id(),
                payload: ShardRuntimeWorkerRequestPayload::RunOnce(limits),
            })
            .collect()
    }

    fn plan_worker_run_limited_requests(
        &self,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Vec<ShardRuntimeWorkerRequest> {
        self.worker_handles
            .iter()
            .map(|worker| ShardRuntimeWorkerRequest {
                shard_id: worker.shard_id(),
                payload: ShardRuntimeWorkerRequestPayload::RunLimited { limits, limit },
            })
            .collect()
    }

    fn plan_worker_status_requests(&self) -> Vec<ShardRuntimeWorkerRequest> {
        self.worker_handles
            .iter()
            .map(|worker| ShardRuntimeWorkerRequest {
                shard_id: worker.shard_id(),
                payload: ShardRuntimeWorkerRequestPayload::Status,
            })
            .collect()
    }

    fn plan_worker_shutdown_requests(&self) -> Vec<ShardRuntimeWorkerRequest> {
        self.worker_handles
            .iter()
            .map(|worker| ShardRuntimeWorkerRequest {
                shard_id: worker.shard_id(),
                payload: ShardRuntimeWorkerRequestPayload::Shutdown,
            })
            .collect()
    }

    fn worker_for_shard(
        &self,
        shard_id: RuntimeShardId,
    ) -> Result<&ShardRuntimeWorkerHandle, ShardRuntimeSetError> {
        self.worker_handles
            .iter()
            .find(|worker| worker.shard_id() == shard_id)
            .ok_or(ShardRuntimeSetError::ShardRuntimeUnavailable(shard_id))
    }

    fn dispatch_worker_status_request(
        &self,
        request: ShardRuntimeWorkerRequest,
    ) -> Result<ShardRuntimeWorkerResponse, ShardRuntimeSetError> {
        self.worker_for_shard(request.shard_id)?
            .dispatch_status_request(request)
    }

    fn dispatch_worker_request(
        &mut self,
        request: ShardRuntimeWorkerRequest,
        run_context: Option<ShardRuntimeWorkerRunContext<'_>>,
    ) -> Result<ShardRuntimeWorkerResponse, ShardRuntimeSetError> {
        let shard_id = request.shard_id;

        self.worker_mut_for_shard(shard_id)?
            .dispatch_request(request, run_context)
    }

    fn dispatch_threaded_worker_requests(
        &self,
        requests: Vec<ShardRuntimeWorkerRequest>,
    ) -> Result<Vec<ShardRuntimeWorkerResponse>, ShardRuntimeSetError> {
        let shard_ids: Vec<RuntimeShardId> =
            requests.iter().map(|request| request.shard_id).collect();

        for request in requests {
            self.worker_for_shard(request.shard_id)?
                .send_threaded_request(request)?;
        }

        shard_ids
            .into_iter()
            .map(|shard_id| self.worker_for_shard(shard_id)?.receive_threaded_response())
            .collect()
    }

    fn all_workers_threaded(&self) -> bool {
        self.worker_handles
            .iter()
            .all(ShardRuntimeWorkerHandle::is_threaded)
    }

    fn worker_mut_for_shard(
        &mut self,
        shard_id: RuntimeShardId,
    ) -> Result<&mut ShardRuntimeWorkerHandle, ShardRuntimeSetError> {
        self.worker_handles
            .iter_mut()
            .find(|worker| worker.shard_id() == shard_id)
            .ok_or(ShardRuntimeSetError::ShardRuntimeUnavailable(shard_id))
    }
}

impl InputHandoffWriter for ThreadPerShardRuntimeSet {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWritePlan>, ShardRuntimeSetError> {
        let owner_by_symbol = self.validate_enqueue_inputs(entries)?;
        let mut entries_by_worker: Vec<Vec<JournalInputEntry>> =
            (0..self.worker_handles.len()).map(|_| Vec::new()).collect();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let worker_index = owner_by_symbol
                .get(&symbol)
                .expect("entry symbol should have an owning worker after validation");
            entries_by_worker[*worker_index].push(entry.clone());
        }

        Ok(self
            .worker_handles
            .iter()
            .zip(entries_by_worker)
            .filter(|(_, entries)| !entries.is_empty())
            .map(|(worker, entries)| InputHandoffWritePlan::WriteInputs {
                shard_id: worker.shard_id(),
                entries,
            })
            .collect())
    }

    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeSetError> {
        self.write_inputs(vec![entry]).map(|_| ())
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError> {
        let written_count = entries.len();
        let worker_requests = self.plan_worker_write_requests(&entries)?;

        if self.all_workers_threaded() {
            for response in self.dispatch_threaded_worker_requests(worker_requests)? {
                response.into_write_inputs_result()?;
            }
        } else {
            for request in worker_requests {
                self.dispatch_worker_request(request, None)?
                    .into_write_inputs_result()?;
            }
        }

        Ok(written_count)
    }

    fn can_write_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), ShardRuntimeSetError> {
        self.validate_enqueue_inputs(entries).map(|_| ())
    }
}

impl ShardRuntimeSet for ThreadPerShardRuntimeSet {
    fn shard_count(&self) -> usize {
        self.worker_handles.len()
    }

    fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.worker_handles
            .iter()
            .map(ShardRuntimeWorkerHandle::shard_id)
            .collect()
    }

    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.worker_symbols_for_shard(shard_id)
    }

    fn shard_statuses(&self) -> Result<Vec<MatchingRuntimeShardStatus>, ShardRuntimeSetError> {
        let worker_requests = self.plan_worker_status_requests();
        let mut shard_statuses = Vec::new();

        if self.all_workers_threaded() {
            for response in self.dispatch_threaded_worker_requests(worker_requests)? {
                shard_statuses.push(response.into_shard_status()?);
            }
        } else {
            for request in worker_requests {
                shard_statuses.push(
                    self.dispatch_worker_status_request(request)?
                        .into_shard_status()?,
                );
            }
        }

        Ok(shard_statuses)
    }

    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, ShardRuntimeSetError> {
        let worker_requests = self.plan_worker_run_once_requests(limits);

        let mut shard_reports = Vec::new();

        if self.all_workers_threaded() {
            for response in self.dispatch_threaded_worker_requests(worker_requests)? {
                shard_reports.push(response.into_shard_run_once_report()?);
            }
        } else {
            for request in worker_requests {
                shard_reports.push(
                    self.dispatch_worker_request(
                        request,
                        Some(ShardRuntimeWorkerRunContext {
                            journal_client,
                            output,
                        }),
                    )?
                    .into_shard_run_once_report()?,
                );
            }
        }

        Ok(MatchingRuntimeRunOnceReport { shard_reports })
    }

    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, ShardRuntimeSetError> {
        let worker_requests = self.plan_worker_run_limited_requests(limits, limit);

        let mut shard_reports = Vec::new();

        if self.all_workers_threaded() {
            for response in self.dispatch_threaded_worker_requests(worker_requests)? {
                shard_reports.push(response.into_shard_run_report()?);
            }
        } else {
            for request in worker_requests {
                shard_reports.push(
                    self.dispatch_worker_request(
                        request,
                        Some(ShardRuntimeWorkerRunContext {
                            journal_client,
                            output,
                        }),
                    )?
                    .into_shard_run_report()?,
                );
            }
        }

        Ok(MatchingRuntimeRunReport { shard_reports })
    }

    fn shutdown(&mut self) -> Result<ShardRuntimeSetShutdownReport, ShardRuntimeSetError> {
        let worker_requests = self.plan_worker_shutdown_requests();
        let mut shard_ids = Vec::new();

        if self.all_workers_threaded() {
            for response in self.dispatch_threaded_worker_requests(worker_requests)? {
                shard_ids.push(response.into_shutdown_shard_id()?);
            }

            for shard_id in shard_ids.clone() {
                self.worker_mut_for_shard(shard_id)?
                    .join_threaded_worker()?;
            }
        } else {
            for request in worker_requests {
                shard_ids.push(
                    self.dispatch_worker_request(request, None)?
                        .into_shutdown_shard_id()?,
                );
            }
        }

        Ok(ShardRuntimeSetShutdownReport { shard_ids })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeSetShutdownReport {
    pub shard_ids: Vec<RuntimeShardId>,
}

impl From<ShardRuntimeError> for ShardRuntimeSetError {
    fn from(error: ShardRuntimeError) -> Self {
        Self::ShardRuntime(error)
    }
}

pub struct InlineShardRuntimeSet {
    runtimes: Vec<ShardRuntime>,
}

impl InlineShardRuntimeSet {
    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeTopologyError> {
        Ok(Self {
            runtimes: ShardRuntime::from_symbols_with_config(symbols, config)?,
        })
    }

    fn runtime_index_for_symbol(&self, symbol: &Symbol) -> Option<usize> {
        self.runtimes
            .iter()
            .position(|runtime| runtime.symbols().contains(symbol))
    }

    fn validate_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<HashMap<Symbol, usize>, ShardRuntimeSetError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();
        let mut owner_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runtime_index = self.runtime_index_for_symbol(&symbol).ok_or_else(|| {
                ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::UnregisteredHandoff(
                    symbol.clone(),
                ))
            })?;

            *requested_by_symbol.entry(symbol.clone()).or_insert(0) += 1;
            owner_by_symbol.insert(symbol, runtime_index);
        }

        let mut requested_symbols: Vec<Symbol> = requested_by_symbol.keys().cloned().collect();
        requested_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in requested_symbols {
            let requested_count = requested_by_symbol
                .get(&symbol)
                .expect("requested symbol should have a requested count");
            let runtime_index = owner_by_symbol
                .get(&symbol)
                .expect("requested symbol should have an owning runtime");
            let pending_input_status = self.runtimes[*runtime_index]
                .pending_input_status(&symbol)
                .ok_or_else(|| {
                    ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                        symbol.clone(),
                    ))
                })?;
            let available_capacity = pending_input_status
                .capacity
                .saturating_sub(pending_input_status.len);

            if available_capacity < *requested_count {
                return Err(ShardRuntimeSetError::ShardRuntime(
                    ShardRuntimeError::InputHandoffFull(symbol),
                ));
            }
        }

        Ok(owner_by_symbol)
    }
}

impl InputHandoffWriter for InlineShardRuntimeSet {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWritePlan>, ShardRuntimeSetError> {
        let owner_by_symbol = self.validate_enqueue_inputs(entries)?;
        let mut entries_by_runtime: Vec<Vec<JournalInputEntry>> =
            (0..self.runtimes.len()).map(|_| Vec::new()).collect();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runtime_index = owner_by_symbol
                .get(&symbol)
                .expect("entry symbol should have an owning runtime after validation");
            entries_by_runtime[*runtime_index].push(entry.clone());
        }

        Ok(self
            .runtimes
            .iter()
            .zip(entries_by_runtime)
            .filter(|(_, entries)| !entries.is_empty())
            .map(|(runtime, entries)| InputHandoffWritePlan::WriteInputs {
                shard_id: runtime.shard_id(),
                entries,
            })
            .collect())
    }

    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeSetError> {
        let symbol = entry.command.symbol().clone();
        let runtime_index =
            self.runtime_index_for_symbol(&symbol)
                .ok_or(ShardRuntimeSetError::ShardRuntime(
                    ShardRuntimeError::UnregisteredHandoff(symbol),
                ))?;

        self.runtimes[runtime_index]
            .enqueue_input(entry)
            .map_err(ShardRuntimeSetError::from)
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError> {
        let written_count = entries.len();
        let plans = self.plan_writes(&entries)?;

        for plan in plans {
            match plan {
                InputHandoffWritePlan::WriteInputs { shard_id, entries } => {
                    let runtime = self
                        .runtimes
                        .iter_mut()
                        .find(|runtime| runtime.shard_id() == shard_id)
                        .ok_or(ShardRuntimeSetError::ShardRuntimeUnavailable(shard_id))?;
                    runtime
                        .enqueue_inputs(entries)
                        .map_err(ShardRuntimeSetError::from)?;
                }
            }
        }

        Ok(written_count)
    }

    fn can_write_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), ShardRuntimeSetError> {
        self.validate_enqueue_inputs(entries).map(|_| ())
    }
}

impl ShardRuntimeSet for InlineShardRuntimeSet {
    fn shard_count(&self) -> usize {
        self.runtimes.len()
    }

    fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.runtimes.iter().map(ShardRuntime::shard_id).collect()
    }

    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.runtimes
            .iter()
            .find(|runtime| runtime.shard_id() == shard_id)
            .map(ShardRuntime::symbols)
    }

    fn shard_statuses(&self) -> Result<Vec<MatchingRuntimeShardStatus>, ShardRuntimeSetError> {
        let mut shard_statuses = Vec::new();

        for runtime in &self.runtimes {
            let mut symbol_statuses = Vec::new();

            for symbol in runtime.symbols() {
                let pending_input_status =
                    runtime.pending_input_status(symbol).ok_or_else(|| {
                        ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                            symbol.clone(),
                        ))
                    })?;
                let runtime_status =
                    runtime
                        .symbol_status(symbol)
                        .ok_or(ShardRuntimeSetError::ShardRuntime(
                            ShardRuntimeError::ShardExecutionCore(
                                ShardExecutionCoreError::UnknownSymbol,
                            ),
                        ))?;

                symbol_statuses.push(symbol_status_from_runtime_status(
                    symbol.clone(),
                    pending_input_status.len,
                    pending_input_status.capacity,
                    pending_input_status.full,
                    runtime_status,
                ));
            }

            shard_statuses.push(MatchingRuntimeShardStatus {
                shard_id: runtime.shard_id(),
                symbol_statuses,
            });
        }

        Ok(shard_statuses)
    }

    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, ShardRuntimeSetError> {
        let mut shard_reports = Vec::new();

        for runtime in &mut self.runtimes {
            let run_once_report: ShardRuntimeRunOnceReport = runtime
                .run_once(journal_client, output, limits)
                .map_err(ShardRuntimeSetError::from)?;
            shard_reports.push(MatchingRuntimeShardRunOnceReport {
                shard_id: runtime.shard_id(),
                run_once_report,
            });
        }

        Ok(MatchingRuntimeRunOnceReport { shard_reports })
    }

    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, ShardRuntimeSetError> {
        let mut shard_reports = Vec::new();

        for runtime in &mut self.runtimes {
            let run_report: ShardRuntimeRunReport = runtime
                .run_limited(journal_client, output, limits, limit)
                .map_err(ShardRuntimeSetError::from)?;
            shard_reports.push(MatchingRuntimeShardRunReport {
                shard_id: runtime.shard_id(),
                run_report,
            });
        }

        Ok(MatchingRuntimeRunReport { shard_reports })
    }

    fn shutdown(&mut self) -> Result<ShardRuntimeSetShutdownReport, ShardRuntimeSetError> {
        Ok(ShardRuntimeSetShutdownReport {
            shard_ids: self.shard_ids(),
        })
    }
}

fn symbol_status_from_runtime_status(
    symbol: Symbol,
    pending_input_len: usize,
    pending_input_capacity: usize,
    pending_input_full: bool,
    runtime_status: SymbolRuntimeStatus,
) -> MatchingRuntimeSymbolStatus {
    MatchingRuntimeSymbolStatus {
        symbol,
        pending_input_len,
        pending_input_capacity,
        pending_input_full,
        pending_output_len: runtime_status.pending_output_len,
        pending_output_capacity: runtime_status.pending_output_capacity,
        pending_output_full: runtime_status.pending_output_full,
        output_commit_blocked: runtime_status.output_commit_blockage.is_some(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_response_conversion_reports_unexpected_payload_instead_of_panicking() {
        let response = ShardRuntimeWorkerResponse {
            shard_id: RuntimeShardId(7),
            payload: ShardRuntimeWorkerResponsePayload::Shutdown,
        };

        assert_eq!(
            response.into_write_inputs_result(),
            Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: RuntimeShardId(7),
                expected: "write-input response",
            })
        );
    }

    #[test]
    fn shutdown_response_conversion_reports_unexpected_payload_instead_of_panicking() {
        let response = ShardRuntimeWorkerResponse {
            shard_id: RuntimeShardId(3),
            payload: ShardRuntimeWorkerResponsePayload::Status(Ok(Vec::new())),
        };

        assert_eq!(
            response.into_shutdown_shard_id(),
            Err(ShardRuntimeSetError::ShardRuntimeWorkerUnexpectedResponse {
                shard_id: RuntimeShardId(3),
                expected: "shutdown response",
            })
        );
    }
}
