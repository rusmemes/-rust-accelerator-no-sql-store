use crate::common::CommunicationStreamEither;
use crate::conversions::manager_api::v1::WorkerEvent;
use crate::conversions::worker_api::v1::WorkerEvent as ClientApiWorkerEvent;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use tonic::Status;

type ClientApiWorkerIOStreamError = CommunicationStreamEither<Status, ClientApiWorkerEvent>;
type WorkerIOStreamError = CommunicationStreamEither<Status, WorkerEvent>;

#[async_trait]
pub(super) trait IOStreamExt<Event, Error> {
    async fn send(&self, event: Event) -> Result<(), Error>;
    fn is_closed(&self) -> bool;
}

pub(super) type WorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;

pub(super) type ClientApiWorkerIOStream = CommunicationStreamEither<
    Sender<Result<ClientApiWorkerEvent, Status>>,
    Sender<ClientApiWorkerEvent>,
>;

#[async_trait]
impl IOStreamExt<ClientApiWorkerEvent, ClientApiWorkerIOStreamError> for ClientApiWorkerIOStream {
    async fn send(&self, event: ClientApiWorkerEvent) -> Result<(), ClientApiWorkerIOStreamError> {
        match self {
            ClientApiWorkerIOStream::Input(sender) => {
                if let Err(e) = sender.send(Ok(event)).await {
                    let result = e.0;
                    if let Err(status) = result {
                        return Err(ClientApiWorkerIOStreamError::Input(status));
                    }
                }
            }
            ClientApiWorkerIOStream::Output(sender) => {
                if let Err(e) = sender.send(event).await {
                    return Err(ClientApiWorkerIOStreamError::Output(e.0));
                }
            }
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        match self {
            ClientApiWorkerIOStream::Input(sender) => sender.is_closed(),
            ClientApiWorkerIOStream::Output(sender) => sender.is_closed(),
        }
    }
}

#[async_trait]
impl IOStreamExt<WorkerEvent, WorkerIOStreamError> for WorkerIOStream {
    async fn send(&self, event: WorkerEvent) -> Result<(), WorkerIOStreamError> {
        match self {
            WorkerIOStream::Input(sender) => {
                if let Err(e) = sender.send(Ok(event)).await {
                    let result = e.0;
                    if let Err(status) = result {
                        return Err(WorkerIOStreamError::Input(status));
                    }
                }
            }
            WorkerIOStream::Output(sender) => {
                if let Err(e) = sender.send(event).await {
                    return Err(WorkerIOStreamError::Output(e.0));
                }
            }
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        match self {
            WorkerIOStream::Input(sender) => sender.is_closed(),
            WorkerIOStream::Output(sender) => sender.is_closed(),
        }
    }
}
