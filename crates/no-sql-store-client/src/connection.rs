use crate::proto::worker_api::v1::{
    ClientEvent, ClientRequest, Record, client_event::Payload, worker_api_client::WorkerApiClient,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

type Response = Result<Option<Record>, String>;
type PendingRequests = Arc<Mutex<HashMap<i32, oneshot::Sender<Response>>>>;

pub(crate) struct WorkerConnection {
    outbound: mpsc::Sender<ClientEvent>,
    pending: PendingRequests,
}

impl WorkerConnection {
    pub(crate) async fn connect(endpoint: &str) -> Result<Self, String> {
        let channel = Channel::from_shared(endpoint.to_owned())
            .map_err(|error| error.to_string())?
            .connect()
            .await
            .map_err(|error| error.to_string())?;
        let (outbound, receiver) = mpsc::channel(32);
        let mut inbound = WorkerApiClient::new(channel)
            .open_client_connection(ReceiverStream::new(receiver))
            .await
            .map_err(|error| error.to_string())?
            .into_inner();
        let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));
        let response_pending = pending.clone();
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(ClientEvent {
                        payload: Some(Payload::Response(response)),
                    })) => {
                        if let Some(sender) =
                            response_pending.lock().await.remove(&response.request_id)
                        {
                            let _ = sender.send(Ok(response.record));
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => {
                        drain(&response_pending, error.to_string()).await;
                        return;
                    }
                }
            }
            drain(&response_pending, "worker stream closed".into()).await;
        });
        Ok(Self { outbound, pending })
    }

    pub(crate) async fn request(&self, request: ClientRequest) -> Result<Option<Record>, String> {
        let id = request.id;
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id, sender);
        if self
            .outbound
            .send(ClientEvent {
                payload: Some(Payload::Request(request)),
            })
            .await
            .is_err()
        {
            self.pending.lock().await.remove(&id);
            return Err("worker stream closed".into());
        }
        receiver
            .await
            .map_err(|_| "worker stream closed".to_string())?
    }
}

async fn drain(pending: &Mutex<HashMap<i32, oneshot::Sender<Response>>>, reason: String) {
    for (_, sender) in std::mem::take(&mut *pending.lock().await) {
        let _ = sender.send(Err(reason.clone()));
    }
}
