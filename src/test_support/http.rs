//! Loopback-only raw HTTP fixture for downloader regression tests.
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

pub(crate) struct Server {
    pub(crate) url: String,
    requests: mpsc::Receiver<Request>,
    task: JoinHandle<()>,
}

pub(crate) struct Request {
    pub(crate) head: String,
    reply: oneshot::Sender<(Vec<u8>, oneshot::Receiver<()>)>,
}

impl Request {
    /// Keep the returned sender alive to hold the socket open after these bytes.
    pub(crate) fn respond(self, bytes: impl Into<Vec<u8>>) -> oneshot::Sender<()> {
        let (finish, wait) = oneshot::channel();
        self.reply.send((bytes.into(), wait)).unwrap();
        finish
    }
}

impl Server {
    pub(crate) async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::channel(16);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let tx = tx.clone();
                        connections.spawn(async move {
                            let mut head = Vec::new();
                            while !head.ends_with(b"\r\n\r\n") {
                                let byte = socket.read_u8().await.unwrap();
                                head.push(byte);
                                assert!(head.len() < 16384);
                            }
                            let (reply, response) = oneshot::channel();
                            if tx.send(Request { head: String::from_utf8(head).unwrap(), reply }).await.is_err() {
                                return;
                            }
                            if let Ok((bytes, finish)) = response.await {
                                let _ = socket.write_all(&bytes).await;
                                let _ = finish.await;
                            }
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                }
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }

    pub(crate) async fn next(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .expect("local HTTP request stalled")
            .expect("server stopped")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n",
        body.len()
    )
    .into_bytes();
    bytes.extend_from_slice(body);
    bytes
}
