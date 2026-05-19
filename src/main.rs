use webtransport::WebTransportServer;
use wtransport::Identity;
use wtransport::tls::Sha256DigestFmt;

#[tokio::main]
async fn main() {
    let wt_identity = Identity::self_signed(["localhost"]).unwrap();
    let cert_digest = wt_identity.certificate_chain().as_slice()[0].hash();
    println!("{}", cert_digest.fmt(Sha256DigestFmt::BytesArray));

    let webtransport_server = WebTransportServer::new(wt_identity, 5002);
    webtransport_server.serve().await;
}

mod webtransport {
    use bytes::Bytes;
    use futures::future::join_all;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};
    use tokio::time::{Duration, timeout};
    use wtransport::Endpoint;
    use wtransport::Identity;
    use wtransport::RecvStream;
    use wtransport::ServerConfig;
    use wtransport::endpoint::IncomingSession;
    use wtransport::endpoint::endpoint_side::Server;

    #[derive(Clone)]
    pub struct DatagramMsg {
        pub sender_id: usize,
        pub data: Bytes,
        pub received_at: std::time::Instant,
    }

    #[derive(Clone)]
    pub enum RouterMsg {
        UniStream {
            sender_id: usize,
            subscribe_tx: mpsc::Sender<mpsc::Sender<Bytes>>,
        },
        BiStream {
            sender_id: usize,
            req_data: Bytes,
            winner_tx: mpsc::Sender<(Bytes, RecvStream)>,
        },
    }

    pub struct WebTransportServer {
        endpoint: Endpoint<Server>,
    }

    impl WebTransportServer {
        pub fn new(identity: Identity, port: u16) -> Self {
            let config = ServerConfig::builder()
                .with_bind_default(port)
                .with_identity(identity)
                .keep_alive_interval(Some(Duration::from_secs(3)))
                .build();

            let endpoint = Endpoint::server(config).unwrap();
            Self { endpoint }
        }

        async fn handle_session(
            incoming_session: IncomingSession,
            global_dgram_tx: broadcast::Sender<DatagramMsg>,
            global_route_tx: broadcast::Sender<RouterMsg>,
        ) {
            let session_request = match incoming_session.await {
                Ok(req) => req,
                Err(_) => return,
            };

            let path = session_request.path();

            let uuid_prefix = "/628b460e-c34c-429d-a73a-ec69dda577cz";

            if !path.starts_with(&uuid_prefix) {
                return;
            }

            let connection = match session_request.accept().await {
                Ok(conn) => conn,
                Err(_) => return,
            };

            let connection = Arc::new(connection);
            let my_id = connection.stable_id();

            let mut dgram_rx = global_dgram_tx.subscribe();
            let mut route_rx = global_route_tx.subscribe();

            loop {
                tokio::select! {
                    dgram_res = connection.receive_datagram() => {
                        if let Ok(dgram) = dgram_res {
                            let _ = global_dgram_tx.send(DatagramMsg {
                                sender_id: my_id,
                                data: Bytes::copy_from_slice(&dgram),
                                received_at: std::time::Instant::now(),
                            });
                        } else { break; }
                    }

                    uni_res = connection.accept_uni() => {
                        if let Ok(mut recv_stream) = uni_res {
                            let expected_receivers = global_route_tx.receiver_count().saturating_sub(1);
                            let (subscribe_tx, mut subscribe_rx) = mpsc::channel::<mpsc::Sender<Bytes>>(128);

                            let _ = global_route_tx.send(RouterMsg::UniStream {
                                sender_id: my_id,
                                subscribe_tx,
                            });

                            tokio::spawn(async move {
                                let mut receivers = Vec::new();

                                if expected_receivers > 0 {
                                    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                                        while let Some(tx) = subscribe_rx.recv().await {
                                            receivers.push(tx);
                                            if receivers.len() >= expected_receivers {
                                                break;
                                            }
                                        }
                                    }).await;
                                }

                                let mut buffer = vec![0; 65536];

                                while let Ok(Some(bytes_read)) = recv_stream.read(&mut buffer).await {
                                    let chunk = Bytes::copy_from_slice(&buffer[..bytes_read]);

                                    while let Ok(tx) = subscribe_rx.try_recv() {
                                        receivers.push(tx);
                                    }

                                    let mut futures_list = Vec::new();
                                    for tx in receivers {
                                        let chunk = chunk.clone();
                                        futures_list.push(async move {
                                            let res = tokio::time::timeout(std::time::Duration::from_secs(10), tx.send(chunk)).await;
                                            (tx, res)
                                        });
                                    }

                                    let mut next_receivers = Vec::new();
                                    for (tx, res) in join_all(futures_list).await {
                                        match res {
                                            Ok(Ok(_)) => next_receivers.push(tx),
                                            Ok(Err(_)) => {},
                                            Err(_) => {},
                                        }
                                    }
                                    receivers = next_receivers;
                                }
                            });
                        } else { break; }
                    }

                    bi_res = connection.accept_bi() => {
                            if let Ok((mut a_send, mut a_recv)) = bi_res {
                            let global_route_tx = global_route_tx.clone();

                            tokio::spawn(async move {
                                let mut buf = vec![0; 64];
                                let mut total_read = 0;

                                while let Ok(Some(n)) = a_recv.read(&mut buf[total_read..]).await {
                                    total_read += n;

                                    if total_read == buf.len() {
                                        return;
                                    }
                                }

                                if total_read == 0 {
                                    return;
                                }

                                let req_data = Bytes::copy_from_slice(&buf[..total_read]);

                                let (winner_tx, mut winner_rx) = mpsc::channel(1);

                                let _ = global_route_tx.send(RouterMsg::BiStream {
                                    sender_id: my_id,
                                    req_data,
                                    winner_tx,
                                });

                                if let Ok(Some((first_chunk, mut winning_recv))) = timeout(Duration::from_secs(10), winner_rx.recv()).await {

                                    drop(winner_rx);

                                    if a_send.write_all(&first_chunk).await.is_ok() {
                                        let mut buf = vec![0; 65536];
                                        while let Ok(Some(n)) = winning_recv.read(&mut buf).await {
                                            if a_send.write_all(&buf[..n]).await.is_err() {
                                                break;
                                            }
                                        }
                                        let _ = a_send.finish().await;
                                    }
                                }
                            });
                        } else { break; }
                    }

                    dgram_msg = dgram_rx.recv() => {
                        match dgram_msg {
                            Ok(DatagramMsg { sender_id, data, received_at }) => {
                                if sender_id != my_id && received_at.elapsed() < std::time::Duration::from_millis(150) {
                                    let _ = connection.send_datagram(data);
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }

                    route_msg = route_rx.recv() => {
                      match route_msg {
                          Ok(RouterMsg::UniStream { sender_id, subscribe_tx }) => {
                              if sender_id != my_id {
                                  let connection = connection.clone();
                                  let (tx, mut rx) = mpsc::channel::<Bytes>(128);

                                  tokio::spawn(async move {
                                      if subscribe_tx.send(tx).await.is_ok() {

                                          if let Ok(opening) = connection.open_uni().await {
                                              if let Ok(mut out_stream) = opening.await {

                                                  while let Some(chunk) = rx.recv().await {
                                                      if out_stream.write_all(&chunk).await.is_err() {
                                                          break;
                                                      }
                                                  }

                                                  let _ = out_stream.finish().await;
                                              }
                                          }
                                      }
                                  });
                              }
                          }

                            Ok(RouterMsg::BiStream { sender_id, req_data, winner_tx }) => {
                                if sender_id != my_id {
                                    let connection = connection.clone();

                                    tokio::spawn(async move {
                                        if let Ok(opening) = connection.open_bi().await {
                                            if let Ok((mut b_send, mut b_recv)) = opening.await {

                                                if b_send.write_all(&req_data).await.is_err() {
                                                    return;
                                                }

                                                 let _ = b_send.finish().await;

                                                drop(b_send);

                                                let mut buf = vec![0; 65536];
                                                tokio::select! {
                                                    res = b_recv.read(&mut buf) => {
                                                        match res {
                                                            Ok(Some(n)) => {
                                                                let first_chunk = Bytes::copy_from_slice(&buf[..n]);

                                                                let _ = winner_tx.send((first_chunk, b_recv)).await;
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                    _ = winner_tx.closed() => {}
                                                }
                                            }
                                        }
                                    });
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        }

        pub async fn serve(self) {
            let (dgram_tx, _) = broadcast::channel::<DatagramMsg>(4096);
            let (route_tx, _) = broadcast::channel::<RouterMsg>(256);

            loop {
                let incoming = self.endpoint.accept().await;
                tokio::spawn(Self::handle_session(
                    incoming,
                    dgram_tx.clone(),
                    route_tx.clone(),
                ));
            }
        }
    }
}
