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
            tx: broadcast::Sender<Bytes>,
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
                      let (stream_tx, _) = broadcast::channel::<Bytes>(1024);

                      let expected_receivers = global_route_tx.receiver_count().saturating_sub(1);

                      let _ = global_route_tx.send(RouterMsg::UniStream {
                       sender_id: my_id,
                       tx: stream_tx.clone(),
                      });

                      tokio::spawn(async move {
                          if expected_receivers > 0 {
                              for _ in 0..50 {
                                  if stream_tx.receiver_count() >= expected_receivers {
                                      break;
                                  }
                                  tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                              }
                          }

                          let mut buffer = vec![0; 65536];
                          while let Ok(Some(bytes_read)) = recv_stream.read(&mut buffer).await {
                              let chunk = Bytes::copy_from_slice(&buffer[..bytes_read]);
                              let _ = stream_tx.send(chunk);
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
                            Ok(RouterMsg::UniStream { sender_id, tx }) => {
                                if sender_id != my_id {
                                    let connection = connection.clone();
                                    let mut stream_rx = tx.subscribe();

                                    tokio::spawn(async move {
                                        if let Ok(opening) = connection.open_uni().await {
                                            if let Ok(mut out_stream) = opening.await {
                                                while let Ok(chunk) = stream_rx.recv().await {
                                                    if out_stream.write_all(&chunk).await.is_err() { break; }
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
            let (route_tx, _) = broadcast::channel::<RouterMsg>(16);

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
