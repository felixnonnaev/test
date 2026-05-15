use axum::{
    Router,
    extract::Request,
    http::{
        HeaderMap, StatusCode, Uri,
        header::{CONTENT_ENCODING, CONTENT_TYPE, HOST},
    },
    response::{IntoResponse, Response},
    routing::get,
};
use core::result::Result::Err;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
use tower_service::Service;
use webtransport::WebTransportServer;
use wtransport::Identity;

mod http_data {
    pub const HYPERSTREAMS_INDEX: &[u8] = include_bytes!("hyperstreams.html.br");
    pub const HYPERMESSAGES_INDEX: &[u8] = include_bytes!("hypermessages.html.br");
}

async fn serve_index(uri: Uri, headers: HeaderMap) -> Response {
    let host_str = headers
        .get(HOST)
        .and_then(|val| val.to_str().ok())
        .or_else(|| uri.authority().map(|auth| auth.host()))
        .unwrap_or("");

    let domain = host_str.split(':').next().unwrap_or(host_str);

    let data: &'static [u8] = match domain {
        "hypermessages.deadbrains.app" => http_data::HYPERMESSAGES_INDEX,
        "hyperstreams.deadbrains.app" => http_data::HYPERSTREAMS_INDEX,
        _ => http_data::HYPERSTREAMS_INDEX,
    };

    (
        StatusCode::OK,
        [
            (CONTENT_TYPE, "text/html; charset=utf-8"),
            (CONTENT_ENCODING, "br"),
        ],
        data,
    )
        .into_response()
}

fn rustls_server_config(
    key_path: impl AsRef<Path>,
    cert_path: impl AsRef<Path>,
) -> Arc<ServerConfig> {
    let key = PrivateKeyDer::from_pem_file(key_path).unwrap();
    let certs = CertificateDer::pem_file_iter(cert_path)
        .unwrap()
        .map(|cert| cert.unwrap())
        .collect();

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    config.max_early_data_size = 16384;

    Arc::new(config)
}

#[tokio::main]
async fn main() {
    println!("/628b460e-c34c-429d-a73a-ec69dda577cz");

    let cert_path = "tls_cert.pem";
    let key_path = "tls_key.pem";

    let rustls_config = rustls_server_config(key_path, cert_path);
    let tls_acceptor = TlsAcceptor::from(rustls_config);

    let http_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 5000));
    let tcp_listener = TcpListener::bind(http_addr).await.unwrap();

    let app = Router::new().route("/628b460e-c34c-429d-a73a-ec69dda577cz", get(serve_index));

    let http_server_task = tokio::spawn(async move {
        loop {
            let tower_service = app.clone();
            let tls_acceptor = tls_acceptor.clone();

            let (cnx, _) = match tcp_listener.accept().await {
                Ok(res) => res,
                Err(_) => continue,
            };

            tokio::spawn(async move {
                let stream = match tls_acceptor.accept(cnx).await {
                    Ok(s) => s,
                    Err(_) => return,
                };

                let io = TokioIo::new(stream);

                let hyper_service =
                    hyper::service::service_fn(move |request: Request<Incoming>| {
                        tower_service.clone().call(request)
                    });

                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(io, hyper_service)
                    .await;
            });
        }
    });

    let wt_identity = Identity::load_pemfiles(cert_path, key_path).await.unwrap();

    let webtransport_server = WebTransportServer::new(wt_identity, 5001);

    let wt_server_task = tokio::spawn(async move {
        webtransport_server.serve().await;
    });

    tokio::select! {
        _ = http_server_task => {}
        _ = wt_server_task => {}
    }
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
