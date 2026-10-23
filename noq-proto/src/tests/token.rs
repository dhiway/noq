//! Tests specifically for tokens

#[cfg(feature = "qlog")]
use alloc::vec::Vec;
use alloc::{boxed::Box, sync::Arc};
use std::sync::Mutex;

use assert_matches::assert_matches;

use crate::{
    ConnectionError, Duration, Event, SystemTime, TimeSource, TokenStore, TransportErrorCode,
    VarInt,
};

use super::util::{
    IncomingConnectionBehavior, Pair, client_config, server_config, subscribe, validate_incoming,
};

use bytes::Bytes;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

#[test]
fn stateless_retry() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let (client_ch, _server_ch) = pair.connect();
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn retry_token_expired() {
    let _guard = subscribe();

    let fake_time = Arc::new(FakeTimeSource::new());
    let retry_token_lifetime = Duration::from_secs(1);

    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);

    let mut config = server_config();
    config
        .time_source(Arc::clone(&fake_time) as _)
        .retry_token_lifetime(retry_token_lifetime);
    pair.server.set_server_config(Some(Arc::new(config)));

    let client_ch = pair.begin_connect(client_config());
    pair.drive_client();
    pair.drive_server();
    pair.drive_client();

    // to expire retry token
    fake_time.advance(retry_token_lifetime + Duration::from_millis(1));

    pair.drive();
    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::ConnectionLost { reason: ConnectionError::ConnectionClosed(err) })
        if err.error_code == TransportErrorCode::INVALID_TOKEN
    );

    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_token() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = client_config();
    let (client_ch, _server_ch) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn retry_then_use_token() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = client_config();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let (client_ch, _server_ch) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_token_then_retry() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = client_config();
    let (client_ch, _server_ch) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new({
        let mut i = 0;
        move |incoming| {
            if i == 0 {
                assert!(incoming.remote_address_validated());
                assert!(incoming.may_retry());
                i += 1;
                IncomingConnectionBehavior::Retry
            } else if i == 1 {
                assert!(incoming.remote_address_validated());
                assert!(!incoming.may_retry());
                i += 1;
                IncomingConnectionBehavior::Accept
            } else {
                panic!("too many handle_incoming iterations")
            }
        }
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_same_token_twice() {
    #[derive(Default)]
    struct EvilTokenStore(Mutex<Bytes>);

    impl TokenStore for EvilTokenStore {
        fn insert(&self, _server_name: &str, token: Bytes) {
            let mut lock = self.0.lock().unwrap();
            if lock.is_empty() {
                *lock = token;
            }
        }

        fn take(&self, _server_name: &str) -> Option<Bytes> {
            let lock = self.0.lock().unwrap();
            if lock.is_empty() {
                None
            } else {
                Some(lock.clone())
            }
        }
    }

    let _guard = subscribe();
    let mut pair = Pair::default();
    let mut client_config = client_config();
    client_config.token_store(Arc::new(EvilTokenStore::default()));
    let (client_ch, _server_ch) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(!incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_3, _server_ch_3) = pair.connect_with(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_3)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_token_expired() {
    let _guard = subscribe();
    let fake_time = Arc::new(FakeTimeSource::new());
    let lifetime = Duration::from_secs(10000);
    let mut server_config = server_config();
    server_config
        .time_source(Arc::clone(&fake_time) as _)
        .validation_token
        .lifetime(lifetime);
    let mut pair = Pair::new(Default::default(), server_config);
    let client_config = client_config();
    let (client_ch, _server_ch) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with(client_config.clone());
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    fake_time.advance(lifetime + Duration::from_secs(1));

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(!incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_3, _server_ch_3) = pair.connect_with(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_3)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

pub(super) struct FakeTimeSource(Mutex<SystemTime>);

impl FakeTimeSource {
    pub(super) fn new() -> Self {
        Self(Mutex::new(SystemTime::now()))
    }

    pub(super) fn advance(&self, dur: Duration) {
        *self.0.lock().unwrap() += dur;
    }
}

impl TimeSource for FakeTimeSource {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

#[test]
fn discarded_retry_on_another_path_does_not_migrate() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let mut config = client_config();
    let mut transport = crate::TransportConfig::default();
    transport.server_handshake_migration(true);
    #[cfg(feature = "qlog")]
    let capture = QlogCapture::default();
    #[cfg(feature = "qlog")]
    transport.qlog_factory(Arc::new(capture.clone()));
    config.transport = Arc::new(transport);
    let client = pair.begin_connect(config);
    pair.drive_client();

    // Fan the same first Initial over two paths, as Iroh's mixed address does.
    let (when, initial) = pair.server.inbound.pop_first().unwrap();
    let mut other_initial = initial.clone();
    let other_server = "[fd15:70a:510b:1::2]:12345"
        .parse::<core::net::SocketAddr>()
        .unwrap();
    other_initial.remote = "[fd15:70a:510b:1::1]:12345".parse().unwrap();
    other_initial.dst_ip = Some(other_server.ip());
    pair.server.inbound.push(when, initial);
    pair.server.inbound.push(when, other_initial);
    pair.server.drive(pair.time);
    let (_, first_bytes) = pair.server.outbound.pop_front().unwrap();
    let (_, second_bytes) = pair.server.outbound.pop_front().unwrap();
    assert!(pair.server.outbound.is_empty());

    let first = super::util::Inbound {
        ecn: None,
        packet: first_bytes.into(),
        remote: Pair::SERVER_ADDR,
        dst_ip: Some(Pair::CLIENT_ADDR.ip()),
    };
    let second = super::util::Inbound {
        ecn: None,
        packet: second_bytes.into(),
        remote: other_server,
        dst_ip: Some("fd15:70a:510b:1::1".parse().unwrap()),
    };
    pair.client.inbound.push(pair.time, first);
    pair.client.drive_incoming(pair.time);
    for event in pair.client.conn_events.remove(&client).unwrap() {
        pair.client_conn_mut(client).handle_event(event);
    }
    let chosen = pair
        .client_conn_mut(client)
        .network_path(crate::PathId::ZERO)
        .unwrap();
    assert_eq!(chosen.remote(), Pair::SERVER_ADDR);

    let next_timeout = pair.client_conn_mut(client).poll_timeout();
    assert!(next_timeout.is_some());
    #[cfg(feature = "qlog")]
    let qlog_start = capture.checkpoint();

    // Deliver the competing Retry before polling another transmit.
    pair.time += Duration::from_millis(1);
    pair.client.inbound.push(pair.time, second);
    pair.client.drive_incoming(pair.time);
    for event in pair.client.conn_events.remove(&client).unwrap() {
        pair.client_conn_mut(client).handle_event(event);
    }
    assert_eq!(
        pair.client_conn_mut(client)
            .network_path(crate::PathId::ZERO)
            .unwrap(),
        chosen,
        "a discarded Retry must not change the path selected by the accepted Retry"
    );

    assert_eq!(pair.client_conn_mut(client).poll_timeout(), next_timeout);
    #[cfg(feature = "qlog")]
    capture.assert_only_received_since(qlog_start);

    // The original address-bound token and CID must still complete the handshake.
    pair.drive();
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
}

#[test]
fn bad_tag_retry_does_not_migrate_or_block_valid_retry() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let mut config = client_config();
    let mut transport = crate::TransportConfig::default();
    transport.server_handshake_migration(true);
    #[cfg(feature = "qlog")]
    let capture = QlogCapture::default();
    #[cfg(feature = "qlog")]
    transport.qlog_factory(Arc::new(capture.clone()));
    config.transport = Arc::new(transport);
    let client = pair.begin_connect(config);
    let original = pair
        .client_conn_mut(client)
        .network_path(crate::PathId::ZERO)
        .unwrap();
    pair.drive_client();
    pair.drive_server();
    let (_, valid_retry) = pair.client.inbound.pop_first().unwrap();
    let mut invalid = valid_retry.clone();
    invalid.remote = "[fd15:70a:510b:1::2]:12345".parse().unwrap();
    let last = invalid.packet.len() - 1;
    invalid.packet[last] ^= 1; // Corrupt the Retry integrity tag.
    let next_timeout = pair.client_conn_mut(client).poll_timeout();
    assert!(next_timeout.is_some());
    #[cfg(feature = "qlog")]
    let qlog_start = capture.checkpoint();
    pair.time += Duration::from_millis(1);
    pair.client.inbound.push(pair.time, invalid);
    pair.client.drive_incoming(pair.time);
    for event in pair.client.conn_events.remove(&client).unwrap() {
        pair.client_conn_mut(client).handle_event(event);
    }
    assert_eq!(
        pair.client_conn_mut(client)
            .network_path(crate::PathId::ZERO)
            .unwrap(),
        original,
        "a Retry with a bad integrity tag must not migrate the handshake"
    );
    assert_eq!(pair.client_conn_mut(client).poll_timeout(), next_timeout);
    #[cfg(feature = "qlog")]
    capture.assert_only_received_since(qlog_start);

    pair.client.inbound.push(pair.time, valid_retry);
    pair.drive();
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
}

#[test]
fn valid_first_retry_can_migrate_and_complete_handshake() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let mut config = client_config();
    let mut transport = crate::TransportConfig::default();
    transport.server_handshake_migration(true);
    #[cfg(feature = "qlog")]
    let capture = QlogCapture::default();
    #[cfg(feature = "qlog")]
    transport.qlog_factory(Arc::new(capture.clone()));
    config.transport = Arc::new(transport);
    let client = pair.begin_connect(config);
    let original = pair
        .client_conn_mut(client)
        .network_path(crate::PathId::ZERO)
        .unwrap();
    pair.drive_client();

    // The same Initial reaches the server on a different path. Its first response
    // is a genuine address-bound Retry, and subsequent traffic uses that path.
    let migrated_server = "[fd15:70a:510b:1::2]:12345"
        .parse::<core::net::SocketAddr>()
        .unwrap();
    let (when, mut initial) = pair.server.inbound.pop_first().unwrap();
    initial.dst_ip = Some(migrated_server.ip());
    pair.server.inbound.push(when, initial);
    pair.routes.as_basic_mut().server_addr = migrated_server;
    pair.drive_server();
    #[cfg(feature = "qlog")]
    let qlog_start = capture.checkpoint();
    pair.client.drive_incoming(pair.time);
    for event in pair.client.conn_events.remove(&client).unwrap() {
        pair.client_conn_mut(client).handle_event(event);
    }
    let chosen = pair
        .client_conn_mut(client)
        .network_path(crate::PathId::ZERO)
        .unwrap();
    assert_ne!(chosen, original);
    assert_eq!(chosen.remote(), migrated_server);
    #[cfg(feature = "qlog")]
    {
        assert_eq!(capture.events_since(qlog_start, "quic:packet_received"), 1);
        assert_eq!(capture.events_since(qlog_start, "quic:tuple_assigned"), 1);
    }

    // Migration must preserve the valid first Retry's token/CID processing.
    pair.drive();
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
}

#[cfg(feature = "qlog")]
#[derive(Clone, Default)]
struct QlogCapture(Arc<Mutex<Vec<u8>>>);

#[cfg(feature = "qlog")]
impl QlogCapture {
    fn checkpoint(&self) -> usize {
        let len = self.0.lock().unwrap().len();
        assert!(len > 0, "the connection must have an active qlog sink");
        len
    }

    fn events_since(&self, start: usize, name: &str) -> usize {
        let bytes = self.0.lock().unwrap();
        // Count semantic event names in the complete JSON-SEQ records written by
        // the real qlog streamer, without requiring another parsing dependency.
        core::str::from_utf8(&bytes[start..])
            .unwrap()
            .split('\u{1e}')
            .filter(|record| record.contains(name))
            .count()
    }

    fn assert_only_received_since(&self, start: usize) {
        assert_eq!(self.events_since(start, "quic:packet_received"), 1);
        assert_eq!(self.events_since(start, "quic:tuple_assigned"), 0);
        assert_eq!(self.events_since(start, "quic:timer_updated"), 0);
    }
}

#[cfg(feature = "qlog")]
impl std::io::Write for QlogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "qlog")]
impl crate::QlogFactory for QlogCapture {
    fn for_connection(
        &self,
        _side: crate::Side,
        _remote: core::net::SocketAddr,
        _initial_dst_cid: crate::ConnectionId,
        _now: crate::Instant,
    ) -> Option<crate::QlogConfig> {
        Some(crate::QlogConfig::new(Box::new(self.clone())))
    }
}
