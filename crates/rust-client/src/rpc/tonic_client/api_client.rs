use alloc::string::String;
use core::fmt::Write;
use core::ops::{Deref, DerefMut};

use api_client_wrapper::{ApiClient, InnerClient};
use miden_protocol::Word;
use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::service::Interceptor;

/// A caller-supplied request header, injected into every outbound gRPC call made by
/// [`ApiClient`].
///
/// The key is a `&'static str` so headers are always known at compile time (matching the
/// shape of the existing `accept` header). The value is a runtime `String` so secrets such
/// as bearer tokens can be passed through from configuration.
pub(crate) type ExtraHeader = (&'static str, String);

// WEB CLIENT
// ================================================================================================

#[cfg(target_arch = "wasm32")]
pub(crate) mod api_client_wrapper {
    use alloc::string::String;
    use alloc::vec::Vec;

    use miden_protocol::Word;
    use tonic::service::interceptor::InterceptedService;

    use super::{ExtraHeader, MetadataInterceptor, accept_header_interceptor};
    use crate::rpc::RpcError;
    use crate::rpc::generated::rpc::api_client::ApiClient as ProtoClient;

    pub type WasmClient = tonic_web_wasm_client::Client;
    pub type InnerClient = ProtoClient<InterceptedService<WasmClient, MetadataInterceptor>>;
    #[derive(Clone)]
    pub struct ApiClient {
        pub(crate) client: InnerClient,
        wasm_client: WasmClient,
        extra_headers: Vec<ExtraHeader>,
    }

    impl ApiClient {
        /// Connects to the Miden node API using the provided URL and genesis commitment.
        ///
        /// `extra_headers` are injected into every outbound request alongside the standard
        /// `accept` header. The client is configured with an interceptor that sets all
        /// requisite request metadata.
        // Kept async for API parity with the native client; in WASM this is synchronous.
        #[allow(clippy::unused_async)]
        pub async fn new_client(
            endpoint: String,
            _timeout_ms: u64,
            genesis_commitment: Option<Word>,
            extra_headers: Vec<ExtraHeader>,
        ) -> Result<ApiClient, RpcError> {
            let wasm_client = WasmClient::new(endpoint);
            let interceptor = accept_header_interceptor(genesis_commitment, &extra_headers)?;
            let client = ProtoClient::with_interceptor(wasm_client.clone(), interceptor);
            Ok(ApiClient { client, wasm_client, extra_headers })
        }

        /// Connects to the Miden node API without injecting an Accept header.
        ///
        /// `extra_headers` are still injected into every outbound request.
        // Kept async for API parity with the native client; in WASM this is synchronous.
        #[allow(clippy::unused_async)]
        pub async fn new_client_without_accept_header(
            endpoint: String,
            _timeout_ms: u64,
            extra_headers: Vec<ExtraHeader>,
        ) -> Result<ApiClient, RpcError> {
            let wasm_client = WasmClient::new(endpoint);
            let interceptor = MetadataInterceptor::default().with_extra_headers(&extra_headers)?;
            let client = ProtoClient::with_interceptor(wasm_client.clone(), interceptor);
            Ok(ApiClient { client, wasm_client, extra_headers })
        }

        /// Returns a new `ApiClient` with an updated genesis commitment.
        /// This creates a new client that shares the same underlying channel. Any
        /// `extra_headers` passed to the constructor are preserved.
        pub fn set_genesis_commitment(
            &mut self,
            genesis_commitment: Word,
        ) -> Result<&mut Self, RpcError> {
            let interceptor =
                accept_header_interceptor(Some(genesis_commitment), &self.extra_headers)?;
            self.client = ProtoClient::with_interceptor(self.wasm_client.clone(), interceptor);
            Ok(self)
        }
    }
}

// CLIENT
// ================================================================================================

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod api_client_wrapper {
    use alloc::boxed::Box;
    use alloc::string::String;
    use alloc::vec::Vec;
    use core::time::Duration;

    use miden_protocol::Word;
    use tonic::service::interceptor::InterceptedService;
    use tonic::transport::Channel;

    use super::{ExtraHeader, MetadataInterceptor, accept_header_interceptor};
    use crate::rpc::RpcError;
    use crate::rpc::generated::rpc::api_client::ApiClient as ProtoClient;

    pub type InnerClient = ProtoClient<InterceptedService<Channel, MetadataInterceptor>>;
    #[derive(Clone)]
    pub struct ApiClient {
        pub(crate) client: InnerClient,
        channel: Channel,
        extra_headers: Vec<ExtraHeader>,
    }

    impl ApiClient {
        /// Connects to the Miden node API using the provided URL, timeout and genesis commitment.
        ///
        /// `extra_headers` are injected into every outbound request alongside the standard
        /// `accept` header. The client is configured with an interceptor that sets all
        /// requisite request metadata.
        pub async fn new_client(
            endpoint: String,
            timeout_ms: u64,
            genesis_commitment: Option<Word>,
            extra_headers: Vec<ExtraHeader>,
        ) -> Result<ApiClient, RpcError> {
            // Build the interceptor first so invalid caller-supplied metadata fails fast,
            // before we attempt the network connection.
            let interceptor = accept_header_interceptor(genesis_commitment, &extra_headers)?;

            // Setup connection channel.
            let endpoint = tonic::transport::Endpoint::try_from(endpoint)
                .map_err(|err| RpcError::ConnectionError(Box::new(err)))?
                .timeout(Duration::from_millis(timeout_ms));
            let channel = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                .map_err(|err| RpcError::ConnectionError(Box::new(err)))?
                .connect()
                .await
                .map_err(|err| RpcError::ConnectionError(Box::new(err)))?;

            // Return the connected client.
            let client = ProtoClient::with_interceptor(channel.clone(), interceptor);
            Ok(ApiClient { client, channel, extra_headers })
        }

        /// Connects to the Miden node API without injecting an Accept header.
        ///
        /// `extra_headers` are still injected into every outbound request.
        pub async fn new_client_without_accept_header(
            endpoint: String,
            timeout_ms: u64,
            extra_headers: Vec<ExtraHeader>,
        ) -> Result<ApiClient, RpcError> {
            // Fail fast on invalid caller-supplied metadata, before opening the channel.
            let interceptor = MetadataInterceptor::default().with_extra_headers(&extra_headers)?;

            // Setup connection channel.
            let endpoint = tonic::transport::Endpoint::try_from(endpoint)
                .map_err(|err| RpcError::ConnectionError(Box::new(err)))?
                .timeout(Duration::from_millis(timeout_ms));
            let channel = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                .map_err(|err| RpcError::ConnectionError(Box::new(err)))?
                .connect()
                .await
                .map_err(|err| RpcError::ConnectionError(Box::new(err)))?;

            let client = ProtoClient::with_interceptor(channel.clone(), interceptor);
            Ok(ApiClient { client, channel, extra_headers })
        }

        /// Returns a new `ApiClient` with an updated genesis commitment.
        /// This creates a new client that shares the same underlying channel. Any
        /// `extra_headers` passed to the constructor are preserved.
        pub fn set_genesis_commitment(
            &mut self,
            genesis_commitment: Word,
        ) -> Result<&mut Self, RpcError> {
            let interceptor =
                accept_header_interceptor(Some(genesis_commitment), &self.extra_headers)?;
            self.client = ProtoClient::with_interceptor(self.channel.clone(), interceptor);
            Ok(self)
        }
    }
}

impl Deref for ApiClient {
    type Target = InnerClient;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for ApiClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

// INTERCEPTOR
// ================================================================================================

/// Interceptor designed to inject required metadata into all [`ApiClient`] requests.
#[derive(Default, Clone)]
pub struct MetadataInterceptor {
    metadata: alloc::collections::BTreeMap<&'static str, AsciiMetadataValue>,
}

impl MetadataInterceptor {
    /// Adds or overwrites metadata on the interceptor.
    pub fn with_metadata(
        mut self,
        key: &'static str,
        value: String,
    ) -> Result<Self, InvalidMetadataValue> {
        self.metadata.insert(key, AsciiMetadataValue::try_from(value)?);
        Ok(self)
    }

    /// Adds or overwrites every `(key, value)` pair in `headers` on the interceptor.
    ///
    /// Returns [`RpcError::ConnectionError`] if any value is not a valid ASCII metadata value,
    /// mirroring the behaviour of other transport-setup failures on the client.
    pub(super) fn with_extra_headers(
        mut self,
        headers: &[ExtraHeader],
    ) -> Result<Self, crate::rpc::RpcError> {
        for (key, value) in headers {
            self = self.with_metadata(key, value.clone()).map_err(|err| {
                crate::rpc::RpcError::ConnectionError(alloc::boxed::Box::new(err))
            })?;
        }
        Ok(self)
    }
}

impl Interceptor for MetadataInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = request;
        for (key, value) in &self.metadata {
            request.metadata_mut().insert(*key, value.clone());
        }
        Ok(request)
    }
}

/// Returns the HTTP header [`MetadataInterceptor`] that is expected by Miden RPC.
///
/// The interceptor sets the `accept` header to the Miden API version and optionally includes the
/// genesis commitment. Any `extra_headers` supplied by the caller are appended, allowing callers
/// to inject request metadata such as an `authorization` bearer token.
fn accept_header_interceptor(
    genesis_digest: Option<Word>,
    extra_headers: &[ExtraHeader],
) -> Result<MetadataInterceptor, crate::rpc::RpcError> {
    let version = env!("CARGO_PKG_VERSION");
    let mut accept_value = format!("application/vnd.miden; version={version}");
    if let Some(commitment) = genesis_digest {
        write!(accept_value, "; genesis={}", commitment.to_hex())
            .expect("valid hex representation of Word");
    }

    MetadataInterceptor::default()
        .with_metadata("accept", accept_value)
        .expect("valid key/value metadata for interceptor")
        .with_extra_headers(extra_headers)
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use tonic::Request;
    use tonic::service::Interceptor;

    use super::{MetadataInterceptor, accept_header_interceptor};

    #[test]
    fn interceptor_injects_caller_supplied_headers_onto_request() {
        // Build the same interceptor that the native/WASM clients would use, with a caller
        // `authorization` header in addition to the standard `accept`.
        let extra = [("authorization", "Bearer test-token".to_string())];
        let mut interceptor = accept_header_interceptor(None, &extra).expect("build interceptor");

        // Run it against a bare request to inspect what actually ends up on the wire.
        let request = interceptor.call(Request::new(())).expect("interceptor call succeeds");
        let metadata = request.metadata();

        let auth = metadata
            .get("authorization")
            .expect("authorization header must be present on outbound request");
        assert_eq!(auth.to_str().unwrap(), "Bearer test-token");

        // The standard accept header is still set alongside the caller's header.
        assert!(metadata.get("accept").is_some(), "accept header must still be present");
    }

    #[test]
    fn interceptor_omits_caller_headers_when_none_configured() {
        let mut interceptor = accept_header_interceptor(None, &[]).expect("build interceptor");

        let request = interceptor.call(Request::new(())).expect("interceptor call succeeds");
        let metadata = request.metadata();

        assert!(
            metadata.get("authorization").is_none(),
            "authorization must not leak when no header is configured",
        );
        assert!(metadata.get("accept").is_some(), "accept header must still be present");
    }

    #[test]
    fn with_extra_headers_rejects_invalid_ascii_values() {
        // Control characters are not valid ASCII metadata values; the builder must reject them
        // rather than silently dropping the header.
        let bad = [("authorization", "Bearer bad\nvalue".to_string())];
        match MetadataInterceptor::default().with_extra_headers(&bad) {
            Err(crate::rpc::RpcError::ConnectionError(_)) => {},
            Err(other) => panic!("expected ConnectionError, got {other:?}"),
            Ok(_) => panic!("expected invalid metadata value to error"),
        }
    }
}
