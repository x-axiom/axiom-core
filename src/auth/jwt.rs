//! JWT signing, verification, and refresh-token management.
//!
//! Uses Ed25519 (EdDSA) via the `jsonwebtoken` crate.
//!
//! # Token types
//!
//! | Type    | TTL    | Stateful?                          |
//! |---------|--------|------------------------------------|
//! | Access  | 15 min | No (stateless, verified by pubkey)  |
//! | Refresh | 7 days | Yes (stored in FDB, revocable)     |
//!
//! # Key rotation
//!
//! [`JwtService`] accepts multiple [`JwtKeyPair`]s.  The active key signs new
//! tokens; all keys are tried during verification, enabling zero-downtime
//! rotation.  All public keys are exposed via [`JwtService::jwks`].
//!
//! # Refresh-token storage (FDB)
//!
//! Key layout: `rt\x00{jti}` → JSON `{"user_id": "…", "exp": 1234567890}`.
//! On `verify_and_consume_refresh_token`, the record is deleted atomically
//! (single-use semantics).  On revoke, it is explicitly cleared.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use foundationdb::{Database, FdbError};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{CasError, CasResult};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Access token time-to-live (15 minutes).
pub const ACCESS_TTL_SECS: u64 = 15 * 60;
/// Refresh token time-to-live (7 days).
pub const REFRESH_TTL_SECS: u64 = 7 * 24 * 3600;

/// FDB key prefix for refresh-token records.
const RT_PREFIX: &str = "rt\x00";

// ---------------------------------------------------------------------------
// Claims
// ---------------------------------------------------------------------------

/// Claims embedded in an **access token**.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessClaims {
    /// Subject: user ID.
    pub sub: String,
    pub tenant_id: String,
    pub org_id: String,
    /// Role names: `"Owner"` | `"Admin"` | `"Member"` | `"Viewer"`.
    pub roles: Vec<String>,
    /// Issued-at (Unix seconds).
    pub iat: u64,
    /// Expiry (Unix seconds).
    pub exp: u64,
    /// Unique token ID (UUID v4), usable as a jti.
    pub jti: String,
}

/// Claims embedded in a **refresh token** (minimal, no sensitive fields).
#[derive(Debug, Serialize, Deserialize)]
struct RefreshClaims {
    /// Subject: user ID.
    sub: String,
    /// Unique token ID — used as the FDB key for revocation.
    jti: String,
    iat: u64,
    exp: u64,
}

// ---------------------------------------------------------------------------
// FDB refresh-token record
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct RefreshRecord {
    user_id: String,
    exp: u64,
}

// ---------------------------------------------------------------------------
// JWKS
// ---------------------------------------------------------------------------

/// A single JSON Web Key (Ed25519 / OKP curve).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub crv: String,
    /// Base64url-encoded raw 32-byte Ed25519 public key.
    pub x: String,
    pub kid: String,
    #[serde(rename = "use")]
    pub use_: String,
    pub alg: String,
}

/// A JSON Web Key Set, returned from `GET /.well-known/jwks.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkSet {
    pub keys: Vec<Jwk>,
}

// ---------------------------------------------------------------------------
// JwtKeyPair
// ---------------------------------------------------------------------------

/// An Ed25519 key pair used for JWT signing and verification.
pub struct JwtKeyPair {
    pub kid: String,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    /// Raw 32-byte Ed25519 public key — exported in JWKS.
    raw_public_key: [u8; 32],
}

impl JwtKeyPair {
    /// Generate a fresh Ed25519 key pair with a random key ID.
    pub fn generate() -> CasResult<Self> {
        use ed25519_dalek::pkcs8::EncodePrivateKey;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let raw_public_key = signing_key.verifying_key().to_bytes();

        // Encode private key to PKCS8 DER → PEM for `EncodingKey::from_ed_pem`.
        let pkcs8_doc = signing_key
            .to_pkcs8_der()
            .map_err(|e| CasError::Store(format!("Ed25519 PKCS8 DER: {e}")))?;
        let private_pem = der_to_pem("PRIVATE KEY", pkcs8_doc.as_bytes());

        // Build SPKI DER for the public key → PEM for `DecodingKey::from_ed_pem`.
        // The SPKI (SubjectPublicKeyInfo) DER for Ed25519 is a fixed 12-byte
        // prefix (SEQUENCE + AlgorithmIdentifier for OID 1.3.101.112 + BIT STRING)
        // followed by the 32-byte raw public key.
        let spki_der = ed25519_spki_der(&raw_public_key);
        let public_pem = der_to_pem("PUBLIC KEY", &spki_der);

        let kid = Uuid::new_v4().to_string();
        Self::from_pem(kid, private_pem.as_bytes(), public_pem.as_bytes(), raw_public_key)
    }

    /// Load a key pair from PEM-encoded private (PKCS8) and public (SPKI) keys.
    ///
    /// `raw_public_key` is the raw 32-byte Ed25519 public key used only for
    /// JWKS export.
    pub fn from_pem(
        kid: impl Into<String>,
        private_pem: &[u8],
        public_pem: &[u8],
        raw_public_key: [u8; 32],
    ) -> CasResult<Self> {
        let encoding_key = EncodingKey::from_ed_pem(private_pem)
            .map_err(|e| CasError::Store(format!("JWT encoding key: {e}")))?;
        let decoding_key = DecodingKey::from_ed_pem(public_pem)
            .map_err(|e| CasError::Store(format!("JWT decoding key: {e}")))?;
        Ok(Self {
            kid: kid.into(),
            encoding_key,
            decoding_key,
            raw_public_key,
        })
    }

    /// Return this key's public component as a [`Jwk`].
    pub fn as_jwk(&self) -> Jwk {
        use base64::Engine;
        let x = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.raw_public_key);
        Jwk {
            kty: "OKP".into(),
            crv: "Ed25519".into(),
            x,
            kid: self.kid.clone(),
            use_: "sig".into(),
            alg: "EdDSA".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// JwtService
// ---------------------------------------------------------------------------

/// Issues and verifies JWTs, and manages revocable refresh tokens in FDB.
///
/// Thread-safe; wrap in `Arc<JwtService>` to share across axum handlers.
pub struct JwtService {
    /// All registered key pairs (active + retired for rotation).
    keys: Vec<JwtKeyPair>,
    /// Index into `keys` for the current signing key.
    active_idx: usize,
    /// FDB database for refresh-token storage.
    db: Arc<Database>,
}

impl JwtService {
    /// Create a service with the given key pairs.
    ///
    /// `active_idx` selects the signing key.  All keys are used for
    /// verification, enabling zero-downtime rotation.
    pub fn new(keys: Vec<JwtKeyPair>, active_idx: usize, db: Arc<Database>) -> Self {
        assert!(!keys.is_empty(), "JwtService: keys must not be empty");
        assert!(active_idx < keys.len(), "JwtService: active_idx out of range");
        Self { keys, active_idx, db }
    }

    /// Convenience: generate one fresh key pair and build a `JwtService`.
    pub fn with_generated_key(db: Arc<Database>) -> CasResult<Self> {
        let kp = JwtKeyPair::generate()?;
        Ok(Self::new(vec![kp], 0, db))
    }

    // -----------------------------------------------------------------------
    // Access tokens
    // -----------------------------------------------------------------------

    /// Sign and return a new access token.
    pub fn issue_access_token(
        &self,
        user_id: &str,
        tenant_id: &str,
        org_id: &str,
        roles: Vec<String>,
    ) -> CasResult<String> {
        let now = unix_now();
        let claims = AccessClaims {
            sub: user_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            org_id: org_id.to_owned(),
            roles,
            iat: now,
            exp: now + ACCESS_TTL_SECS,
            jti: Uuid::new_v4().to_string(),
        };
        let active = &self.keys[self.active_idx];
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(active.kid.clone());
        encode(&header, &claims, &active.encoding_key)
            .map_err(|e| CasError::Store(format!("JWT access encode: {e}")))
    }

    /// Verify an access token and return its claims.
    ///
    /// Tries keys in order.  Returns [`CasError::Unauthorized`] if the token
    /// is expired or cannot be verified by any registered key.
    pub fn verify_access_token(&self, token: &str) -> CasResult<AccessClaims> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = true;

        for kp in &self.keys {
            match decode::<AccessClaims>(token, &kp.decoding_key, &validation) {
                Ok(data) => return Ok(data.claims),
                Err(e) => {
                    use jsonwebtoken::errors::ErrorKind;
                    match e.kind() {
                        // Right key but token expired — fail fast.
                        ErrorKind::ExpiredSignature => {
                            return Err(CasError::Unauthorized("token expired".into()));
                        }
                        // Wrong key — try the next one.
                        ErrorKind::InvalidSignature => continue,
                        // Any other error (malformed, etc.) — try next key.
                        _ => continue,
                    }
                }
            }
        }
        Err(CasError::Unauthorized("invalid token".into()))
    }

    // -----------------------------------------------------------------------
    // Refresh tokens
    // -----------------------------------------------------------------------

    /// Issue a refresh token and persist its record in FDB.
    pub fn issue_refresh_token(&self, user_id: &str) -> CasResult<String> {
        let now = unix_now();
        let jti = Uuid::new_v4().to_string();
        let exp = now + REFRESH_TTL_SECS;
        let claims = RefreshClaims {
            sub: user_id.to_owned(),
            jti: jti.clone(),
            iat: now,
            exp,
        };

        let active = &self.keys[self.active_idx];
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(active.kid.clone());
        let token = encode(&header, &claims, &active.encoding_key)
            .map_err(|e| CasError::Store(format!("JWT refresh encode: {e}")))?;

        // Persist in FDB.
        let record = RefreshRecord { user_id: user_id.to_owned(), exp };
        let record_bytes = serde_json::to_vec(&record).expect("serialize");
        let key = rt_key(&jti);
        let db = Arc::clone(&self.db);
        Self::rt()?.block_on(db.run(|trx, _| {
            let key = key.clone();
            let bytes = record_bytes.clone();
            async move {
                trx.set(&key, &bytes);
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB refresh store: {e}")))?;

        Ok(token)
    }

    /// Verify a refresh token and **atomically consume** it (single-use).
    ///
    /// Deletes the FDB record on success; returns the `user_id`.
    /// Returns [`CasError::Unauthorized`] if expired, revoked, or invalid.
    pub fn verify_and_consume_refresh_token(&self, token: &str) -> CasResult<String> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = true;

        let claims = self.decode_refresh_claims(token, &validation)?;
        let key = rt_key(&claims.jti);

        // Read the FDB record.
        let record_bytes = Self::fdb_get(Arc::clone(&self.db), key.clone())?
            .ok_or_else(|| CasError::Unauthorized("refresh token revoked or not found".into()))?;
        let record: RefreshRecord = serde_json::from_slice(&record_bytes)?;

        // Double-check server-side expiry.
        if unix_now() > record.exp {
            // Lazily clean up the expired record.
            let _ = self.revoke_refresh_token(&claims.jti);
            return Err(CasError::Unauthorized("refresh token expired".into()));
        }

        // Consume: delete from FDB.
        let db = Arc::clone(&self.db);
        Self::rt()?.block_on(db.run(|trx, _| {
            let key = key.clone();
            async move {
                trx.clear(&key);
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB refresh consume: {e}")))?;

        Ok(record.user_id)
    }

    /// Revoke a refresh token by its JTI (e.g. on explicit logout).
    pub fn revoke_refresh_token(&self, jti: &str) -> CasResult<()> {
        let key = rt_key(jti);
        let db = Arc::clone(&self.db);
        Self::rt()?.block_on(db.run(|trx, _| {
            let key = key.clone();
            async move {
                trx.clear(&key);
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB revoke: {e}")))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // JWKS
    // -----------------------------------------------------------------------

    /// Return the public key set (all registered keys) for the JWKS endpoint.
    pub fn jwks(&self) -> JwkSet {
        JwkSet {
            keys: self.keys.iter().map(JwtKeyPair::as_jwk).collect(),
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn decode_refresh_claims(&self, token: &str, validation: &Validation) -> CasResult<RefreshClaims> {
        for kp in &self.keys {
            match decode::<RefreshClaims>(token, &kp.decoding_key, validation) {
                Ok(data) => return Ok(data.claims),
                Err(e) => {
                    use jsonwebtoken::errors::ErrorKind;
                    match e.kind() {
                        ErrorKind::ExpiredSignature => {
                            return Err(CasError::Unauthorized("refresh token expired".into()));
                        }
                        ErrorKind::InvalidSignature => continue,
                        _ => continue,
                    }
                }
            }
        }
        Err(CasError::Unauthorized("invalid refresh token".into()))
    }

    fn fdb_get(db: Arc<Database>, key: Vec<u8>) -> CasResult<Option<Vec<u8>>> {
        Self::rt()?.block_on(async move {
            let trx = db
                .create_trx()
                .map_err(|e| CasError::Store(format!("FDB create_trx: {e}")))?;
            let v = trx
                .get(&key, false)
                .await
                .map_err(|e| CasError::Store(format!("FDB get: {e}")))?;
            Ok(v.map(|b| b.to_vec()))
        })
    }

    fn rt() -> CasResult<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current()
            .map_err(|e| CasError::Store(format!("JwtService requires a Tokio runtime: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Current Unix timestamp in seconds.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs()
}

/// FDB key for a refresh-token record.
fn rt_key(jti: &str) -> Vec<u8> {
    format!("{RT_PREFIX}{jti}").into_bytes()
}

/// Wrap DER-encoded bytes in a PEM block using standard base64 (RFC 7468).
fn der_to_pem(label: &str, der: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let b64 = STANDARD.encode(der);
    let body = b64
        .as_bytes()
        .chunks(64)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
}

/// Build a SubjectPublicKeyInfo (SPKI) DER blob for an Ed25519 public key.
///
/// The encoding is fixed-length (44 bytes):
/// ```text
/// SEQUENCE {
///   SEQUENCE { OID 1.3.101.112 }     -- AlgorithmIdentifier for Ed25519
///   BIT STRING { 0x00 || raw_key }   -- 32-byte public key
/// }
/// ```
fn ed25519_spki_der(raw_public_key: &[u8; 32]) -> Vec<u8> {
    let mut der = Vec::with_capacity(44);
    der.extend_from_slice(&[
        0x30, 0x2a,              // SEQUENCE, 42 bytes
        0x30, 0x05,              // SEQUENCE (AlgorithmIdentifier), 5 bytes
        0x06, 0x03, 0x2b, 0x65, 0x70, // OID 1.3.101.112 (id-EdDSA / Ed25519)
        0x03, 0x21,              // BIT STRING, 33 bytes
        0x00,                    // unused bits = 0
    ]);
    der.extend_from_slice(raw_public_key);
    der
}
