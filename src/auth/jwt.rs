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

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::refresh::{RefreshRecord, RefreshTokenStore};
use crate::error::{CasError, CasResult};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Access token time-to-live (15 minutes).
pub const ACCESS_TTL_SECS: u64 = 15 * 60;
/// Refresh token time-to-live (7 days).
pub const REFRESH_TTL_SECS: u64 = 7 * 24 * 3600;

/// OAuth `state` token time-to-live (10 minutes).
pub const STATE_TTL_SECS: u64 = 10 * 60;

/// Invitation link time-to-live (7 days).
pub const INVITATION_TTL_SECS: u64 = 7 * 24 * 3600;

/// FDB key prefix for refresh-token records (kept for compatibility with
/// records written before the store abstraction was introduced).
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
    /// Billing plan (`"free"` / `"pro"` / `"enterprise"`). Drives quota and
    /// rate-limit tiers; absent in legacy tokens → treated as Free.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
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

/// Claims embedded in an invitation token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvitationClaims {
    pub org_id: String,
    pub email: String,
    pub role: String,
    pub inviter: String,
    pub exp: u64,
    pub jti: String,
}

/// Claims embedded in a short-lived OAuth `state` token.
#[derive(Debug, Serialize, Deserialize)]
struct StateClaims {
    nonce: String,
    iat: u64,
    exp: u64,
}

// ---------------------------------------------------------------------------
// Refresh-token record
// ---------------------------------------------------------------------------

// `RefreshRecord` lives in `auth::refresh`; the type is imported at the top of
// this module and used directly.

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

    /// Load a key pair from PEM **files** on disk.
    ///
    /// The raw public key required for JWKS export is recovered from the SPKI
    /// DER structure (last 32 bytes of `PUBLIC KEY`).
    pub fn from_pem_files(
        kid: impl Into<String>,
        private_path: &str,
        public_path: &str,
    ) -> CasResult<Self> {
        let private_pem = std::fs::read(private_path).map_err(|e| {
            CasError::Store(format!("read JWT private key '{private_path}': {e}"))
        })?;
        let public_pem = std::fs::read(public_path).map_err(|e| {
            CasError::Store(format!("read JWT public key '{public_path}': {e}"))
        })?;
        let raw = raw_public_key_from_pem(&public_pem)?;
        Self::from_pem(kid, &private_pem, &public_pem, raw)
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

/// Issues and verifies JWTs, and manages revocable refresh tokens.
///
/// Thread-safe; wrap in `Arc<JwtService>` to share across axum handlers.
pub struct JwtService {
    /// All registered key pairs (active + retired for rotation).
    keys: Vec<JwtKeyPair>,
    /// Index into `keys` for the current signing key.
    active_idx: usize,
    /// Storage for revocable refresh-token records.
    store: Arc<dyn RefreshTokenStore>,
}

impl JwtService {
    /// Create a service with the given key pairs.
    ///
    /// `active_idx` selects the signing key.  All keys are used for
    /// verification, enabling zero-downtime rotation.
    pub fn new(
        keys: Vec<JwtKeyPair>,
        active_idx: usize,
        store: Arc<dyn RefreshTokenStore>,
    ) -> Self {
        assert!(!keys.is_empty(), "JwtService: keys must not be empty");
        assert!(active_idx < keys.len(), "JwtService: active_idx out of range");
        Self { keys, active_idx, store }
    }

    /// Convenience: generate one fresh key pair with an in-memory refresh store.
    ///
    /// Intended for local single-user mode and tests.
    pub fn with_memory_store() -> CasResult<Self> {
        let kp = JwtKeyPair::generate()?;
        Ok(Self::new(
            vec![kp],
            0,
            Arc::new(crate::auth::refresh::MemoryRefreshTokenStore::new()),
        ))
    }

    /// Convenience: generate one fresh key pair backed by FoundationDB
    /// refresh-token storage.
    #[cfg(feature = "fdb")]
    pub fn with_generated_key(db: Arc<foundationdb::Database>) -> CasResult<Self> {
        let kp = JwtKeyPair::generate()?;
        Ok(Self::new(
            vec![kp],
            0,
            Arc::new(crate::auth::refresh::FdbRefreshTokenStore::new(db)),
        ))
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
        self.issue_access_token_with_plan(user_id, tenant_id, org_id, roles, None)
    }

    /// Sign an access token that also carries the tenant's billing plan.
    pub fn issue_access_token_with_plan(
        &self,
        user_id: &str,
        tenant_id: &str,
        org_id: &str,
        roles: Vec<String>,
        plan: Option<&str>,
    ) -> CasResult<String> {
        let now = unix_now();
        let claims = AccessClaims {
            sub: user_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            org_id: org_id.to_owned(),
            roles,
            plan: plan.map(str::to_owned),
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
                        // Right key but token expired — fail fast and let the
                        // caller map this to `token_expired` (401).
                        ErrorKind::ExpiredSignature => {
                            return Err(CasError::TokenExpired("access token expired".into()));
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

        // Persist the revocation record.
        let record = RefreshRecord { user_id: user_id.to_owned(), exp };
        self.store.put(&jti, record)?;

        Ok(token)
    }

    /// Verify a refresh token and **atomically consume** it (single-use).
    ///
    /// Deletes the stored record on success; returns the `user_id`.
    /// Returns [`CasError::Unauthorized`] if expired, revoked, or invalid.
    pub fn verify_and_consume_refresh_token(&self, token: &str) -> CasResult<String> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = true;

        let claims = self.decode_refresh_claims(token, &validation)?;

        let record = self
            .store
            .take(&claims.jti)?
            .ok_or_else(|| CasError::Unauthorized("refresh token revoked or not found".into()))?;

        // Double-check server-side expiry.
        if unix_now() > record.exp {
            return Err(CasError::TokenExpired("refresh token expired".into()));
        }

        Ok(record.user_id)
    }

    /// Revoke a refresh token by its JTI (e.g. on explicit logout).
    pub fn revoke_refresh_token(&self, jti: &str) -> CasResult<()> {
        self.store.revoke(jti)
    }

    /// Extract the JTI of a refresh token without consuming it.
    ///
    /// Used by logout so a caller can present an already-expired access token
    /// alongside a refresh token and still get deterministic revocation.
    pub fn refresh_token_jti(&self, token: &str) -> CasResult<String> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        // Logout must work even for recently-expired refresh tokens.
        validation.validate_exp = false;
        let claims = self.decode_refresh_claims(token, &validation)?;
        Ok(claims.jti)
    }

    // -----------------------------------------------------------------------
    // Invitation tokens
    // -----------------------------------------------------------------------

    /// Sign an invitation for `email` to join `org_id` with `role`.
    ///
    /// Returns the token and its absolute expiry. Tokens are self-contained so
    /// accepting an invite needs no server-side session table.
    pub fn issue_invitation(
        &self,
        org_id: &str,
        email: &str,
        role: &str,
        inviter: &str,
    ) -> CasResult<(String, u64)> {
        let now = unix_now();
        let exp = now + INVITATION_TTL_SECS;
        let claims = InvitationClaims {
            org_id: org_id.to_owned(),
            email: email.trim().to_ascii_lowercase(),
            role: role.to_owned(),
            inviter: inviter.to_owned(),
            exp,
            jti: Uuid::new_v4().to_string(),
        };
        let active = &self.keys[self.active_idx];
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(active.kid.clone());
        let token = encode(&header, &claims, &active.encoding_key)
            .map_err(|e| CasError::Store(format!("JWT invitation encode: {e}")))?;
        Ok((token, exp))
    }

    /// Verify an invitation token.
    pub fn verify_invitation(&self, token: &str) -> CasResult<InvitationClaims> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = true;
        for kp in &self.keys {
            match decode::<InvitationClaims>(token, &kp.decoding_key, &validation) {
                Ok(data) => return Ok(data.claims),
                Err(e) => {
                    use jsonwebtoken::errors::ErrorKind;
                    if matches!(e.kind(), ErrorKind::ExpiredSignature) {
                        return Err(CasError::TokenExpired("invitation expired".into()));
                    }
                    continue;
                }
            }
        }
        Err(CasError::Unauthorized("invalid invitation".into()))
    }

    // -----------------------------------------------------------------------
    // OAuth state tokens
    // -----------------------------------------------------------------------

    /// Sign a short-lived OAuth `state` value (CSRF protection without
    /// server-side session storage).
    pub fn issue_state_token(&self, nonce: &str) -> CasResult<String> {
        let now = unix_now();
        let claims = StateClaims {
            nonce: nonce.to_owned(),
            iat: now,
            exp: now + STATE_TTL_SECS,
        };
        let active = &self.keys[self.active_idx];
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(active.kid.clone());
        encode(&header, &claims, &active.encoding_key)
            .map_err(|e| CasError::Store(format!("JWT state encode: {e}")))
    }

    /// Verify an OAuth `state` value produced by [`issue_state_token`].
    pub fn verify_state_token(&self, token: &str) -> CasResult<()> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = true;
        for kp in &self.keys {
            match decode::<StateClaims>(token, &kp.decoding_key, &validation) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    use jsonwebtoken::errors::ErrorKind;
                    if matches!(e.kind(), ErrorKind::ExpiredSignature) {
                        return Err(CasError::TokenExpired("oauth state expired".into()));
                    }
                    continue;
                }
            }
        }
        Err(CasError::Unauthorized("invalid oauth state".into()))
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

    // -----------------------------------------------------------------------
    // Test-only helpers
    // -----------------------------------------------------------------------

    /// Issue an access token with an explicit expiry timestamp.
    ///
    /// Used in tests to create already-expired tokens without sleeping.
    pub fn issue_access_token_with_exp(
        &self,
        user_id: &str,
        tenant_id: &str,
        org_id: &str,
        roles: Vec<String>,
        exp: u64,
    ) -> CasResult<String> {
        let now = unix_now();
        let claims = AccessClaims {
            sub: user_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            org_id: org_id.to_owned(),
            roles,
            plan: None,
            iat: now,
            exp,
            jti: Uuid::new_v4().to_string(),
        };
        let active = &self.keys[self.active_idx];
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(active.kid.clone());
        encode(&header, &claims, &active.encoding_key)
            .map_err(|e| CasError::Store(format!("JWT test encode: {e}")))
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

/// FDB key for a refresh-token record (legacy layout, retained for reference).
#[allow(dead_code)]
fn rt_key(jti: &str) -> Vec<u8> {
    format!("{RT_PREFIX}{jti}").into_bytes()
}

/// Recover the raw 32-byte Ed25519 public key from a `PUBLIC KEY` PEM (SPKI).
fn raw_public_key_from_pem(pem: &[u8]) -> CasResult<[u8; 32]> {
    use base64::{Engine, engine::general_purpose::STANDARD};

    let text = std::str::from_utf8(pem)
        .map_err(|e| CasError::Store(format!("JWT public key is not UTF-8: {e}")))?;
    let body: String = text
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<Vec<_>>()
        .join("");
    let der = STANDARD
        .decode(body.trim())
        .map_err(|e| CasError::Store(format!("JWT public key base64: {e}")))?;

    if der.len() < 32 {
        return Err(CasError::Store(format!(
            "JWT public key too short ({} bytes)",
            der.len()
        )));
    }

    let mut raw = [0u8; 32];
    raw.copy_from_slice(&der[der.len() - 32..]);
    Ok(raw)
}

/// Wrap DER-encoded bytes in a PEM block using standard base64 (RFC 7468).
fn der_to_pem(label: &str, der: &[u8]) -> String {    use base64::{Engine, engine::general_purpose::STANDARD};
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
