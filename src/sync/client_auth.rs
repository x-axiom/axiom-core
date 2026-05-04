use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};

use crate::error::{CasError, CasResult};

#[derive(Clone, Default)]
pub(crate) struct ClientAuth {
    header: Option<MetadataValue<Ascii>>,
}

impl ClientAuth {
    pub(crate) fn from_token(token: Option<&str>) -> CasResult<Self> {
        let header = match token.map(str::trim).filter(|token| !token.is_empty()) {
            Some(token) => Some(
                MetadataValue::try_from(format!("Bearer {token}"))
                    .map_err(|e| CasError::SyncError(format!("authorization metadata: {e}")))?,
            ),
            None => None,
        };
        Ok(Self { header })
    }

    pub(crate) fn apply<T>(&self, mut request: Request<T>) -> Request<T> {
        if let Some(header) = &self.header {
            request.metadata_mut().insert("authorization", header.clone());
        }
        request
    }
}

#[cfg(test)]
mod tests {
    use super::ClientAuth;
    use tonic::Request;

    #[test]
    fn applies_bearer_header() {
        let auth = ClientAuth::from_token(Some("secret-token")).unwrap();
        let request = auth.apply(Request::new(()));
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer secret-token"
        );
    }

    #[test]
    fn empty_token_skips_header() {
        let auth = ClientAuth::from_token(Some("   ")).unwrap();
        let request = auth.apply(Request::new(()));
        assert!(request.metadata().get("authorization").is_none());
    }
}