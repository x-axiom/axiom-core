//! Pagination limits (P2-3).
//!
//! Every `limit` query parameter is clamped to `MAX_LIMIT` so a client cannot
//! ask the server for an unbounded result set (memory + latency amplification).

/// Hard ceiling for any list endpoint.
pub const MAX_LIMIT: usize = 100;

/// Default page size when the client omits `limit`.
pub const DEFAULT_LIMIT: usize = 50;

/// Clamp a requested limit into `1..=MAX_LIMIT`.
pub fn clamp_limit(requested: usize) -> usize {
    requested.clamp(1, MAX_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_zero_and_huge_values() {
        assert_eq!(clamp_limit(0), 1);
        assert_eq!(clamp_limit(50), 50);
        assert_eq!(clamp_limit(MAX_LIMIT), MAX_LIMIT);
        assert_eq!(clamp_limit(100_000), MAX_LIMIT);
    }
}
