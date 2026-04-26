//! GC task scheduler with FDB distributed lock and Prometheus metrics (E11-S05).
//!
//! # Architecture
//!
//! ```text
//!  ┌─────────────────────────────────────────────────────┐
//!  │  GcScheduler                                        │
//!  │                                                     │
//!  │   run_once(GcType, tid, wid)                        │
//!  │     ├─ FdbGcLock::try_acquire()  ─ FDB CAS lock     │
//!  │     ├─ GcSweeper::run_sweep()   ─ mark-sweep        │
//!  │     ├─ GcMetrics::record()      ─ Prometheus        │
//!  │     └─ FdbGcLock::release()                         │
//!  │                                                     │
//!  │   start_cron_task()  ─ tokio task, daily at 3 AM    │
//!  │   trigger_incremental()  ─ call after bulk deletes  │
//!  └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Distributed lock
//!
//! The FDB key `gc_lock\x00{tenant}\x00{workspace}` stores an 8-byte LE u64
//! expiry timestamp followed by the holder ID string (UTF-8).  `try_acquire`
//! uses an FDB read-modify-write transaction so the check-and-set is atomic.
//! The lock TTL (default 1 h) prevents a crashed holder from blocking forever.
//!
//! # Prometheus metrics
//!
//! All metrics use a caller-supplied [`Registry`] so tests can inspect them
//! without polluting the global registry.  The helper [`GcMetrics::new_default`]
//! registers with Prometheus' default registry for production use.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use foundationdb::{Database, FdbError};
use prometheus::{Counter, CounterVec, Histogram, HistogramOpts, Opts, Registry};
use tokio::task::JoinHandle;

use crate::error::{CasError, CasResult};
use crate::model::hash::current_timestamp;
use super::sweep::{GcReport, GcSweeper};

// ---------------------------------------------------------------------------
// GcType
// ---------------------------------------------------------------------------

/// Whether this GC run is a scheduled full sweep or an incremental trigger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcType {
    /// Daily full mark-sweep of all workspaces (cron).
    Full,
    /// Workspace-scoped incremental GC triggered after large deletes.
    Incremental,
}

impl fmt::Display for GcType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GcType::Full => f.write_str("full"),
            GcType::Incremental => f.write_str("incremental"),
        }
    }
}

// ---------------------------------------------------------------------------
// FdbGcLock — distributed lock backed by FoundationDB
// ---------------------------------------------------------------------------

/// FDB-backed distributed lock for the GC scheduler.
///
/// Guarantees that at most one GC instance is running for a given
/// `(tenant_id, workspace_id)` at any moment.
///
/// ## Key layout
///
/// ```text
/// gc_lock\x00{tenant_id}\x00{workspace_id}
///   → [expires_at: u64 LE (8 bytes)] [holder_id: UTF-8 string]
/// ```
pub struct FdbGcLock {
    db: Arc<Database>,
}

impl FdbGcLock {
    /// Wrap an existing FDB `Database`.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Attempt to acquire the GC lock for `(tenant_id, workspace_id)`.
    ///
    /// Returns `true` if the lock was acquired, `false` if it is held by
    /// another instance and has not yet expired.
    ///
    /// The lock expires automatically after `ttl_secs` seconds; a crashed
    /// holder therefore unblocks other instances within one TTL window.
    pub fn try_acquire(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        holder_id: &str,
        ttl_secs: u64,
    ) -> CasResult<bool> {
        let lock_key = Self::lock_key(tenant_id, workspace_id);
        let holder_bytes = holder_id.as_bytes().to_vec();
        let now = current_timestamp();
        let expires_at = now.saturating_add(ttl_secs);

        let rt = Self::rt()?;
        let acquired = rt
            .block_on(self.db.run(|trx, _| {
                let lock_key = lock_key.clone();
                let holder_bytes = holder_bytes.clone();
                async move {
                    let current = trx
                        .get(&lock_key, false)
                        .await?;

                    let should_acquire = match current.as_deref() {
                        None => true, // no lock — free to acquire
                        Some(bytes) if bytes.len() >= 8 => {
                            let current_expiry =
                                u64::from_le_bytes(bytes[..8].try_into().unwrap());
                            current_expiry <= now // lock expired
                        }
                        _ => false, // malformed value — treat as locked
                    };

                    if should_acquire {
                        let mut val = expires_at.to_le_bytes().to_vec();
                        val.extend_from_slice(&holder_bytes);
                        trx.set(&lock_key, &val);
                    }

                    Ok(should_acquire)
                }
            }))
            .map_err(|e: FdbError| CasError::Store(format!("FDB gc_lock try_acquire: {e}")))?;

        Ok(acquired)
    }

    /// Release the lock if it is currently held by `holder_id`.
    ///
    /// No-op if the lock is not held by this holder (e.g. already expired and
    /// taken by another instance).
    pub fn release(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        holder_id: &str,
    ) -> CasResult<()> {
        let lock_key = Self::lock_key(tenant_id, workspace_id);
        let holder_bytes = holder_id.as_bytes().to_vec();

        let rt = Self::rt()?;
        rt.block_on(self.db.run(|trx, _| {
            let lock_key = lock_key.clone();
            let holder_bytes = holder_bytes.clone();
            async move {
                let current = trx.get(&lock_key, false).await?;
                if let Some(bytes) = current.as_deref() {
                    if bytes.len() > 8 && &bytes[8..] == holder_bytes.as_slice() {
                        trx.clear(&lock_key);
                    }
                }
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB gc_lock release: {e}")))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Key builder (pub(crate) for testing)
    // -----------------------------------------------------------------------

    pub(crate) fn lock_key(tenant_id: &str, workspace_id: &str) -> Vec<u8> {
        format!("gc_lock\x00{tenant_id}\x00{workspace_id}").into_bytes()
    }

    fn rt() -> CasResult<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current()
            .map_err(|e| CasError::Store(format!("FdbGcLock requires a Tokio runtime: {e}")))
    }
}

// ---------------------------------------------------------------------------
// GcMetrics — Prometheus counters and histograms
// ---------------------------------------------------------------------------

/// Prometheus metrics recorded per GC run.
///
/// Pass a [`Registry`] at construction time; use [`GcMetrics::new_default`]
/// to register with Prometheus' global default registry.
///
/// # Metrics
///
/// | Name | Type | Labels | Description |
/// |------|------|--------|-------------|
/// | `axiom_gc_runs_total` | Counter | `type` (full/incremental) | Total GC runs |
/// | `axiom_gc_objects_deleted_total` | Counter | — | Objects promoted to pending_delete |
/// | `axiom_gc_bytes_reclaimed_total` | Counter | — | Bytes reclaimed (objects × avg chunk size estimate) |
/// | `axiom_gc_duration_seconds` | Histogram | — | Wall-clock time of each GC run |
pub struct GcMetrics {
    /// `axiom_gc_runs_total{type="full|incremental"}`
    pub runs_total: CounterVec,
    /// `axiom_gc_objects_deleted_total`
    pub objects_deleted_total: Counter,
    /// `axiom_gc_bytes_reclaimed_total`
    pub bytes_reclaimed_total: Counter,
    /// `axiom_gc_duration_seconds`
    pub duration_seconds: Histogram,
}

impl GcMetrics {
    /// Create and register all metrics with the supplied `registry`.
    pub fn new(registry: &Registry) -> CasResult<Self> {
        let runs_total = CounterVec::new(
            Opts::new("axiom_gc_runs_total", "Total GC runs by type"),
            &["type"],
        )
        .map_err(|e| CasError::Store(format!("prometheus register axiom_gc_runs_total: {e}")))?;

        let objects_deleted_total = Counter::new(
            "axiom_gc_objects_deleted_total",
            "Total objects promoted to pending_delete across all GC runs",
        )
        .map_err(|e| {
            CasError::Store(format!(
                "prometheus register axiom_gc_objects_deleted_total: {e}"
            ))
        })?;

        let bytes_reclaimed_total = Counter::new(
            "axiom_gc_bytes_reclaimed_total",
            "Estimated bytes reclaimed (objects_deleted × average_chunk_size)",
        )
        .map_err(|e| {
            CasError::Store(format!(
                "prometheus register axiom_gc_bytes_reclaimed_total: {e}"
            ))
        })?;

        let duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "axiom_gc_duration_seconds",
                "Wall-clock time of each GC run in seconds",
            )
            .buckets(prometheus::exponential_buckets(0.1, 2.0, 10).map_err(|e| {
                CasError::Store(format!("prometheus bucket spec: {e}"))
            })?),
        )
        .map_err(|e| {
            CasError::Store(format!(
                "prometheus register axiom_gc_duration_seconds: {e}"
            ))
        })?;

        registry
            .register(Box::new(runs_total.clone()))
            .map_err(|e| CasError::Store(format!("prometheus register runs_total: {e}")))?;
        registry
            .register(Box::new(objects_deleted_total.clone()))
            .map_err(|e| {
                CasError::Store(format!("prometheus register objects_deleted_total: {e}"))
            })?;
        registry
            .register(Box::new(bytes_reclaimed_total.clone()))
            .map_err(|e| {
                CasError::Store(format!("prometheus register bytes_reclaimed_total: {e}"))
            })?;
        registry
            .register(Box::new(duration_seconds.clone()))
            .map_err(|e| {
                CasError::Store(format!("prometheus register duration_seconds: {e}"))
            })?;

        Ok(Self {
            runs_total,
            objects_deleted_total,
            bytes_reclaimed_total,
            duration_seconds,
        })
    }

    /// Register with Prometheus' global default registry.
    ///
    /// Call once at application startup.
    pub fn new_default() -> CasResult<Self> {
        Self::new(prometheus::default_registry())
    }

    /// Record the outcome of one GC run.
    ///
    /// `bytes_per_object` is a caller-supplied estimate (e.g. average chunk
    /// size in bytes).  Pass `0` to skip the bytes counter.
    pub fn record(
        &self,
        gc_type: &GcType,
        report: &GcReport,
        elapsed_secs: f64,
        bytes_per_object: u64,
    ) {
        self.runs_total
            .with_label_values(&[&gc_type.to_string()])
            .inc();
        self.objects_deleted_total
            .inc_by(report.pending_delete_enqueued as f64);
        if bytes_per_object > 0 {
            self.bytes_reclaimed_total
                .inc_by((report.pending_delete_enqueued as u64 * bytes_per_object) as f64);
        }
        self.duration_seconds.observe(elapsed_secs);
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig
// ---------------------------------------------------------------------------

/// Configuration for the GC scheduler.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    /// UTC hour of day at which the daily full GC is triggered (0–23).
    /// Default: `3` (3:00 AM UTC).
    pub full_gc_hour_utc: u32,
    /// How long (seconds) the distributed lock is held before it expires.
    /// Default: `3600` (1 hour).
    pub lock_ttl_secs: u64,
    /// Unique identifier for this scheduler instance.  Used as the lock
    /// holder ID so the lock can be released by the correct holder on
    /// shutdown.  Defaults to a fresh UUIDv4.
    pub holder_id: String,
    /// Estimated average bytes per object — used only for the
    /// `axiom_gc_bytes_reclaimed_total` metric approximation.
    /// Default: `0` (disables the bytes counter).
    pub bytes_per_object_estimate: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            full_gc_hour_utc: 3,
            lock_ttl_secs: 3600,
            holder_id: uuid::Uuid::new_v4().to_string(),
            bytes_per_object_estimate: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// GcScheduler
// ---------------------------------------------------------------------------

/// Orchestrates GC runs: acquires the distributed FDB lock, invokes
/// [`GcSweeper`], and records Prometheus metrics.
///
/// # Typical setup
///
/// ```ignore
/// let scheduler = Arc::new(GcScheduler::new(sweeper, lock, metrics, config));
///
/// // Background cron (runs until the handle is dropped):
/// let _handle = scheduler.clone().start_cron_task("tenant-1".into(), "ws-1".into()).await;
///
/// // Manual incremental trigger:
/// scheduler.trigger_incremental("tenant-1", "ws-1")?;
/// ```
pub struct GcScheduler {
    sweeper: Arc<GcSweeper>,
    lock: Arc<FdbGcLock>,
    metrics: Arc<GcMetrics>,
    config: SchedulerConfig,
}

impl GcScheduler {
    /// Create a new scheduler.
    pub fn new(
        sweeper: Arc<GcSweeper>,
        lock: Arc<FdbGcLock>,
        metrics: Arc<GcMetrics>,
        config: SchedulerConfig,
    ) -> Self {
        Self { sweeper, lock, metrics, config }
    }

    // -----------------------------------------------------------------------
    // Core run logic
    // -----------------------------------------------------------------------

    /// Run one GC cycle — acquires the FDB lock, sweeps, records metrics.
    ///
    /// Returns `Ok(None)` if the lock could not be acquired (another instance
    /// is already running GC for this workspace).
    /// Returns `Ok(Some(report))` on success.
    pub fn run_once(
        &self,
        gc_type: GcType,
        tenant_id: &str,
        workspace_id: &str,
    ) -> CasResult<Option<GcReport>> {
        // ── Acquire distributed lock ─────────────────────────────────────
        let acquired = self.lock.try_acquire(
            tenant_id,
            workspace_id,
            &self.config.holder_id,
            self.config.lock_ttl_secs,
        )?;

        if !acquired {
            tracing::warn!(
                tenant_id,
                workspace_id,
                gc_type = %gc_type,
                "GC skipped: lock is held by another instance"
            );
            return Ok(None);
        }

        tracing::info!(
            tenant_id,
            workspace_id,
            gc_type = %gc_type,
            holder = %self.config.holder_id,
            "GC started"
        );

        let t0 = Instant::now();
        let result = self.sweeper.run_sweep(tenant_id, workspace_id);
        let elapsed = t0.elapsed().as_secs_f64();

        // ── Release lock regardless of sweep outcome ──────────────────────
        if let Err(e) = self.lock.release(tenant_id, workspace_id, &self.config.holder_id) {
            tracing::error!(
                error = %e,
                tenant_id,
                workspace_id,
                "GC: failed to release FDB lock — will expire after TTL"
            );
        }

        // ── Record metrics and propagate error ────────────────────────────
        match result {
            Ok(report) => {
                self.metrics.record(
                    &gc_type,
                    &report,
                    elapsed,
                    self.config.bytes_per_object_estimate,
                );
                tracing::info!(
                    tenant_id,
                    workspace_id,
                    gc_type = %gc_type,
                    elapsed_secs = elapsed,
                    pending_delete_enqueued = report.pending_delete_enqueued,
                    "GC completed"
                );
                Ok(Some(report))
            }
            Err(e) => {
                // Still record a run so operators can see failure spikes.
                self.metrics.runs_total
                    .with_label_values(&[&gc_type.to_string()])
                    .inc();
                self.metrics.duration_seconds.observe(elapsed);
                tracing::error!(
                    tenant_id,
                    workspace_id,
                    gc_type = %gc_type,
                    error = %e,
                    "GC failed"
                );
                Err(e)
            }
        }
    }

    /// Convenience wrapper: trigger an incremental GC for one workspace.
    ///
    /// Call this after a large batch of version deletions to reclaim space
    /// promptly rather than waiting for the next scheduled full GC.
    pub fn trigger_incremental(
        &self,
        tenant_id: &str,
        workspace_id: &str,
    ) -> CasResult<Option<GcReport>> {
        self.run_once(GcType::Incremental, tenant_id, workspace_id)
    }

    // -----------------------------------------------------------------------
    // Cron scheduling
    // -----------------------------------------------------------------------

    /// Start the background daily full-GC cron task.
    ///
    /// Spawns a Tokio task that sleeps until the next `full_gc_hour_utc:00:00`
    /// UTC, runs a full GC sweep, then sleeps until the same time tomorrow.
    /// The task runs indefinitely; abort the returned `JoinHandle` to stop it.
    pub fn start_cron_task(
        self: Arc<Self>,
        tenant_id: String,
        workspace_id: String,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let now = current_timestamp();
                let next = Self::next_run_at_secs(self.config.full_gc_hour_utc, now);
                let sleep_secs = next.saturating_sub(now);

                tracing::info!(
                    tenant_id = %tenant_id,
                    workspace_id = %workspace_id,
                    sleep_secs,
                    next_run_at = next,
                    "GC cron: next full GC scheduled"
                );

                tokio::time::sleep(Duration::from_secs(sleep_secs)).await;

                match self.run_once(GcType::Full, &tenant_id, &workspace_id) {
                    Ok(Some(report)) => tracing::info!(
                        tenant_id = %tenant_id,
                        workspace_id = %workspace_id,
                        pending_delete_enqueued = report.pending_delete_enqueued,
                        "GC cron: full GC completed"
                    ),
                    Ok(None) => tracing::warn!(
                        tenant_id = %tenant_id,
                        workspace_id = %workspace_id,
                        "GC cron: skipped (lock held)"
                    ),
                    Err(e) => tracing::error!(
                        tenant_id = %tenant_id,
                        workspace_id = %workspace_id,
                        error = %e,
                        "GC cron: full GC failed"
                    ),
                }
            }
        })
    }

    // -----------------------------------------------------------------------
    // Scheduling helpers (pub for testing)
    // -----------------------------------------------------------------------

    /// Compute the next Unix timestamp (seconds) at which
    /// `hour_utc:00:00 UTC` occurs, strictly after `now_secs`.
    ///
    /// # Example
    /// ```
    /// use axiom_core::gc::scheduler::GcScheduler;
    /// // now = 2024-01-01 02:00:00 UTC (7200 s into day)
    /// // next 3:00 AM is 3600 s later
    /// let next = GcScheduler::next_run_at_secs(3, 86_400 + 2 * 3600);
    /// assert_eq!(next, 86_400 + 3 * 3600);
    /// ```
    pub fn next_run_at_secs(hour_utc: u32, now_secs: u64) -> u64 {
        let secs_per_day = 86_400u64;
        let day_start = (now_secs / secs_per_day) * secs_per_day;
        let target = day_start + (hour_utc as u64) * 3600;
        if target > now_secs {
            target
        } else {
            target + secs_per_day
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    // ── GcType ───────────────────────────────────────────────────────────────

    #[test]
    fn gc_type_display_full() {
        assert_eq!(GcType::Full.to_string(), "full");
    }

    #[test]
    fn gc_type_display_incremental() {
        assert_eq!(GcType::Incremental.to_string(), "incremental");
    }

    #[test]
    fn gc_type_eq() {
        assert_eq!(GcType::Full, GcType::Full);
        assert_ne!(GcType::Full, GcType::Incremental);
    }

    // ── SchedulerConfig ──────────────────────────────────────────────────────

    #[test]
    fn default_config_values() {
        let cfg = SchedulerConfig::default();
        assert_eq!(cfg.full_gc_hour_utc, 3);
        assert_eq!(cfg.lock_ttl_secs, 3600);
        assert!(!cfg.holder_id.is_empty());
        assert_eq!(cfg.bytes_per_object_estimate, 0);
    }

    #[test]
    fn each_default_holder_id_is_unique() {
        let a = SchedulerConfig::default().holder_id;
        let b = SchedulerConfig::default().holder_id;
        assert_ne!(a, b);
    }

    // ── FdbGcLock key generation ─────────────────────────────────────────────

    #[test]
    fn lock_key_contains_tenant_and_workspace() {
        let key = FdbGcLock::lock_key("tenant-abc", "ws-xyz");
        let key_str = String::from_utf8_lossy(&key);
        assert!(key_str.contains("gc_lock"));
        assert!(key_str.contains("tenant-abc"));
        assert!(key_str.contains("ws-xyz"));
    }

    #[test]
    fn lock_keys_differ_for_different_workspaces() {
        let k1 = FdbGcLock::lock_key("t1", "ws1");
        let k2 = FdbGcLock::lock_key("t1", "ws2");
        assert_ne!(k1, k2);
    }

    #[test]
    fn lock_keys_differ_for_different_tenants() {
        let k1 = FdbGcLock::lock_key("t1", "ws1");
        let k2 = FdbGcLock::lock_key("t2", "ws1");
        assert_ne!(k1, k2);
    }

    // ── GcScheduler::next_run_at_secs ────────────────────────────────────────

    #[test]
    fn next_run_at_future_today_when_before_target_hour() {
        // now = 2:00 AM UTC on day 0, target = 3:00 AM UTC
        let day0 = 0u64;
        let now = day0 + 2 * 3600;
        let next = GcScheduler::next_run_at_secs(3, now);
        assert_eq!(next, day0 + 3 * 3600);
    }

    #[test]
    fn next_run_at_tomorrow_when_past_target_hour() {
        // now = 4:00 AM UTC on day 0, target = 3:00 AM UTC
        let day0 = 0u64;
        let now = day0 + 4 * 3600;
        let next = GcScheduler::next_run_at_secs(3, now);
        assert_eq!(next, day0 + 86_400 + 3 * 3600);
    }

    #[test]
    fn next_run_at_tomorrow_when_exactly_on_target_hour() {
        // now is exactly 3:00 AM — should schedule for tomorrow
        let day0 = 0u64;
        let now = day0 + 3 * 3600;
        let next = GcScheduler::next_run_at_secs(3, now);
        assert_eq!(next, day0 + 86_400 + 3 * 3600);
    }

    #[test]
    fn next_run_at_midnight_target() {
        // now = 1:00 AM, target = midnight (hour=0)
        let day0 = 86_400u64; // start of day 1
        let now = day0 + 3600; // 1 AM
        let next = GcScheduler::next_run_at_secs(0, now);
        // midnight tomorrow
        assert_eq!(next, day0 + 86_400);
    }

    #[test]
    fn next_run_at_late_night_target() {
        // now = 22:00, target = 23:00
        let day0 = 0u64;
        let now = day0 + 22 * 3600;
        let next = GcScheduler::next_run_at_secs(23, now);
        assert_eq!(next, day0 + 23 * 3600);
    }

    // ── GcMetrics ────────────────────────────────────────────────────────────

    fn make_metrics() -> GcMetrics {
        let registry = Registry::new();
        GcMetrics::new(&registry).expect("metrics registration failed")
    }

    fn sample_report(pending: usize) -> GcReport {
        GcReport {
            live_objects: 10,
            garbage_candidates: 5,
            grace_period_pending: 0,
            unsafe_skipped: 0,
            pending_delete_enqueued: pending,
        }
    }

    #[test]
    fn metrics_runs_total_increments_per_type() {
        let m = make_metrics();
        m.record(&GcType::Full, &sample_report(0), 1.0, 0);
        m.record(&GcType::Full, &sample_report(0), 1.0, 0);
        m.record(&GcType::Incremental, &sample_report(0), 1.0, 0);

        let full = m.runs_total.with_label_values(&["full"]).get();
        let incr = m.runs_total.with_label_values(&["incremental"]).get();
        assert_eq!(full, 2.0);
        assert_eq!(incr, 1.0);
    }

    #[test]
    fn metrics_objects_deleted_total_accumulates() {
        let m = make_metrics();
        m.record(&GcType::Full, &sample_report(3), 1.0, 0);
        m.record(&GcType::Incremental, &sample_report(7), 1.0, 0);
        assert_eq!(m.objects_deleted_total.get(), 10.0);
    }

    #[test]
    fn metrics_bytes_reclaimed_uses_estimate() {
        let m = make_metrics();
        m.record(&GcType::Full, &sample_report(4), 1.0, 1_000);
        assert_eq!(m.bytes_reclaimed_total.get(), 4_000.0);
    }

    #[test]
    fn metrics_bytes_reclaimed_zero_when_no_estimate() {
        let m = make_metrics();
        m.record(&GcType::Full, &sample_report(10), 1.0, 0);
        assert_eq!(m.bytes_reclaimed_total.get(), 0.0);
    }

    #[test]
    fn metrics_duration_observed() {
        let m = make_metrics();
        m.record(&GcType::Full, &sample_report(0), 2.5, 0);
        // histogram sample count should be 1
        let count = m.duration_seconds.get_sample_count();
        assert_eq!(count, 1);
    }

    #[test]
    fn metrics_registration_is_idempotent_across_fresh_registries() {
        // Each call with a fresh Registry should succeed independently
        let r1 = Registry::new();
        let r2 = Registry::new();
        assert!(GcMetrics::new(&r1).is_ok());
        assert!(GcMetrics::new(&r2).is_ok());
    }
}
