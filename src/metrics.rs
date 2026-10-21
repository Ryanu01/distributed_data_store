use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use sysinfo::{Disks, System};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const MAX_CONCURRENT_OPERATIONS: f64 = 8.0;
const EMA_ALPHA: f64 = 0.25;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_id: EndpointId,
    pub free_storage: u64,
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub latency: f64,
    pub bandwidth: f64,
    pub reliability: f64,
    pub failure_rate: f64,
    pub current_load: f64,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    node_id: EndpointId,
    data_dir: PathBuf,
    free_storage: AtomicU64,
    cpu_usage: AtomicF32,
    memory_usage: AtomicF32,
    latency_ms: AtomicF64,
    bandwidth_bps: AtomicF64,
    reliability: AtomicF64,
    failure_rate: AtomicF64,
    active_operations: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    running: AtomicBool,
}

impl Metrics {
    pub fn start(node_id: EndpointId, data_dir: &Path) -> Self {
        let data_dir = std::path::absolute(data_dir).unwrap_or_else(|_| data_dir.to_path_buf());
        let metrics = Self {
            inner: Arc::new(Inner {
                node_id,
                data_dir,
                free_storage: AtomicU64::new(0),
                cpu_usage: AtomicF32::new(0.0),
                memory_usage: AtomicF32::new(0.0),
                latency_ms: AtomicF64::new(0.0),
                bandwidth_bps: AtomicF64::new(0.0),
                reliability: AtomicF64::new(1.0),
                failure_rate: AtomicF64::new(0.0),
                active_operations: AtomicU64::new(0),
                successes: AtomicU64::new(0),
                failures: AtomicU64::new(0),
                running: AtomicBool::new(true),
            }),
        };

        let mut system = System::new_all();
        system.refresh_memory();
        system.refresh_cpu_usage();
        let disks = Disks::new_with_refreshed_list();
        update_system_metrics(&metrics.inner, &system, &disks);

        tokio::spawn(sample_loop(metrics.inner.clone(), system, disks));
        metrics
    }

    pub fn snapshot(&self) -> NodeHealth {
        let active = self.inner.active_operations.load(Ordering::Relaxed) as f64;
        NodeHealth {
            node_id: self.inner.node_id,
            free_storage: self.inner.free_storage.load(Ordering::Relaxed),
            cpu_usage: self.inner.cpu_usage.load(),
            memory_usage: self.inner.memory_usage.load(),
            latency: self.inner.latency_ms.load(),
            bandwidth: self.inner.bandwidth_bps.load(),
            reliability: self.inner.reliability.load(),
            failure_rate: self.inner.failure_rate.load(),
            current_load: (active / MAX_CONCURRENT_OPERATIONS).clamp(0.0, 1.0),
        }
    }

    pub fn begin_operation(&self) -> OperationGuard {
        self.inner.active_operations.fetch_add(1, Ordering::Relaxed);
        OperationGuard {
            metrics: self.clone(),
            started: Instant::now(),
            completed: false,
        }
    }

    pub fn shutdown(&self) {
        self.inner.running.store(false, Ordering::Relaxed);
    }

    fn record(&self, success: bool, bytes: u64, elapsed: Duration) {
        self.inner
            .active_operations
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();

        let latency_ms = elapsed.as_secs_f64() * 1000.0;
        update_ema(&self.inner.latency_ms, latency_ms);

        if success {
            self.inner.successes.fetch_add(1, Ordering::Relaxed);
            let seconds = elapsed.as_secs_f64();
            if bytes > 0 && seconds > 0.0 {
                update_ema(&self.inner.bandwidth_bps, bytes as f64 / seconds);
            }
        } else {
            self.inner.failures.fetch_add(1, Ordering::Relaxed);
        }

        let successes = self.inner.successes.load(Ordering::Relaxed);
        let failures = self.inner.failures.load(Ordering::Relaxed);
        let total = successes + failures;
        if total > 0 {
            let success_ratio = successes as f64 / total as f64;
            self.inner.reliability.store(success_ratio);
            self.inner
                .failure_rate
                .store(failures as f64 / total as f64);
        }
    }
}

pub struct OperationGuard {
    metrics: Metrics,
    started: Instant,
    completed: bool,
}

impl OperationGuard {
    pub fn finish_success(mut self, bytes: u64) {
        self.completed = true;
        self.metrics.record(true, bytes, self.started.elapsed());
    }

    pub fn finish_failure(mut self) {
        self.completed = true;
        self.metrics.record(false, 0, self.started.elapsed());
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.record(false, 0, self.started.elapsed());
        }
    }
}

async fn sample_loop(inner: Arc<Inner>, mut system: System, mut disks: Disks) {
    loop {
        tokio::time::sleep(SAMPLE_INTERVAL).await;
        if !inner.running.load(Ordering::Relaxed) {
            break;
        }

        system.refresh_cpu_usage();
        system.refresh_memory();
        disks.refresh(true);
        update_system_metrics(&inner, &system, &disks);
    }
}

fn update_system_metrics(inner: &Inner, system: &System, disks: &Disks) {
    let total_memory = system.total_memory();
    let memory_usage = if total_memory == 0 {
        0.0
    } else {
        system.used_memory() as f64 / total_memory as f64 * 100.0
    };

    inner.cpu_usage.store(system.global_cpu_usage());
    inner.memory_usage.store(memory_usage as f32);
    inner
        .free_storage
        .store(available_space(disks, &inner.data_dir), Ordering::Relaxed);
}

fn available_space(disks: &Disks, path: &Path) -> u64 {
    let mut best: Option<(&Path, u64)> = None;

    for disk in disks.list() {
        let mount_point = disk.mount_point();
        if !path.starts_with(mount_point) {
            continue;
        }

        let is_better = best
            .map(|(best_mount, _)| mount_point.as_os_str().len() > best_mount.as_os_str().len())
            .unwrap_or(true);
        if is_better {
            best = Some((mount_point, disk.available_space()));
        }
    }

    best.map(|(_, space)| space).unwrap_or(0)
}

fn update_ema(target: &AtomicF64, sample: f64) {
    if !sample.is_finite() || sample < 0.0 {
        return;
    }

    let previous = target.load();
    let next = if previous <= 0.0 {
        sample
    } else {
        previous * (1.0 - EMA_ALPHA) + sample * EMA_ALPHA
    };
    target.store(next);
}

#[derive(Debug)]
struct AtomicF32(AtomicU32);

impl AtomicF32 {
    fn new(value: f32) -> Self {
        Self(AtomicU32::new(value.to_bits()))
    }

    fn load(&self) -> f32 {
        f32::from_bits(self.0.load(Ordering::Relaxed))
    }

    fn store(&self, value: f32) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct AtomicF64(AtomicU64);

impl AtomicF64 {
    fn new(value: f64) -> Self {
        Self(AtomicU64::new(value.to_bits()))
    }

    fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }

    fn store(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn exposes_initial_health() {
        let dir = TempDir::new().unwrap();
        let node_id = iroh::SecretKey::generate().public();
        let metrics = Metrics::start(node_id, dir.path());

        let health = metrics.snapshot();
        assert_eq!(health.node_id, node_id);
        assert!((0.0..=100.0).contains(&health.cpu_usage));
        assert!((0.0..=100.0).contains(&health.memory_usage));
        assert_eq!(health.reliability, 1.0);
        assert_eq!(health.failure_rate, 0.0);
        assert_eq!(health.current_load, 0.0);

        metrics.shutdown();
    }

    #[tokio::test]
    async fn tracks_operation_success_and_failure() {
        let dir = TempDir::new().unwrap();
        let node_id = iroh::SecretKey::generate().public();
        let metrics = Metrics::start(node_id, dir.path());

        let guard = metrics.begin_operation();
        assert!(metrics.snapshot().current_load > 0.0);
        guard.finish_success(1024);

        let guard = metrics.begin_operation();
        guard.finish_failure();

        let health = metrics.snapshot();
        assert_eq!(health.reliability, 0.5);
        assert_eq!(health.failure_rate, 0.5);
        assert_eq!(health.current_load, 0.0);

        metrics.shutdown();
    }
}
