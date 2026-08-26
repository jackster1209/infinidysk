using System.Text;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Services.Observability;
using Prometheus;

namespace NzbWebDAV.Tests.Services.Observability;

public sealed class PrometheusMetricsTests
{
    [Fact]
    public async Task RecordsOnlyBoundedSeekAndFetchLabels()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);

        metrics.RecordSeek("warm", TimeSpan.FromMilliseconds(12));
        metrics.RecordSegmentFetch("provider-a", "ok", TimeSpan.FromMilliseconds(20));

        await using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        var exposition = Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("nzbdav_seek_total", exposition);
        Assert.Contains("kind=\"warm\"", exposition);
        Assert.Contains("nzbdav_segment_fetches_total", exposition);
        Assert.Contains("provider_key=\"provider-a\"", exposition);
        Assert.DoesNotContain("path=", exposition);
        Assert.DoesNotContain("filename=", exposition);
    }

    [Fact]
    public async Task RecordsStatVerificationTelemetryWithBoundedLabels()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);

        metrics.RecordStatAttempt("provider-a", "exists", TimeSpan.FromMilliseconds(40));
        metrics.RecordStatAttempt("provider-b", "missing", TimeSpan.FromMilliseconds(600));
        metrics.RecordStatAttempt("provider-b", "error", TimeSpan.FromMilliseconds(90));
        metrics.RecordStatWalk("exists", 3);

        await using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        var exposition = Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("nzbdav_nntp_stat_attempts_total", exposition);
        Assert.Contains("nzbdav_nntp_stat_duration_seconds", exposition);
        Assert.Contains("nzbdav_nntp_stat_walk_depth", exposition);
        Assert.Contains("result=\"exists\"", exposition);
        Assert.Contains("result=\"missing\"", exposition);
        Assert.Contains("result=\"error\"", exposition);
        Assert.Contains("outcome=\"exists\"", exposition);

        // Labels stay bounded: provider key plus fixed enums, never article ids or paths.
        Assert.DoesNotContain("segment_id=", exposition);
        Assert.DoesNotContain("message_id=", exposition);
        Assert.DoesNotContain("filename=", exposition);
    }

    [Fact]
    public async Task StatWalkDepth_FirstProviderHitsAreReadableFromTheLeOneBucket()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);

        // Two segments resolved on the first provider, one after a three-provider walk.
        metrics.RecordStatWalk("exists", 1);
        metrics.RecordStatWalk("exists", 1);
        metrics.RecordStatWalk("exists", 3);

        await using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        var exposition = Encoding.UTF8.GetString(stream.ToArray());

        // le="1" is the first-provider hit count; sum/count is the mean walk depth.
        Assert.Contains("nzbdav_nntp_stat_walk_depth_bucket{outcome=\"exists\",le=\"1\"} 2", exposition);
        Assert.Contains("nzbdav_nntp_stat_walk_depth_sum{outcome=\"exists\"} 5", exposition);
        Assert.Contains("nzbdav_nntp_stat_walk_depth_count{outcome=\"exists\"} 3", exposition);
    }

    [Fact]
    public async Task RegistersSharedStreamRetentionGauges()
    {
        var registry = new CollectorRegistry();
        _ = new PrometheusMetrics(registry);

        await using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        var exposition = Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("nzbdav_shared_stream_ring_retained_bytes", exposition);
        Assert.Contains("nzbdav_shared_stream_ring_retained_bytes_peak", exposition);
        Assert.Contains("nzbdav_shared_stream_ring_logical_bytes", exposition);
        Assert.Contains("nzbdav_shared_stream_pump_scratch_bytes", exposition);
        Assert.Contains("nzbdav_shared_stream_live_entries", exposition);
        Assert.Contains("nzbdav_shared_stream_ready_entries", exposition);
        Assert.Contains("nzbdav_shared_stream_draining_entries", exposition);
        Assert.Contains("nzbdav_shared_stream_lagging_readers", exposition);
        Assert.Contains("nzbdav_shared_stream_pressure_detaches_total", exposition);
        Assert.Contains("nzbdav_shared_stream_pressure_reaps_total", exposition);
    }

    [Fact]
    public async Task ProviderPoolMetricsExposeAndRemoveAdmissionState()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);
        var churn = new ConnectionPoolChurn(1, 2, 3, 4, 5, 6, 7);
        var admission = new ProviderConnectionAdmissionSnapshot(
            ConfiguredTransferLimit: 20,
            EffectiveTransferLimit: 15,
            BaseMetadataCapacity: 0,
            MetadataBurstAllowance: 7,
            MaxMetadataCapacity: 7,
            ActiveTransferOperations: 10,
            ActiveMetadataOperations: 5,
            WaitingTransferOperations: 2,
            WaitingMetadataOperations: 3);
        var snapshot = new ProviderConnectionSnapshot(
            "provider-a", "news.example", ProviderType.Pooled,
            LiveConnections: 15,
            IdleConnections: 0,
            ActiveConnections: 15,
            AvailableConnections: 0,
            PendingSelections: 1,
            churn,
            LearnedConnectionLimit: 17,
            ConfiguredMaxConnections: 50,
            EffectiveMaxConnections: 15,
            admission);

        metrics.SetPool(snapshot);
        var exposition = await ExportAsync(registry);

        Assert.Contains("state=\"transfer_active\"} 10", exposition);
        Assert.Contains("state=\"metadata_active\"} 5", exposition);
        Assert.Contains("state=\"transfer_waiting\"} 2", exposition);
        Assert.Contains("state=\"metadata_waiting\"} 3", exposition);
        Assert.Contains("limit=\"configured\"} 50", exposition);
        Assert.Contains("limit=\"effective\"} 15", exposition);
        Assert.Contains("limit=\"transfer_configured\"} 20", exposition);
        Assert.Contains("limit=\"transfer_effective\"} 15", exposition);
        Assert.Contains("limit=\"metadata_base\"} 0", exposition);
        Assert.Contains("limit=\"metadata_burst\"} 7", exposition);
        Assert.Contains("limit=\"metadata_max\"} 7", exposition);

        metrics.SetPool(snapshot with { Admission = null, LearnedConnectionLimit = null });
        exposition = await ExportAsync(registry);

        Assert.DoesNotContain("state=\"transfer_active\"", exposition);
        Assert.DoesNotContain("state=\"metadata_active\"", exposition);
        Assert.DoesNotContain("limit=\"transfer_configured\"", exposition);
        Assert.DoesNotContain("limit=\"learned\"", exposition);
    }

    [Fact]
    public async Task VerificationCoverageMetricsExposeAndRemoveProviderState()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);
        var snapshot = new ProviderConnectionSnapshot(
            "provider-a", "news.example", ProviderType.Pooled,
            LiveConnections: 1,
            IdleConnections: 0,
            ActiveConnections: 1,
            AvailableConnections: 0,
            PendingSelections: 0,
            new ConnectionPoolChurn(1, 2, 3, 4, 5, 6, 7),
            LearnedConnectionLimit: null,
            ConfiguredMaxConnections: 10,
            EffectiveMaxConnections: 10,
            Admission: null,
            VerificationCoverage: new VerificationCoverageSnapshot(
                VerificationCoverageState.Deprioritized,
                Samples: 240,
                MissRate: 0.9,
                LastTransitionUtc: DateTimeOffset.UnixEpoch,
                Deprioritizations: 2));

        metrics.SetPool(snapshot);
        var exposition = await ExportAsync(registry);

        Assert.Contains("nzbdav_nntp_verification_coverage_state{provider_key=\"provider-a\"} 1", exposition);
        Assert.Contains("nzbdav_nntp_verification_coverage_samples{provider_key=\"provider-a\"} 240", exposition);
        Assert.Contains("nzbdav_nntp_verification_coverage_miss_rate{provider_key=\"provider-a\"} 0.9", exposition);
        Assert.Contains(
            "nzbdav_nntp_verification_coverage_deprioritizations_total{provider_key=\"provider-a\"} 2",
            exposition);

        // Labels stay bounded to the provider key: no message ids, no per-state cardinality.
        Assert.DoesNotContain("message_id=", exposition);

        // A client that reports no coverage must not leave a stale series behind.
        metrics.SetPool(snapshot with { VerificationCoverage = null });
        exposition = await ExportAsync(registry);

        Assert.DoesNotContain("nzbdav_nntp_verification_coverage_state{", exposition);
        Assert.DoesNotContain("nzbdav_nntp_verification_coverage_samples{", exposition);
    }

    [Fact]
    public async Task HealthCheckGateMetricsExposeCurrentAndWindowPeakState()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);

        metrics.SetHealthCheckGate(new HealthCheckConnectionGateSnapshot(
            Limit: 50,
            Active: 40,
            WaitingQueue: 2,
            WaitingBackground: 75,
            PeakActive: 50,
            PeakWaitingQueue: 4,
            PeakWaitingBackground: 120));
        var exposition = await ExportAsync(registry);

        Assert.Contains("nzbdav_health_check_gate_limit 50", exposition);
        Assert.Contains("nzbdav_health_check_gate_active 40", exposition);
        Assert.Contains("nzbdav_health_check_gate_peak_active 50", exposition);
        Assert.Contains("nzbdav_health_check_gate_waiting{priority=\"queue\"} 2", exposition);
        Assert.Contains("nzbdav_health_check_gate_waiting{priority=\"background\"} 75", exposition);
        Assert.Contains("nzbdav_health_check_gate_peak_waiting{priority=\"queue\"} 4", exposition);
        Assert.Contains("nzbdav_health_check_gate_peak_waiting{priority=\"background\"} 120", exposition);
    }

    [Fact]
    public async Task HealthCheckSchedulerMetricsExposeBoundedAggregateState()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);

        metrics.SetHealthCheckScheduler(new HealthCheckStatSchedulerSnapshot(
            Capacity: 50,
            ActiveAssignments: 40,
            PendingAdmissions: 10,
            RunnableSessions: 5,
            PendingSegments: 12_345,
            Dispatches: 500,
            Completions: 450,
            Cancellations: 2,
            Failures: 3,
            Sessions: [],
            Providers:
            [
                new HealthCheckStatProviderSnapshot(
                    ProviderKey: "provider-a",
                    ActiveAssignments: 40,
                    RunnableSessions: 5,
                    PendingSegments: 12_345,
                    BlockedSessions: 5,
                    IsLegacySharedPool: false),
            ],
            GlobalBlockedSessions: 1,
            LegacyCompatibilityAssignments: 0));
        var exposition = await ExportAsync(registry);

        Assert.Contains("nzbdav_health_check_scheduler_capacity 50", exposition);
        Assert.Contains(
            "nzbdav_health_check_scheduler_provider{provider_key=\"provider-a\",state=\"active_assignments\"} 40",
            exposition);
        Assert.Contains(
            "nzbdav_health_check_scheduler_provider{provider_key=\"provider-a\",state=\"blocked_sessions\"} 5",
            exposition);
        Assert.Contains(
            "nzbdav_health_check_scheduler_provider{provider_key=\"provider-a\",state=\"pending_segments\"} 12345",
            exposition);
        Assert.Contains("nzbdav_health_check_scheduler_global_blocked_sessions 1", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_legacy_assignments 0", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_active_assignments 40", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_pending_admissions 10", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_runnable_sessions 5", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_sessions{state=\"running\"} 0", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_pending_segments 12345", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_dispatches_total 500", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_completions_total 450", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_cancellations_total 2", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_failures_total 3", exposition);
        Assert.DoesNotContain("run_id", exposition);
        Assert.DoesNotContain("dav_item_id", exposition);
        Assert.DoesNotContain("message_id", exposition);
    }

    [Fact]
    public async Task AutoCeilingIsReportedAsZeroRatherThanASyntheticLimit()
    {
        var registry = new CollectorRegistry();
        var metrics = new PrometheusMetrics(registry);

        metrics.SetHealthCheckScheduler(new HealthCheckStatSchedulerSnapshot(
            Capacity: null,
            ActiveAssignments: 48,
            PendingAdmissions: 0,
            RunnableSessions: 8,
            PendingSegments: 900,
            Dispatches: 100,
            Completions: 52,
            Cancellations: 0,
            Failures: 0,
            Sessions: [],
            Providers: [],
            GlobalBlockedSessions: 0,
            LegacyCompatibilityAssignments: 0));
        var exposition = await ExportAsync(registry);

        // An explicit ceiling is always >= 1, so 0 unambiguously means Auto.
        Assert.Contains("nzbdav_health_check_scheduler_capacity 0", exposition);
        Assert.Contains("nzbdav_health_check_scheduler_active_assignments 48", exposition);
    }

    private static async Task<string> ExportAsync(CollectorRegistry registry)
    {
        await using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
