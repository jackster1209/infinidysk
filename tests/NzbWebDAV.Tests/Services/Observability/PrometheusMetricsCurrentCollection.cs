namespace NzbWebDAV.Tests.Services.Observability;

/// <summary>
/// Serialises tests that swap the process-wide <c>PrometheusMetrics.Current</c> sink.
/// Any code path may record into it, so two such tests running in parallel would see
/// each other's samples.
/// </summary>
[CollectionDefinition(nameof(PrometheusMetricsCurrentCollection), DisableParallelization = true)]
public sealed class PrometheusMetricsCurrentCollection;
