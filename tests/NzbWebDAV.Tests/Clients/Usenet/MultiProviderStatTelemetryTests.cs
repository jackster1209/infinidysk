using System.Text;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Services.Observability;
using NzbWebDAV.Tests.Services.Observability;
using Prometheus;
using UsenetSharp.Models;

namespace NzbWebDAV.Tests.Clients.Usenet;

/// <summary>
/// STAT is the verification primitive, but the SegmentFetch families are transfer-centric
/// and record nothing for a successful STAT. These assert the stat_* families describe a
/// verification walk end to end: which provider answered, how it answered, and how many
/// providers had to be asked.
/// </summary>
[Collection(nameof(PrometheusMetricsCurrentCollection))]
public sealed class MultiProviderStatTelemetryTests
{
    [Fact]
    public async Task StatResolvedOnSecondProvider_RecordsBothAttemptsAndWalkDepth()
    {
        var (exposition, response) = await CaptureAsync(async client =>
            await client.StatAsync("segment", CancellationToken.None));

        Assert.True(response.ArticleExists);

        // The miss on the first provider and the success on the second are both recorded —
        // the success is the half the SegmentFetch families cannot see.
        Assert.Contains(
            "nzbdav_nntp_stat_attempts_total{provider_key=\"a.example\",result=\"missing\"} 1",
            exposition);
        Assert.Contains(
            "nzbdav_nntp_stat_attempts_total{provider_key=\"b.example\",result=\"exists\"} 1",
            exposition);

        // Two providers were asked before the article resolved.
        Assert.Contains("nzbdav_nntp_stat_walk_depth_sum{outcome=\"exists\"} 2", exposition);
        Assert.Contains("nzbdav_nntp_stat_walk_depth_count{outcome=\"exists\"} 1", exposition);
        // ...so this is not a first-provider hit.
        Assert.Contains("nzbdav_nntp_stat_walk_depth_bucket{outcome=\"exists\",le=\"1\"} 0", exposition);
    }

    [Fact]
    public async Task StatMissingEverywhere_RecordsWalkDepthUnderMissingOutcome()
    {
        var (exposition, response) = await CaptureAsync(
            async client => await client.StatAsync("segment", CancellationToken.None),
            secondProviderFinds: false);

        Assert.False(response.ArticleExists);
        Assert.Contains("nzbdav_nntp_stat_walk_depth_sum{outcome=\"missing\"} 2", exposition);
        Assert.Contains("nzbdav_nntp_stat_walk_depth_count{outcome=\"missing\"} 1", exposition);
        Assert.DoesNotContain("outcome=\"exists\"} 1", exposition);
    }

    [Fact]
    public async Task BodyWalk_DoesNotEmitStatTelemetry()
    {
        // The families are named for STAT and must stay scoped to verification, otherwise
        // walk depth silently blends transfers into the verification picture.
        var (exposition, _) = await CaptureAsync(async client =>
            await client.DecodedBodyAsync("segment", CancellationToken.None));

        Assert.DoesNotContain("nzbdav_nntp_stat_attempts_total{", exposition);
        Assert.DoesNotContain("nzbdav_nntp_stat_walk_depth_count{", exposition);
    }

    private static async Task<(string Exposition, T Result)> CaptureAsync<T>(
        Func<MultiProviderNntpClient, Task<T>> act,
        bool secondProviderFinds = true)
    {
        var registry = new CollectorRegistry();
        var previous = PrometheusMetrics.Current;
        PrometheusMetrics.Current = new PrometheusMetrics(registry);
        try
        {
            var primary = new MultiProviderNntpClientTests.ScriptedNntpClient
            {
                BatchResponseCode = 430,
                SingularResponseCode = (int)UsenetResponseType.NoArticleWithThatMessageId,
            };
            var backup = new MultiProviderNntpClientTests.ScriptedNntpClient
            {
                BatchResponseCode = secondProviderFinds ? 223 : 430,
                SingularResponseCode = secondProviderFinds
                    ? (int)UsenetResponseType.ArticleExists
                    : (int)UsenetResponseType.NoArticleWithThatMessageId,
            };
            using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(primary, host: "a.example"),
                MultiProviderNntpClientTests.CreateProvider(backup, host: "b.example"),
            ]);

            var result = await act(client);

            await using var stream = new MemoryStream();
            await registry.CollectAndExportAsTextAsync(stream);
            return (Encoding.UTF8.GetString(stream.ToArray()), result);
        }
        finally
        {
            PrometheusMetrics.Current = previous;
        }
    }
}
