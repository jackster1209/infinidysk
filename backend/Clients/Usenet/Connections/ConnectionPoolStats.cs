using NzbWebDAV.Config;
using NzbWebDAV.Models;
using NzbWebDAV.Websocket;

namespace NzbWebDAV.Clients.Usenet.Connections;

public class ConnectionPoolStats
{
    // Pool-changed events fire on every connection borrow/return — hundreds per second under
    // load. Websocket updates are coalesced: events only update in-memory counters, and a
    // single flush task emits the latest per-provider stats at most once per interval.
    // The flush is trailing-edge, so the final state after a burst is always sent.
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(200);

    private readonly int[] _live;
    private readonly int[] _idle;
    private readonly int[] _latestLive;
    private readonly int[] _latestIdle;
    private readonly int[] _latestMax;
    private readonly ProviderConnectionAdmissionSnapshot?[] _latestAdmission;
    private readonly bool[] _dirty;
    private readonly bool _splitSummaryEnabled;
    private int _totalLive;
    private int _totalIdle;
    private int _totalMax;
    private int _flushScheduled; // 0 == false, 1 == true
    private int _active = 1;
    private readonly Lock _lock = new();
    private readonly UsenetProviderConfig _providerConfig;
    private readonly WebsocketManager _websocketManager;

    internal bool IsActive => Volatile.Read(ref _active) == 1;

    public ConnectionPoolStats(UsenetProviderConfig providerConfig, WebsocketManager websocketManager)
    {
        // Provider indexes are the cxs replay keys. A replacement configuration may
        // have fewer or reordered providers, so discard the retired generation's state
        // before the new pools publish their initial snapshots.
        websocketManager.ClearKeyedState(WebsocketTopic.UsenetConnections);

        var count = providerConfig.Providers.Count;
        _live = new int[count];
        _idle = new int[count];
        _latestLive = new int[count];
        _latestIdle = new int[count];
        _latestMax = new int[count];
        _latestAdmission = new ProviderConnectionAdmissionSnapshot?[count];
        _dirty = new bool[count];

        // Initialize from config so the header shows the configured ceiling before
        // the first pool event arrives; events then keep it current (effective max).
        for (var i = 0; i < count; i++)
            _latestMax[i] = providerConfig.Providers[i].MaxConnections;
        _totalMax = providerConfig.Providers
            .Where(x => x.Type == ProviderType.Pooled)
            .Select(x => x.MaxConnections)
            .Sum();
        var enabledProviders = providerConfig.Providers
            .Where(provider => provider.Type != ProviderType.Disabled)
            .ToArray();
        _splitSummaryEnabled = enabledProviders.Length > 0
                               && enabledProviders.All(provider =>
                                   provider.MaxTransferConnections.HasValue);

        _providerConfig = providerConfig;
        _websocketManager = websocketManager;
    }

    public EventHandler<ConnectionPoolChangedEventArgs> GetOnConnectionPoolChanged(int providerIndex)
    {
        return OnEvent;

        void OnEvent(object? _, ConnectionPoolChangedEventArgs args)
        {
            if (Volatile.Read(ref _active) == 0)
                return;

            lock (_lock)
            {
                if (_active == 0)
                    return;

                _latestLive[providerIndex] = args.Live;
                _latestIdle[providerIndex] = args.Idle;
                _latestMax[providerIndex] = args.Max;
                _dirty[providerIndex] = true;

                if (_providerConfig.Providers[providerIndex].Type == ProviderType.Pooled)
                {
                    _live[providerIndex] = args.Live;
                    _idle[providerIndex] = args.Idle;
                    _totalLive = _live.Sum();
                    _totalIdle = _idle.Sum();
                    _totalMax = _latestMax
                        .Where((_, i) => _providerConfig.Providers[i].Type == ProviderType.Pooled)
                        .Sum();
                }
            }

            ScheduleFlush();
        }
    }

    public Action<ProviderConnectionAdmissionSnapshot> GetOnConnectionAdmissionChanged(
        int providerIndex)
    {
        return OnChanged;

        void OnChanged(ProviderConnectionAdmissionSnapshot snapshot)
        {
            if (!_splitSummaryEnabled)
                return;
            if (Volatile.Read(ref _active) == 0)
                return;

            lock (_lock)
            {
                if (_active == 0)
                    return;
                _latestAdmission[providerIndex] = snapshot;
                _dirty[providerIndex] = true;
            }

            ScheduleFlush();
        }
    }

    private void ScheduleFlush()
    {
        if (Interlocked.CompareExchange(ref _flushScheduled, 1, 0) == 0)
            _ = FlushAfterDelayAsync();
    }

    private async Task FlushAfterDelayAsync()
    {
        await Task.Delay(FlushInterval).ConfigureAwait(false);

        // allow a new flush to be scheduled *before* taking the snapshot,
        // so events arriving after the snapshot are never lost.
        Volatile.Write(ref _flushScheduled, 0);

        // Intentionally no HasSubscribers gate here: SendMessage records the
        // latest message for state replay before skipping delivery, so flushing
        // while unsubscribed keeps the replayed connection counts fresh for the
        // next browser that subscribes. The messages are tiny strings, so there
        // is no meaningful work to save by gating earlier.
        lock (_lock)
        {
            // Publish while holding the same lock used by Deactivate(). SendMessage is
            // synchronous, so once Deactivate returns no stale flush can still win the
            // last-message race against the replacement generation.
            if (_active == 0)
                return;

            for (var i = 0; i < _dirty.Length; i++)
            {
                if (!_dirty[i]) continue;
                _dirty[i] = false;
                var message =
                    $"{i}|{_latestLive[i]}|{_latestIdle[i]}|{_totalLive}|{_totalMax}|{_totalIdle}";
                if (_splitSummaryEnabled)
                {
                    var summary = CreateSplitSummary();
                    message +=
                        $"|1|{summary.ActiveTransfers}|{summary.TransferLimit}" +
                        $"|{summary.ActiveMetadata}|{summary.MetadataBase}|{summary.MetadataMax}";
                }
                _ = _websocketManager.SendMessage(WebsocketTopic.UsenetConnections, message);
            }
        }
    }

    private SplitConnectionSummary CreateSplitSummary()
    {
        var activeTransfers = 0;
        var transferLimit = 0;
        var activeMetadata = 0;
        var metadataBase = 0;
        var metadataMax = 0;
        for (var i = 0; i < _providerConfig.Providers.Count; i++)
        {
            var provider = _providerConfig.Providers[i];
            if (provider.Type != ProviderType.Pooled) continue;

            var admission = _latestAdmission[i];
            var budget = ProviderConnectionBudget.Calculate(
                _latestMax[i],
                provider.MaxTransferConnections!.Value);
            activeTransfers += admission?.ActiveTransferOperations ?? 0;
            transferLimit += budget.EffectiveTransferLimit;
            activeMetadata += admission?.ActiveMetadataOperations ?? 0;
            metadataBase += budget.BaseMetadataCapacity;
            metadataMax += budget.MaxMetadataCapacity;
        }

        return new SplitConnectionSummary(
            activeTransfers,
            transferLimit,
            activeMetadata,
            metadataBase,
            metadataMax);
    }

    /// <summary>
    /// Stops a retired client generation from overwriting connection totals published by
    /// its replacement while its existing streams finish draining.
    /// </summary>
    internal void Deactivate()
    {
        lock (_lock)
        {
            _active = 0;
            Array.Clear(_dirty);
        }
    }

    public sealed class ConnectionPoolChangedEventArgs(int live, int idle, int max) : EventArgs
    {
        public int Live { get; } = live;
        public int Idle { get; } = idle;
        public int Max { get; } = max;
        public int Active => Live - Idle;
    }

    private sealed record SplitConnectionSummary(
        int ActiveTransfers,
        int TransferLimit,
        int ActiveMetadata,
        int MetadataBase,
        int MetadataMax);
}
