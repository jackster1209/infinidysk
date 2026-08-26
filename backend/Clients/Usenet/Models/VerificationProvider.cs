namespace NzbWebDAV.Clients.Usenet.Models;

/// <summary>
/// One entry in a verification sweep order. A caller takes the order once for a file and
/// sweeps each entry in turn, feeding the unresolved set forward, so one sweep is one
/// provider, one pipelined batch, and one connection.
///
/// Taking the order as a snapshot matters: provider ranking moves as verification coverage
/// state and capacity change, and a sweep that re-derived the order per phase could skip or
/// repeat a provider partway through a file.
///
/// Storage-group siblings are already collapsed to one entry, matching the per-STAT walk,
/// which skips the rest of a group once one member reports the article missing.
/// </summary>
public sealed record VerificationProvider(string ProviderKey, string Host, string StorageGroup);
