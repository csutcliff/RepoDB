using System.Collections.Concurrent;
using RepoDb.Contexts.Execution;

namespace RepoDb.Contexts.Caches;

/// <summary>
/// A class that is being used to cache the execution context of the Merge operation.
/// </summary>
internal static class MergeExecutionContextCache
{
    private static readonly ConcurrentDictionary<string, MergeExecutionContext> cache = new();

    /// <summary>
    /// Flushes all the cached execution context.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    internal static void Add(string key,
        MergeExecutionContext context) =>
        cache.TryAdd(key, context);

    internal static MergeExecutionContext? Get(string key)
    {
        return cache.TryGetValue(key, out var result) ? result : null;
    }
}
