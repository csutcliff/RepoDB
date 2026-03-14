using System.Collections.Concurrent;
using RepoDb.Contexts.Execution;

namespace RepoDb.Context.Caches;

/// <summary>
/// A class that is being used to cache the execution context of the MergeAll operation.
/// </summary>
internal static class MergeAllExecutionContextCache
{
    private static readonly ConcurrentDictionary<string, MergeAllExecutionContext> cache = new();

    /// <summary>
    /// Flushes all the cached execution context.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    internal static void Add(string key,
        MergeAllExecutionContext context) =>
        cache.TryAdd(key, context);

    internal static MergeAllExecutionContext? Get(string key)
    {
        return cache.TryGetValue(key, out var result) ? result : null;
    }
}
