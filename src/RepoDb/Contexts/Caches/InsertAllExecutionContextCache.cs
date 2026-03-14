using System.Collections.Concurrent;
using RepoDb.Contexts.Execution;

namespace RepoDb.Contexts.Caches;

/// <summary>
/// A class that is being used to cache the execution context of the MergeAll operation.
/// </summary>
internal static class InsertAllExecutionContextCache
{
    private static readonly ConcurrentDictionary<string, InsertAllExecutionContext> cache = new();

    /// <summary>
    /// Flushes all the cached execution context.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    internal static void Add(string key,
        InsertAllExecutionContext context) =>
        cache.TryAdd(key, context);

    internal static InsertAllExecutionContext? Get(string key)
    {
        return cache.TryGetValue(key, out var result) ? result : null;
    }
}
