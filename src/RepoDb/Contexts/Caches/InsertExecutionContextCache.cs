using System.Collections.Concurrent;
using RepoDb.Contexts.Execution;

namespace RepoDb.Contexts.Caches;

/// <summary>
/// A class that is being used to cache the execution context of the Insert operation.
/// </summary>
internal static class InsertExecutionContextCache
{
    private static readonly ConcurrentDictionary<string, InsertExecutionContext> cache = new();

    /// <summary>
    /// Flushes all the cached execution context.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    internal static void Add(string key,
        InsertExecutionContext context) =>
        cache.TryAdd(key, context);

    internal static InsertExecutionContext? Get(string key)
    {
        return cache.TryGetValue(key, out var result) ? result : null;
    }
}
