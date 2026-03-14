using System.Collections.Concurrent;
using RepoDb.Contexts.Execution;

namespace RepoDb.Contexts.Caches;

/// <summary>
/// A class that is being used to cache the execution context of the Update operation.
/// </summary>
internal static class UpdateExecutionContextCache
{
    private static readonly ConcurrentDictionary<string, UpdateExecutionContext> cache = new();

    /// <summary>
    /// Flushes all the cached execution context.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    internal static void Add(string key,
        UpdateExecutionContext context) =>
        cache.TryAdd(key, context);

    internal static UpdateExecutionContext? Get(string key)
    {
        if (cache.TryGetValue(key, out var result))
        {
            return result;
        }
        return null;
    }
}
