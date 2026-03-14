using RepoDb.Enumerations;

namespace RepoDb;

/// <summary>
///
/// </summary>
public sealed record class DbSchemaObject
{
    /// <summary>
    ///
    /// </summary>
    public DbSchemaType Type { get; init; }

    /// <summary>
    ///
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    ///
    /// </summary>
    public string? Schema { get; init; }
}


