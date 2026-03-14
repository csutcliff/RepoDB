using System.Text.Json.Nodes;

namespace RepoDb.Interfaces;

/// <summary>
///
/// </summary>
public interface IDbJsonValue
{
    /// <summary>
    ///
    /// </summary>
    JsonNode? JsonNode { get; }
}
