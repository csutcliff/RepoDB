namespace RepoDb;

/// <summary>
/// A class that is being used to hold the <see cref="RepoDb.QueryGroup"/> object type mapping. This class has been introduced
/// to support the needs of the multi-resultsets query operation.
/// </summary>
internal readonly struct QueryGroupTypeMap
{
    /// <summary>
    /// Creates an instance of <see cref="QueryGroupTypeMap"/> class.
    /// </summary>
    /// <param name="queryGroup">The <see cref="RepoDb.QueryGroup"/> object.</param>
    /// <param name="type">The type where the <see cref="RepoDb.QueryGroup"/> object is mapped.</param>
    /// <param name="tableName"></param>
    public QueryGroupTypeMap(QueryGroup queryGroup,
        Type? type, string? tableName = null)
    {
        QueryGroup = queryGroup;
        MappedType = type;
        TableName = tableName;
    }

    /// <summary>
    /// Gets the current associated <see cref="RepoDb.QueryGroup"/> object.
    /// </summary>
    public QueryGroup QueryGroup { get; }

    /// <summary>
    /// Gets the type where the current <see cref="RepoDb.QueryGroup"/> is mapped.
    /// </summary>
    public Type? MappedType { get; }

    /// <summary>
    ///
    /// </summary>
    public string? TableName { get; }
}
