using System.Data;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.Requests;

/// <summary>
/// A class that holds the value of the 'Query' operation arguments.
/// </summary>
internal sealed class QueryRequest : BaseRequest
{
    /// <summary>
    /// Creates a new instance of <see cref="QueryRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="orderBy">The list of order fields.</param>
    /// <param name="top">The filter for the rows.</param>
    /// <param name="offset"></param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public QueryRequest(Type type,
        IDbConnection connection,
        IDbTransaction? transaction,
        IEnumerable<Field>? fields,
        QueryGroup? where = null,
        IEnumerable<OrderField>? orderBy = null,
        int top = 0,
        int offset = 0,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(ClassMappedNameCache.Get(type),
              connection,
              transaction,
              fields,
              where,
              orderBy,
              top,
              offset,
              hints,
              statementBuilder)
    {
        Type = type;
    }

    /// <summary>
    /// Creates a new instance of <see cref="QueryRequest"/> object.
    /// </summary>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="orderBy">The list of order fields.</param>
    /// <param name="top">The filter for the rows.</param>
    /// <param name="offset"></param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public QueryRequest(string name,
        IDbConnection connection,
        IDbTransaction? transaction,
        IEnumerable<Field>? fields,
        QueryGroup? where = null,
        IEnumerable<OrderField>? orderBy = null,
        int top = 0,
        int offset = 0,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : base(name,
              connection,
              transaction,
              statementBuilder)
    {
        Fields = fields?.AsFieldSet();
        Where = where;
        OrderBy = orderBy?.AsList();
        Take = top;
        Offset = offset;
        Hints = hints;
    }

    /// <summary>
    /// Gets the list of the target fields.
    /// </summary>
    public FieldSet? Fields { get; init; }

    /// <summary>
    /// Gets the query expression used.
    /// </summary>
    public QueryGroup? Where { get; }

    /// <summary>
    /// Gets the list of the order fields.
    /// </summary>
    public IEnumerable<OrderField>? OrderBy { get; }

    /// <summary>
    /// Gets the filter for the rows.
    /// </summary>
    public int Take { get; }

    /// <summary>
    ///
    /// </summary>
    public int Offset { get; }

    /// <summary>
    /// Gets the hints for the table.
    /// </summary>
    public string? Hints { get; }

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="QueryRequest"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public override int GetHashCode()
    {
        if (this.HashCode is not { } hashCode)
        {
            // Get first the entity hash code
            hashCode = System.HashCode.Combine(
                typeof(QueryRequest),
                StatementBuilder?.GetType(),
				Connection.GetType(),
                TableName,
                Where,
                Take,
                System.HashCode.Combine(
                    Offset,
                    Hints,
                    Fields
                ));

            // Add the order fields
            if (OrderBy is not null)
            {
                foreach (var orderField in OrderBy)
                {
                    hashCode = System.HashCode.Combine(hashCode, orderField);
                }
            }

            HashCode = hashCode;
        }

        return hashCode;
    }

    protected override bool StrictEquals(BaseRequest other)
    {
        // TODO: Implement Equals() and use from here.
        return other is QueryRequest;
    }

    #endregion
}
