using RepoDb.Enumerations;
using RepoDb.Interfaces;

namespace RepoDb.Extensions.QueryFields;

/// <summary>
/// Abstract base for query fields representing expressions (e.g., field-to-field, arithmetic, etc.).
/// </summary>
public abstract class ExpressionQueryField : QueryField
{
    /// <summary>
    ///
    /// </summary>
    public IEnumerable<Field> Fields { get; }

    /// <summary>
    ///
    /// </summary>
    /// <param name="fields"></param>
    /// <param name="operation"></param>
    protected ExpressionQueryField(IEnumerable<string> fields, Operation operation)
        : this(fields.Select(x => new Field(x)), operation)
    {
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="fields"></param>
    /// <param name="operation"></param>
    protected ExpressionQueryField(IEnumerable<Field> fields, Operation operation)
                : base(fields.First(), operation, null, null)
    {
        Fields = fields;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="index"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    public override string GetString(int index, IDbSetting? dbSetting)
    {
        return GetString(dbSetting);
    }

    /// <inheritdoc/>>
    public abstract override string GetString(IDbSetting? dbSetting);

    /// <inheritdoc/>>
    protected internal override bool NoParametersNeeded => true;
}
