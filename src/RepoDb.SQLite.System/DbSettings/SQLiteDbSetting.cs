namespace RepoDb.DbSettings;

/// <summary>
/// A setting class used for SQLite data provider.
/// </summary>
public sealed record SQLiteDbSetting : BaseDbSetting
{
    /// <summary>
    /// Creates a new instance of <see cref="SQLiteDbSetting"/> class.
    /// </summary>
    public SQLiteDbSetting()
        : this(true)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="SQLiteDbSetting"/> class.
    /// </summary>
    public SQLiteDbSetting(bool isExecuteReaderDisposable)
    {
        AreTableHintsSupported = false;
        AverageableType = typeof(double);
        ClosingQuote = "]";
        DefaultSchema = null;
        IsDirectionSupported = false;
        IsExecuteReaderDisposable = isExecuteReaderDisposable;
        IsMultiStatementExecutable = true;
        IsPreparable = true;
        OpeningQuote = "[";
        ParameterPrefix = "@";
        MaxQueriesInBatchCount = 10; // No need to optimize using higher value as there is no network latency in SQLite
    }

    protected override string TranslateFunctionalFormat(string format)
    {
        if (format.StartsWith("LEFT({0}, "))
            return "SUBSTR({0}, 1, " + format.Substring("LEFT({0}, ".Length);
        else if (format.StartsWith("RIGHT({0}, "))
            return "SUBSTR({0}, -" + format.Substring("RIGHT({0}, ".Length);

        return base.TranslateFunctionalFormat(format);
    }
}
