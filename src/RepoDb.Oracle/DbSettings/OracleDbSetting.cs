using System.Data;
using RepoDb.Extensions.QueryFields;

namespace RepoDb.DbSettings;

public sealed record OracleDbSetting : BaseDbSetting
{
    public OracleDbSetting() : base()
    {
        AreTableHintsSupported = false;
        OpeningQuote = "\"";
        ClosingQuote = "\"";
        AverageableType = typeof(decimal);
        DefaultSchema = null;
        IsDirectionSupported = true;
        IsExecuteReaderDisposable = true;
        IsMultiStatementExecutable = true;
        IsPreparable = true;
        ParameterPrefix = ":p";
        MaxParameterCount = 8096; // 32766;
        MaxQueriesInBatchCount = 1000;
        GenerateFinalSemiColon = false;
    }


    protected override string? CreateJsonExtract(string path, Parameter parameter)
    {
        return string.Concat(
            "JSON_VALUE({0}, '",
            JsonExtractQueryField.ToJsonPath(path).Replace("'", "''"),
            "'",
            parameter.DbType switch
            {
                DbType.Int32 or DbType.Int64 or DbType.Int16 or DbType.Byte or DbType.Decimal or DbType.Double or DbType.Single => " RETURNING NUMBER",
                DbType.DateTime or DbType.DateTime2 or DbType.Date or DbType.DateTimeOffset => " RETURNING DATE",
                _ => null
            },
            ")"
        );
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
