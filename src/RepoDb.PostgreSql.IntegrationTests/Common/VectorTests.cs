
using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Common;

[TestClass]
public class VectorTests : TestCore.VectorTestsBase<PostgreSqlDbInstance>
{
    bool _failVectors = false;

    protected override void InitializeCore()
    {
        GlobalConfiguration.Setup().UsePostgreSqlVectors();
        Database.Initialize();

        using var connection = CreateConnection();
        try
        {
            connection.ExecuteNonQuery("CREATE EXTENSION IF NOT EXISTS vector");

            ((NpgsqlConnection)connection).ReloadTypes();
            NpgsqlConnection.ClearAllPools();
        }
        catch
        {
            _failVectors = true;
        }
    }

    public override bool HaveVectorSupport()
    {
        return !_failVectors && base.HaveVectorSupport();
    }
}
