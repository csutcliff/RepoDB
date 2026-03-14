using System.Data.Common;
using System.Data.SQLite;
using RepoDb.TestCore;

namespace RepoDb.SQLite.System.IntegrationTests.Setup;

public class SQLiteDbInstance : DbInstance<SQLiteConnection>
{
    private readonly SQLiteConnection _conn;

    static SQLiteDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UseSQLite();
    }

    public SQLiteDbInstance()
    {
        var cacheKey = Guid.NewGuid();

        // The NetFramework version of System.Data.SQLite doesn't support Data Source=file: syntax yet. (Is compiletime option. Conservative default)
        var legacyString = "Data Source=" + Path.GetFullPath(cacheKey.ToString()+".db").Replace(Path.DirectorySeparatorChar, '/');

#if NET
        AdminConnectionString = ConnectionString = $"Data Source=file:{cacheKey}.db?mode=memory&cache=shared;";
#else
        AdminConnectionString = ConnectionString = legacyString;
#endif

        // SQLite doesn't have user-level security; limited connection uses same database
        LimitedConnectionString = ConnectionString;

        // Keep one connection open, but don't use it
        try
        {
            _conn = new SQLiteConnection(AdminConnectionString);
            _conn.Open();
        }
        catch
        {
            // But fallback to slow mode if we somehow got an old sqlite version that doesn't support url paths
            AdminConnectionString = ConnectionString = legacyString;

            _conn = new SQLiteConnection(AdminConnectionString);
            _conn.Open();
        }
    }

    public override string DatabaseName => "sqlite";

    protected override Task CreateUserDatabase(DbConnection sql)
    {
        return Task.CompletedTask;
    }
}
