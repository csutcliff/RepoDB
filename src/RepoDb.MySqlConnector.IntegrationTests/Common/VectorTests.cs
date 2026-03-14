using System.Data.Common;
using MySqlConnector;
using RepoDb.MySqlConnector.IntegrationTests.Setup;

namespace RepoDb.MySqlConnector.IntegrationTests.Common;

[TestClass]
public class VectorTests : TestCore.VectorTestsBase<MysqlDbInstance>
{
    protected override void InitializeCore() => Database.Initialize();

    public override bool HaveVectorSupport()
    {
        using var sql = CreateConnection();

        var info = sql.GetDbRuntimeSetting();

        return string.Equals(info?.EngineName, "MySQL", StringComparison.OrdinalIgnoreCase) && info?.EngineVersion >= new Version(9, 0); // Vector support was added with Oracle 23c
    }
}
