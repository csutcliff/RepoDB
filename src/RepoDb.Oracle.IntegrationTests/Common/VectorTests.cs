
using RepoDb.Oracle.IntegrationTests.Setup;

namespace RepoDb.Oracle.IntegrationTests.Common;

[TestClass]
public class VectorTests : TestCore.VectorTestsBase<OracleDbInstance>
{
    protected override void InitializeCore() => Database.Initialize();


    public override bool HaveVectorSupport()
    {
        using var sql = CreateConnection();

        var info = sql.GetDbRuntimeSetting();

        return info?.EngineVersion.Major >= 23; // Vector support was added with Oracle 23c
    }
}
