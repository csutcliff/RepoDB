using Oracle.ManagedDataAccess.Client;

namespace RepoDb.Oracle.System.UnitTests;

[TestClass]
public class MappingTest
{
    [TestInitialize]
    public void Initialize()
    {
        GlobalConfiguration
            .Setup()
            .UseOracle();
    }

    #region Oracle

    [TestMethod]
    public void TestOracleStatementBuilderMapper()
    {
        // Setup
        var builder = StatementBuilderMapper.Get<OracleConnection>();

        // Assert
        Assert.IsNotNull(builder);
    }

    [TestMethod]
    public void TestOracleHelperMapper()
    {
        // Setup
        var helper = DbHelperMapper.Get<OracleConnection>();

        // Assert
        Assert.IsNotNull(helper);
    }

    [TestMethod]
    public void TestOracleSettingMapper()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Assert
        Assert.IsNotNull(setting);
    }

    #endregion
}
