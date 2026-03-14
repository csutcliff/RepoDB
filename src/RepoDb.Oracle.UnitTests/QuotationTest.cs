using Oracle.ManagedDataAccess.Client;
using RepoDb.Extensions;

namespace RepoDb.Oracle.System.UnitTests;

[TestClass]
public class QuotationTest
{
    [TestInitialize]
    public void Initialize()
    {
        GlobalConfiguration
            .Setup()
            .UseOracle();
    }

    #region Oracle

    #region AsQuoted

    [TestMethod]
    public void TestOracleQuotationForQuotedAndTrimmed()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = " Field ".AsQuoted(true, setting);

        // Assert
        Assert.AreEqual("\"Field\"", result);
    }

    [TestMethod]
    public void TestOracleQuotationForQuotedNonTrimmed()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = " Field ".AsQuoted(setting);

        // Assert
        Assert.AreEqual("\" Field \"", result);
    }

    [TestMethod]
    public void TestOracleQuotationForQuotedForPreQuoted()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = "\"Field\"".AsQuoted(setting);

        // Assert
        Assert.AreEqual("\"Field\"", result);
    }

    [TestMethod]
    public void TestOracleQuotationForQuotedForPreQuotedWithSpace()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = "\" Field \"".AsQuoted(setting);

        // Assert
        Assert.AreEqual("\" Field \"", result);
    }

    [TestMethod]
    public void TestOracleQuotationForQuotedForPreQuotedWithSpaceAndTrimmed()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = " \" Field \" ".AsQuoted(true, setting);

        // Assert
        Assert.AreEqual("\" Field \"", result);
    }

    #endregion

    #region AsUnquoted

    [TestMethod]
    public void TestOracleQuotationForUnquotedAndTrimmed()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = " \" Field \" ".AsUnquoted(true, setting);

        // Assert
        Assert.AreEqual("Field", result);
    }

    [TestMethod]
    public void TestOracleQuotationForUnquotedNonTrimmed()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = "\" Field \"".AsUnquoted(setting);

        // Assert
        Assert.AreEqual(" Field ", result);
    }

    [TestMethod]
    public void TestOracleQuotationForUnquotedForPlain()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = "Field".AsUnquoted(setting);

        // Assert
        Assert.AreEqual("Field", result);
    }

    [TestMethod]
    public void TestOracleQuotationForUnquotedForPlainWithSpace()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = " Field ".AsUnquoted(setting);

        // Assert
        Assert.AreEqual(" Field ", result);
    }

    [TestMethod]
    public void TestOracleQuotationForUnquotedAndTrimmedForPlainWithSpace()
    {
        // Setup
        var setting = DbSettingMapper.Get<OracleConnection>();

        // Act
        var result = " Field ".AsUnquoted(true, setting);

        // Assert
        Assert.AreEqual("Field", result);
    }

    #endregion

    #endregion
}
