using System.Data.Common;

namespace RepoDb.Options;

/// <summary>
/// An option class that is containing the optional values when pushing the class properties values towards the database.
/// </summary>
public sealed class ClassHandlerGetOptions : ClassHandlerOptions
{
    internal ClassHandlerGetOptions(DbDataReader reader)
    {
        DataReader = reader;
    }

    #region Properties

    /// <summary>
    /// Gets the associated <see cref="DbDataReader"/> object in used during the hydration process.
    /// </summary>
    public DbDataReader DataReader { get; }

    #endregion

    #region Methods

    internal static ClassHandlerGetOptions Create(DbDataReader reader) =>
        new ClassHandlerGetOptions(reader);

    #endregion
}
