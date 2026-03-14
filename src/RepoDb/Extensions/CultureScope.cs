using System.Globalization;

namespace RepoDb.Extensions;

/// <summary>
/// store current culture and set CultureInfo.DefaultThreadCurrentCulture for unit test case.
/// restore original culture when dispose.
/// </summary>
public readonly struct CultureScope : IDisposable
{
    private readonly CultureInfo originalCulture;

    /// <summary>
    /// Temporarily set the current culture to the specified culture for the scope of this instance.
    /// </summary>
    /// <param name="setCulture"></param>
    public CultureScope(CultureInfo setCulture)
    {
        originalCulture = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = setCulture ?? CultureInfo.InvariantCulture;
    }

    /// <summary>
    /// Restores the original culture that was in place before this instance was created.
    /// </summary>
    public void Dispose()
    {
        CultureInfo.CurrentCulture = originalCulture;
    }
}
