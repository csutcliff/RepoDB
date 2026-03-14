namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown when the parameter object is not found.
/// </summary>
public class ParameterNotFoundException : ArgumentException
{
    /// <summary>
    /// Creates a new instance of <see cref="ParameterNotFoundException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public ParameterNotFoundException(string message)
        : base(message: message, innerException: null) { }

    /// <inheritdoc />
    public ParameterNotFoundException()
    {
    }

    /// <inheritdoc />
    public ParameterNotFoundException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
