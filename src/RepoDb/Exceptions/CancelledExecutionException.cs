namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown when the operation has been cancelled during the tracing.
/// </summary>
public class CancelledExecutionException : OperationCanceledException
{
    /// <summary>
    /// Creates a new instance of <see cref="CancelledExecutionException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public CancelledExecutionException(string message)
        : base(message) { }

    /// <summary>
    ///
    /// </summary>
    public CancelledExecutionException()
    {
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="message"></param>
    /// <param name="innerException"></param>
    public CancelledExecutionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
