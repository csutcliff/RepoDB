using System.Collections;
using System.Diagnostics;
using System.Linq.Expressions;

namespace RepoDb;

/// <summary>
///
/// </summary>
[DebuggerDisplay($"{{{nameof(DebuggerDisplay)},nq}}")]
public sealed class FieldSet : IReadOnlyCollection<Field>
#if NET
    , IReadOnlySet<Field>
#endif
{
    private readonly HashSet<Field> _fields;
    private static readonly HashSet<Field> EmptyFields = new(Field.CompareByName);
    private int? _hashCode;

    private FieldSet()
    {
        _fields = EmptyFields;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="fields"></param>
    public FieldSet(IEnumerable<Field> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);

        // Copy inner hashset to avoid using unneeded intermediates
        _fields = new HashSet<Field>(fields is FieldSet fs ? fs._fields : fields, Field.CompareByName);
    }

    /// <summary>
    ///
    /// </summary>
    public static readonly FieldSet Empty = new();

    /// <inheritdoc/>>
    public int Count => _fields.Count;

    /// <inheritdoc/>>
    public bool Contains(Field item)
    {
        return _fields.Contains(item);
    }

    /// <inheritdoc/>>
    public IEnumerator<Field> GetEnumerator()
    {
        return _fields.GetEnumerator();
    }

    /// <inheritdoc/>>
    public bool IsProperSubsetOf(IEnumerable<Field> other)
    {
        return _fields.IsProperSubsetOf(other);
    }

    /// <inheritdoc/>>
    public bool IsProperSupersetOf(IEnumerable<Field> other)
    {
        return _fields.IsProperSupersetOf(other);
    }

    /// <inheritdoc/>>
    public bool IsSubsetOf(IEnumerable<Field> other)
    {
        return _fields.IsSubsetOf(other);
    }

    /// <inheritdoc/>>
    public bool IsSupersetOf(IEnumerable<Field> other)
    {
        return _fields.IsSupersetOf(other);
    }

    /// <inheritdoc/>>
    public bool Overlaps(IEnumerable<Field> other)
    {
        return _fields.Overlaps(other);
    }

    /// <inheritdoc/>>
    public bool SetEquals(IEnumerable<Field> other)
    {
        return _fields.SetEquals(other);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <inheritdoc/>>
    public FieldSet Union(IEnumerable<Field> other)
    {
        if (IsSupersetOf(other))
            return this;

        return new FieldSet(_fields.Concat(other));
    }

    /// <inheritdoc/>>
    public override bool Equals(object? obj)
    {
        if (obj is not FieldSet fs || fs.Count != Count)
            return false;

        return SetEquals(fs);
    }
    /// <inheritdoc/>>
    public override int GetHashCode()
    {
        return _hashCode ??= HashCode.Combine(Count, _fields.Aggregate(0, (current, field) => current ^ field.GetHashCode()));
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="objA"></param>
    /// <param name="objB"></param>
    /// <returns></returns>
    public static bool operator ==(FieldSet? objA, FieldSet? objB)
        => ReferenceEquals(objA, objB) || (objA?.Equals(objB) == true);

    /// <summary>
    ///
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static bool operator !=(FieldSet? left, FieldSet? right)
        => !(left == right);

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <returns></returns>
    public static FieldSet From<TEntity>()
        where TEntity : class
    {
        return new FieldSet(FieldCache.Get<TEntity>());
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="from"></param>
    /// <returns></returns>
    public static FieldSet Parse<TEntity>(Expression<Func<TEntity, object?>> from)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(from);
        return Field.Parse<TEntity>(from);
    }

    private string DebuggerDisplay => $"{Count} fields: " + string.Join(",", _fields.Select(x => x.FieldName));
}
