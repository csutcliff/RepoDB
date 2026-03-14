using System.Collections.Concurrent;
using System.Linq.Expressions;
using RepoDb.Attributes;
using RepoDb.Exceptions;
using RepoDb.Extensions;

namespace RepoDb;

/// <summary>
/// A class that is being used to set a class property to be an primary property. This is an alternative class to <see cref="PrimaryAttribute"/> object.
/// </summary>
public static class PrimaryMapper
{
    private static readonly ConcurrentDictionary<Type, object> maps = new();

    #region Methods

    /// <summary>
    /// Adds a primary property mapping into a target class (via expression).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="expression">The expression to be parsed.</param>
    public static void Add<TEntity>(Expression<Func<TEntity, object?>> expression)
        where TEntity : class =>
        Add(expression, false);

    /// <summary>
    /// Adds a primary property mapping into a target class (via expression).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="expression">The expression to be parsed.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    public static void Add<TEntity>(Expression<Func<TEntity, object?>> expression,
        bool force)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(expression);

        // Get the property
        if (expression.Body is NewExpression nx
            && nx.Members is { } members)
        {
            // Add to the mapping
            Add<TEntity>(force, [.. members.Select(x => DataEntityExtension.GetClassPropertyOrThrow<TEntity>(x.Name))]);
            return;
        }

        var property = ExpressionExtension.GetProperty(expression);

        // Add to the mapping
        Add<TEntity>(DataEntityExtension.GetClassPropertyOrThrow<TEntity>(property?.Name), force);
    }

    /// <summary>
    /// Adds a primary property mapping into a target class (via property name).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="propertyName">The name of the class property to be mapped.</param>
    public static void Add<TEntity>(string propertyName)
        where TEntity : class =>
        Add<TEntity>(propertyName, false);


    /// <summary>
    /// Adds a primary property mapping into a target class (via property name).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="propertyNames">The names of the class property to be mapped.</param>
    public static void Add<TEntity>(params string[] propertyNames)
        where TEntity : class =>
        Add<TEntity>(false, propertyNames);

    /// <summary>
    /// Adds a primary property mapping into a target class (via property name).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="propertyName">The name of the class property to be mapped.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    public static void Add<TEntity>(string propertyName,
        bool force)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(propertyName);

        // Add to the mapping
        Add<TEntity>(DataEntityExtension.GetClassPropertyOrThrow<TEntity>(propertyName), force);
    }

    /// <summary>
    /// Adds a primary property mapping into a target class (via property name).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    /// <param name="propertyNames">The names of the class property to be mapped.</param>
    public static void Add<TEntity>(bool force, params string[] propertyNames)
        where TEntity : class
    {
        var props = (propertyNames ?? []).Select(propertyName => DataEntityExtension.GetClassPropertyOrThrow<TEntity>(propertyName)).ToArray();

        // Add to the mapping
        Add<TEntity>(force, props);
    }

    /// <summary>
    /// Adds a primary property mapping into a target class (via <see cref="Field"/> object).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="field">The instance of <see cref="Field"/> object to be mapped.</param>
    public static void Add<TEntity>(Field field)
        where TEntity : class =>
        Add<TEntity>(field, false);

    /// <summary>
    /// Adds a primary property mapping into a target class (via <see cref="Field"/> object).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="field">The instance of <see cref="Field"/> object to be mapped.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    public static void Add<TEntity>(Field field,
        bool force)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(field);

        // Add to the mapping
        Add<TEntity>(DataEntityExtension.GetClassPropertyOrThrow<TEntity>(field.FieldName), force);
    }

    /// <summary>
    /// Adds a primary property mapping into a target class (via <see cref="Field"/> object).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="fields">The instances of <see cref="Field"/> objects to be mapped.</param>
    public static void Add<TEntity>(params Field[] fields)
        where TEntity : class =>
        Add<TEntity>(false, fields);

    /// <summary>
    /// Adds a primary property mapping into a target class (via <see cref="Field"/> object).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    /// <param name="fields">The instance of <see cref="Field"/> object to be mapped.</param>
    public static void Add<TEntity>(bool force,
        params Field[] fields)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(fields);

        Add<TEntity>(force, [.. fields.Select(x => x.FieldName)]);
    }

    /// <summary>
    /// Adds a primary property mapping into a <see cref="ClassProperty"/> object.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="classProperty">The instance of <see cref="ClassProperty"/> to be mapped.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    internal static void Add<TEntity>(ClassProperty classProperty,
        bool force)
        where TEntity : class =>
        Add(typeof(TEntity), force, classProperty);

    /// <summary>
    /// Adds a primary property mapping into a <see cref="ClassProperty"/> object.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="classProperties">The instance of <see cref="ClassProperty"/> to be mapped.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    internal static void Add<TEntity>(bool force,
        params ClassProperty[] classProperties)
        where TEntity : class =>
        Add(typeof(TEntity), force, classProperties);

    /// <summary>
    /// Adds a primary property mapping into a <see cref="ClassProperty"/> object.
    /// </summary>
    /// <param name="type">The type of the data entity.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    /// <param name="classProperty">The instance of <see cref="ClassProperty"/> to be mapped.</param>
    internal static void Add(Type type,
        bool force,
        ClassProperty classProperty)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(classProperty);

        // Try get the cache
        if (maps.TryGetValue(type, out var value))
        {
            if (force)
            {
                maps.TryUpdate(type, classProperty, value);
            }
            else
            {
                throw new MappingExistsException("The mapping is already existing.");
            }
        }
        else
        {
            maps.TryAdd(type, classProperty);
        }
    }

    /// <summary>
    /// Adds a primary property mapping into a <see cref="ClassProperty"/> object.
    /// </summary>
    /// <param name="type">The type of the data entity.</param>
    /// <param name="force">A value that indicates whether to force the mapping. If one is already exists, then it will be overwritten.</param>
    /// <param name="classProperties">The instance of <see cref="ClassProperty"/> to be mapped.</param>
    internal static void Add(Type type,
        bool force,
        ClassProperty[] classProperties)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(classProperties);

        // Try get the cache
        if (maps.TryGetValue(type, out var value))
        {
            if (force)
            {
                maps.TryUpdate(type, classProperties, value);
            }
            else
            {
                throw new MappingExistsException("The mapping is already existing.");
            }
        }
        else
        {
            maps.TryAdd(type, classProperties);
        }
    }

    /*
     * Get
     */

    /// <summary>
    /// Get the exising mapped primary property of the target class.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <returns>An instance of the mapped <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get<TEntity>()
        where TEntity : class =>
        Get(typeof(TEntity));


    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <returns></returns>
    public static ClassProperty[]? GetPrimaryKeys<TEntity>()
        where TEntity : class =>
        GetPrimaryKeys(typeof(TEntity));

    /// <summary>
    /// Get the exising mapped primary property of the target class.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <returns>An instance of the mapped <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        // Try get the value
        maps.TryGetValue(type, out var value);

        // Return the value
        return value as ClassProperty ?? (value as ClassProperty[])?[0];
    }

    /// <summary>
    /// Get the exising mapped primary property of the target class.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <returns>An instance of the mapped <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty[]? GetPrimaryKeys(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        // Try get the value
        if (maps.TryGetValue(type, out var value))
        {
            return value is ClassProperty c ? [c] : value as ClassProperty[];
        }

        return null;
    }

    /*
     * Remove
     */

    /// <summary>
    /// Removes the existing mapped primary property of the class.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    public static void Remove<TEntity>()
        where TEntity : class =>
        Remove(typeof(TEntity));

    /// <summary>
    /// Removes the existing mapped primary property of the class.
    /// </summary>
    /// <param name="type">The target type.</param>
    public static void Remove(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        // Try get the value
        maps.TryRemove(type, out var _);
    }

    /*
     * Clear
     */

    /// <summary>
    /// Clears all the existing cached primary properties.
    /// </summary>
    public static void Clear() =>
        maps.Clear();

    #endregion
}
