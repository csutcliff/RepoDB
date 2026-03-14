using System.Linq.Expressions;
using System.Reflection;
using RepoDb.Options;

namespace RepoDb.Reflection;

internal partial class Compiler
{
    #region ClassHandlers

    private static MethodCallExpression CreateClassHandlerGetOptionsExpression(Expression readerExpression)
    {
        // Get the 'Create' method
        var method = GetMethodInfo(() => ClassHandlerGetOptions.Create(null!));

        // Set to default
        readerExpression ??= Expression.Default(StaticType.DbDataReader);

        // Call the method
        return Expression.Call(method, readerExpression);
    }

    private static MethodCallExpression CreateClassHandlerSetOptionsExpression(Expression commandExpression)
    {
        // Get the 'Create' method
        var method = GetMethodInfo(() => ClassHandlerSetOptions.Create(null!));

        // Set to default
        commandExpression ??= Expression.Default(StaticType.IDbDataParameter);

        // Call the method
        return Expression.Call(method, commandExpression);
    }

    #endregion

    #region PropertyHandlers

    private static MethodCallExpression CreatePropertyHandlerGetOptionsExpression(Expression readerExpression,
        ClassProperty? classProperty) =>
        CreatePropertyHandlerGetOptionsExpression(readerExpression,
            classProperty == null ? null : Expression.Constant(classProperty));

    private static MethodCallExpression CreatePropertyHandlerGetOptionsExpression(Expression readerExpression,
        Expression? classPropertyExpression)
    {
        // Get the 'Create' method
        var method = StaticType.PropertyHandlerGetOptions
            .GetMethod(nameof(PropertyHandlerGetOptions.Create), BindingFlags.Static | BindingFlags.NonPublic)!;

        // Set to default
        readerExpression ??= Expression.Default(StaticType.DbDataReader);
        classPropertyExpression ??= Expression.Default(StaticType.ClassProperty);

        // Call the method
        return Expression.Call(method, readerExpression, classPropertyExpression);
    }

    private static MethodCallExpression CreatePropertyHandlerSetOptionsExpression(Expression? parameterExpression,
        ClassProperty? classProperty) =>
        CreatePropertyHandlerSetOptionsExpression(parameterExpression,
            classProperty == null ? null : Expression.Constant(classProperty));

    private static MethodCallExpression CreatePropertyHandlerSetOptionsExpression(Expression? parameterExpression,
        Expression? classPropertyExpression)
    {
        // Get the 'Create' method
        var method = StaticType.PropertyHandlerSetOptions.GetMethod(nameof(PropertyHandlerSetOptions.Create),
            BindingFlags.Static | BindingFlags.NonPublic)!;

        // Set to default
        parameterExpression ??= Expression.Default(StaticType.IDbDataParameter);
        classPropertyExpression ??= Expression.Default(StaticType.ClassProperty);

        // Call the method
        return Expression.Call(method, parameterExpression, classPropertyExpression);
    }

    #endregion
}
