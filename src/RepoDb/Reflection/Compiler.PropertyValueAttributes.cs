using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using RepoDb.Attributes.Parameter;
using RepoDb.Extensions;

namespace RepoDb.Reflection;

internal partial class Compiler
{
    #region PropertyValueAttribute

    private static List<Expression>? GetParameterPropertyValueSetterAttributesAssignmentExpressions(
        Expression dbParameterExpression,
        ClassProperty? classProperty)
    {
        var attributes = classProperty?.GetPropertyValueAttributes();
        if (attributes?.Any() != true)
        {
            return null;
        }

        List<Expression>? expressions = null;

        foreach (var attribute in attributes)
        {
            var exclude = !attribute.IncludedInCompilation ||
                string.Equals(nameof(IDbDataParameter.ParameterName), attribute.PropertyName, StringComparison.OrdinalIgnoreCase);

            if (exclude)
            {
                continue;
            }

            if (GetPropertyValueAttributesAssignmentExpression(dbParameterExpression, attribute) is { } expression)
            {
                expressions ??= [];
                expressions.Add(expression);
            }
        }

        return expressions;
    }

    private static MethodCallExpression GetPropertyValueAttributesAssignmentExpression(
        ParameterExpression dbParameterExpression,
        PropertyValueAttribute attribute) =>
        GetPropertyValueAttributesAssignmentExpression((Expression)dbParameterExpression, attribute);

    private static MethodCallExpression GetPropertyValueAttributesAssignmentExpression(
        Expression parameterExpression,
        PropertyValueAttribute attribute)
    {
        ArgumentNullException.ThrowIfNull(attribute);

        // The problem to this is because of the possibilities of multiple attributes configured for
        // DB multiple providers within a single entity and if the parameterExpression is not really
        // covertible to the target attriute.ParameterType

        var method = GetPropertyValueAttributeSetValueMethod();
        return Expression.Call(Expression.Constant(attribute), method, parameterExpression);
    }

    private static MethodInfo GetPropertyValueAttributeSetValueMethod() =>

        StaticType.PropertyValueAttribute.GetMethod(nameof(PropertyValueAttribute.SetValue),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    #endregion
}
