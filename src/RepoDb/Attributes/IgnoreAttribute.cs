namespace RepoDb.Attributes;

/// <summary>
/// Specifies that a property should be ignored by certain processing or serialization operations.
/// </summary>
/// <remarks>Apply this attribute to a property to indicate that it should be excluded from specific frameworks or
/// tools that recognize the attribute, such as serializers or mappers. Multiple instances can be applied to the same
/// property to support different scenarios or frameworks. The effect of this attribute depends on the consuming library
/// or framework.</remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class IgnoreAttribute : Attribute
{
}
