
# CacheItem

---

This class is used as an item for the [ICache](/interface/icache) object. It implements the [IExpirable](/interface/iexpirable) interface.

## Properties

Below is the list of properties.

| Name | Description |
|:-----|:------------|
| Key | The key to the cache. |
| Value | The value of the class (a generic type). |
| CacheItemExpiration | The expiration of the cache in minutes. |

## Creating New Instance

Below is the way on how to create a new instance of this class.

```csharp
var expirationInMinutes = 180;
var item = new CacheItem("Key", "Value", expirationInMinutes);
```

## Passing to the Cache

Usually, the [ICache](/interface/icache) object is embedded within the repository. Let us say you have a repository named `PersonRepository`.

The code below is an explicit call to add a cache in a manual manner.

```csharp
var people = GetPeople();
var expirationInMinutes = 180;
var item = new CacheItem<IEnumerable<Person>>("CacheKey:ActivePeople", people, expirationInMinutes);

using (var repository = new PersonRepository())
{
    repository.Cache.Add(item);
}
```
> Usually, the cache will automatically be set by passing a value to the `cacheKey` argument when calling the [Query](/docs/operations/query.md) or [BatchQuery](/docs/operations/batchquery.md) operations.

## Retrieving from the Cache

To retrieve an item from the cache, directly access the [ICache](/interface/icache) object and call the `Get()` method passing the key.

Let us say, the repository `PersonRepository` is existing.

```csharp
var item = repository.Cache.Get<IEnumerable<Person>>("CacheKey:ActivePeople");
```
> If the cache is not found on the given key, by default it will throw an exception. You can set the `throwException` argument to `false` if you wish not to throw an exception.

