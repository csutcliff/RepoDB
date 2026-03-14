
# KeyColumnReturnBehavior

---

This enum is used to define how the push operations (i.e.: [Insert](/docs/operations/insert.md), [InsertAll](/docs/operations/insertall.md), [Merge](/docs/operations/merge.md) and [MergeAll](/docs/operations/mergeall.md)) behaves when returning the value from the column columns (i.e.: Primary and Identity).

### Enum Values

Below is the list of enum values.

| Name | Description |
|:-----|:------------|
| Primary | Returns the value of the primary column. |
| Identity | Returns the value of the identity column. |
| PrimaryOrElseIdentity | Returns the coalesced value of the primary and identity columns. This is the default value. |
| IdentityOrElsePrimary | Returns the coalesced value of the identity and primary columns. |
