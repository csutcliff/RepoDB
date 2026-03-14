# Bulk Operations

High-performance bulk insert, update, delete, and merge operations for different databases.

## Bulk Operations Overview

Bulk operations allow you to perform large-scale data operations with maximum performance. They bypass database constraints and logging to achieve speeds that are often 90%+ faster than regular batch operations.

A bulk operation is a process of bringing all the data from the application into the database server at once. It ignores some database specific activities (i.e.: Logging, Audits, Data-Type Checks, Constraints, etc) behind the scene, thus gives you maximum performance during the operation.

Basically, you mostly do the normal [Delete](/docs/operations/delete.md), [Insert](/docs/operations/insert.md), [Merge](/docs/operations/merge.md) and [Update](/docs/operations/update.md) operations when interacting with the database. Through this, the data is being processed in an atomic way. If you do call the [batch operations](/docs/features/batchoperations.md), it only execute the multiple single-operations together and does not completely eliminate the round-trips between your application and the database.

With the bulk operations, as mentioned above, all data is brought from the client application into the database at one go. Once the data is on the server, it is then being processed together within the database (server), maximizing the performance.

The bulk operations can improve the performance by more than 90% when processing a large dataset.

## Recommendation

Below are the items you may need to consider when to use the right operations (Bulk vs [Batch](/docs/features/batchoperations.md)).

- Network Latency
- Infrastructure
- No. of Columns
- Type of Data

Though there is no standard recommendation of when to use what, when using the library, we recommend to use the bulk operations if the datasets you are working is beyond 1000. Otherwhise, use the [batch](/docs/features/batchoperations.md) operations.

## Database-Specific Implementations

- **[SQL Server](/docs/features/bulkoperations/sqlserver.md)** - BulkInsert, BulkUpdate, BulkDelete, BulkMerge
- **[PostgreSQL](/docs/features/bulkoperations/postgresql.md)** - BinaryBulkInsert, BinaryBulkUpdate, BinaryBulkDelete, BinaryBulkMerge

## General Bulk Operations

- [BulkInsert](/docs/operations/bulkinsert.md) - High-performance insert operation
- [BulkUpdate](/docs/operations/bulkupdate.md) - High-performance update operation
- [BulkDelete](/docs/operations/bulkdelete.md) - High-performance delete operation
- [BulkMerge](/docs/operations/bulkmerge.md) - High-performance merge operation

## PostgreSQL-Specific Operations

- [BinaryBulkInsert](/docs/operations/binarybulkinsert.md)
- [BinaryBulkUpdate](/docs/operations/binarybulkupdate.md)
- [BinaryBulkDelete](/docs/operations/binarybulkdelete.md)
- [BinaryBulkDeleteByKey](/docs/operations/binarybulkdeletebykey.md)
- [BinaryBulkMerge](/docs/operations/binarybulkmerge.md)
- [BinaryImport](/docs/operations/binaryimport.md)

---

[← Back to Features](/docs/features/README.md)
