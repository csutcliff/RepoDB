# Getting Started

Choose your database platform and follow the guide:

## Supported Databases

- **[SQL Server](/docs/getstarted/sqlserver.md)** - Full-featured support with Microsoft.Data.SqlClient
- **[PostgreSQL](/docs/getstarted/postgresql.md)** - Advanced open-source database  
- **[MySQL](/docs/getstarted/mysql.md)** - Popular relational database
- **[SQLite](/docs/getstarted/sqlite.md)** - Perfect for testing and embedded scenarios
- **[Oracle](/docs/getstarted/oracle.md)** - Enterprise database support

## Installation Steps

1. Install the core package and your database provider:
   ```
   Install-Package AmpScm.RepoDb
   Install-Package AmpScm.RepoDb.SqlServer    # or your database package
   ```

2. Initialize RepoDB in your application:
   ```csharp
   using RepoDb;

   GlobalConfiguration.Setup().UseSqlServer();  // or your database
   ```

3. Start using RepoDB with your database connection!

## What's Next?

- Learn about [Features](/docs/features/README.md)
- Explore [Operations](/docs/operations/README.md)
- Check out [Class Mapping](/docs/features/classmapping.md)

---

[← Back to Documentation](/docs/README.md)
