# Attributes

Entity and property attributes for mapping and configuration.

## Entity Attributes

- **[Map](/docs/attributes/map.md)** - Map class to table name
- **[Primary](/docs/attributes/primary.md)** - Mark property as primary key
- **[Identity](/docs/attributes/identity.md)** - Mark property as identity column
- **[ClassHandler](/docs/attributes/classhandler.md)** - Custom class transformation handler

## Property Attributes

- **[PropertyHandler](/docs/attributes/propertyhandler.md)** - Custom property transformation handler
- **[TypeMap](/docs/attributes/typemap.md)** - Map property to specific database type

## Parameter Attributes

- **[Parameter](/docs/attributes/parameter/parameter.md)** - Base parameter configuration
- **[DbType](/docs/attributes/parameter/dbtype.md)** - Specify database type
- **[Direction](/docs/attributes/parameter/direction.md)** - Parameter direction (Input/Output/etc)
- **[IsNullable](/docs/attributes/parameter/isnullable.md)** - Mark parameter as nullable
- **[Name](/docs/attributes/parameter/name.md)** - Custom parameter name
- **[Precision](/docs/attributes/parameter/precision.md)** - Numeric precision
- **[Scale](/docs/attributes/parameter/scale.md)** - Numeric scale
- **[Size](/docs/attributes/parameter/size.md)** - Parameter size

## Database-Specific Attributes

### SQL Server

- **[SqlServer](/docs/attributes/sqlserver/sqlserver.md)** - SQL Server specific configuration
- **[SqlDbType](/docs/attributes/sqlserver/sqldbtype.md)** - SQL Server type specification
- **[TypeName](/docs/attributes/sqlserver/typename.md)** - Custom type name
- **[UdtTypeName](/docs/attributes/sqlserver/udttypename.md)** - User-defined type name
- **[XmlSchemaCollectionName](/docs/attributes/sqlserver/xmlschemacollectionname.md)** - XML schema collection
- **[XmlSchemaCollectionOwningSchema](/docs/attributes/sqlserver/xmlschemacollectionowningschema.md)** - XML schema owner
- **[XmlSchemaCollectionDatabase](/docs/attributes/sqlserver/xmlschemacollectiondatabase.md)** - XML schema database
- **[ForceColumnEncryption](/docs/attributes/sqlserver/forcecolumnencryption.md)** - Force encryption
- **[CompareInfo](/docs/attributes/sqlserver/compareinfo.md)** - Collation comparison info
- **[LocaleId](/docs/attributes/sqlserver/localeid.md)** - Locale identifier
- **[Offset](/docs/attributes/sqlserver/offset.md)** - Time zone offset

### PostgreSQL

- **[Npgsql](/docs/attributes/npgsql/npgsql.md)** - PostgreSQL specific configuration
- **[NpgsqlDbType](/docs/attributes/npgsql/npgsqldbtype.md)** - PostgreSQL type specification
- **[DataTypeName](/docs/attributes/npgsql/datatypename.md)** - PostgreSQL data type name
- **[ConvertedValue](/docs/attributes/npgsql/convertedvalue.md)** - Value conversion handler

### MySQL

- **[MySql](/docs/attributes/mysql/mysql.md)** - MySQL specific configuration
- **[MySqlDbType](/docs/attributes/mysql/mysqldbtype.md)** - MySQL type specification

### SQLite

- **[Sqlite](/docs/attributes/sqlite/sqlite.md)** - SQLite specific configuration
- **[SqliteType](/docs/attributes/sqlite/sqlitetype.md)** - SQLite type specification
- **[TypeName](/docs/attributes/sqlite/typename.md)** - Custom type name

---

[← Back to Documentation](/docs/README.md)
