# 🐘 Unleasharp.DB.PostgreSQL

[![NuGet version (Unleasharp.DB.PostgreSQL)](https://img.shields.io/nuget/v/Unleasharp.DB.PostgreSQL.svg?style=flat-square)](https://www.nuget.org/packages/Unleasharp.DB.PostgreSQL/)

[![Unleasharp.DB.PostgreSQL](https://socialify.git.ci/TraberSoftware/Unleasharp.DB.PostgreSQL/image?description=1&font=Inter&logo=https%3A%2F%2Fraw.githubusercontent.com%2FTraberSoftware%2FUnleasharp%2Frefs%2Fheads%2Fmain%2Fassets%2Flogo-small.png&name=1&owner=1&pattern=Circuit+Board&theme=Light)](https://github.com/TraberSoftware/Unleasharp.DB.PostgreSQL)

PostgreSQL implementation of Unleasharp.DB.Base. This repository provides a PostgreSQL-specific implementation that leverages the base abstraction layer for common database operations.

## 📦 Installation

Install the NuGet package using one of the following methods:

### Package Manager Console
```powershell
Install-Package Unleasharp.DB.PostgreSQL
```

### .NET CLI
```bash
dotnet add package Unleasharp.DB.PostgreSQL
```

### PackageReference (Manual)
```xml
<PackageReference Include="Unleasharp.DB.PostgreSQL" Version="1.2.1" />
```

## 🎯 Features

- **PostgreSQL-Specific Query Rendering**: Custom query building and rendering tailored for PostgreSQL
- **Connection Management**: Robust connection handling through ConnectorManager
- **Query Builder Integration**: Seamless integration with the base QueryBuilder
- **Schema Definition Support**: Full support for table and column attributes

## 🚀 Connection Initialization

The `ConnectorManager` handles database connections. You can initialize it using a connection string, `NpgsqlConnectionStringBuilder` or `NpgsqlDataSourceBuilder`.

### Using Connection String
```csharp
ConnectorManager DBConnector = new ConnectorManager("Host=localhost;Port=5432;Database=unleasharp;Username=unleasharp;Password=unleasharp;Include Error Detail=true");
```

### Using Fluent Configuration
```csharp
ConnectorManager dbConnector = new ConnectorManager("Host=localhost;Port=5432;Database=unleasharp;Username=unleasharp;Password=unleasharp;Include Error Detail=true")
    .WithAutomaticConnectionRenewal(true)
    .WithAutomaticConnectionRenewalInterval(TimeSpan.FromHours(1))
    .WithDataSourceBuilderSetup(sourceBuilder => {
        sourceBuilder.MapEnum<EnumExample>("enumexample");
    })
    .WithOnQueryExceptionAction(ex => Console.WriteLine(ex.Message))
;
```

### Using NpgsqlConnectionStringBuilder
```csharp
ConnectorManager dbConnector = new ConnectorManager(
    new NpgsqlConnectionStringBuilder("Host=localhost;Port=5432;Database=unleasharp;Username=unleasharp;Password=unleasharp;")
);
```

### Using NpgsqlDataSourceBuilder
```csharp
ConnectorManager dbConnector = new ConnectorManager(
    new NpgsqlDataSourceBuilder("Host=localhost;Port=5432;Database=unleasharp;Username=unleasharp;Password=unleasharp;Include Error Detail=true")
);
```

## 📝 Usage Examples

### Sample Table Structure

```csharp
using System.ComponentModel;
using Unleasharp.DB.Base.SchemaDefinition;

namespace Unleasharp.DB.PostgreSQL.Sample;

[Table("example_table")]
[PrimaryKey("id")]
[UniqueKey("id", "id", "_enum")]
public class ExampleTable {
	[Column("id", ColumnDataType.UInt64, Unsigned = true, PrimaryKey = true, AutoIncrement = true, NotNull = true)]
	public long? Id { get; set; }

	[Column("_mediumtext", ColumnDataType.Text)]
	public string MediumText  { get; set; }

	[Column("_longtext", ColumnDataType.Text)]
	public string Longtext { get; set; }

	[Column("_json", ColumnDataType.Json)]
	public string Json { get; set; }

	[Column("_longblob", ColumnDataType.Binary)]
	public byte[] CustomFieldName { get; set; }

	[Column("_enum", ColumnDataType.Enum)]
	public EnumExample? Enum { get; set; }

	[Column("_varchar", "varchar", Length = 255)]
	public string Varchar { get; set; }
}

public enum EnumExample {
    [PgName("Y")]
    Y,
    [PgName("NEGATIVE")]
    N
}
```

### Create Enum Types
```csharp
ConnectorManager dbConnector = new ConnectorManager();
dbConnector.QueryBuilder().Build(query => query.CreateEnumType<EnumExample>()).Execute().DBQuery.Render();
```

### Enum Types Mapping

#### Using Fluent Configuration
```csharp
ConnectorManager dbConnector = new ConnectorManager()
    .WithDataSourceBuilderSetup(sourceBuilder => {
        sourceBuilder.MapEnum<EnumExample>("enumexample");
    })
;
```

#### Using ConnectorManager.DataSourceBuilder
```csharp
ConnectorManager dbConnector = new ConnectorManager();
dbConnector.QueryBuilder().Build(query => query.CreateEnumType<EnumExample>()).Execute().DBQuery.Render();
dbConnector.DataSourceBuilder.MapEnum<EnumExample>("enumexample");
```

### Sample Program

```csharp
using Npgsql;
using Unleasharp.DB.PostgreSQL;
using Unleasharp.DB.Base.QueryBuilding;

namespace Unleasharp.DB.PostgreSQL.Sample;

internal class Program 
{
    static void Main(string[] args) 
    {
        // Initialize database connection
        ConnectorManager dbConnector = new ConnectorManager(new NpgsqlDataSourceBuilder("Host=localhost;Port=5432;Database=unleasharp;Username=unleasharp;Password=unleasharp;Include Error Detail=true"))
            .WithAutomaticConnectionRenewal(true)
            .WithAutomaticConnectionRenewalInterval(TimeSpan.FromHours(1))
            .WithDataSourceBuilderSetup(sourceBuilder => {
                // Map Enum types
                sourceBuilder.MapEnum<EnumExample>("enumexample");
            })
            .WithOnQueryExceptionAction(ex => Console.WriteLine(ex.Message))
        ;
        
        // Create Enum types if needed
        dbConnector.QueryBuilder().Build(query => query.CreateEnumType<EnumExample>()).Execute();
        // Create table if needed
        dbConnector.QueryBuilder().Build(Query => Query.Create<ExampleTable>       ()).Execute();

        // Insert data
        dbConnector.QueryBuilder().Build(Query => { Query
            .From<ExampleTable>()
            .Value(new ExampleTable {
                MediumText = "Medium text example value",
                Enum       = EnumExample.N
            })
            .Values(new List<ExampleTable> {
                new ExampleTable {
                    Json            = @"{""sample_json_field"": ""sample_json_value""}",
                    Enum            = EnumExample.Y,
                    CustomFieldName = new byte[8] { 81,47,15,21,12,16,23,39 }
                },
                new ExampleTable {
                    Longtext = "Long text example value",
                }
            })
            .Insert();
        }).Execute();
        
        // Select single row
        ExampleTable Row = dbConnector.QueryBuilder().Build(Query => Query
            .From("example_table")
            .OrderBy("id", OrderDirection.ASC)
            .Limit(1)
            .Select()
        ).FirstOrDefault<ExampleTable>();
        
        // Select multiple rows, to unmapped table class
        List<example_table> Rows = dbConnector.QueryBuilder().Build(Query => Query
            .From("example_table")
            .OrderBy("id", OrderDirection.DESC)
            .Select()
        ).ToList<example_table>();
    }
}
```

### Sample Query Rendering (WIP)

```csharp
// Complex query demonstration with subqueries
Query veryComplexQuery = Query.GetInstance()
    .Select("query_field")
    .Select($"COUNT({new FieldSelector("table_x", "table_y")})", true)
    .From("query_from")
    .Where("field", "value")
    .WhereIn(
        "field_list",
        Query.GetInstance()
            .Select("*", false)
            .From("subquery_table")
            .Where("subquery_field", true)
            .WhereIn(
                "subquery_in_field",
                Query.GetInstance()
                    .Select("subquery_subquery_in_field")
                    .From("subquery_subquery_in_table")
                    .Where("subquery_subquery_in_where", true)
            )
            .Limit(100)
    )
    .WhereIn("field_list", new List<dynamic> { null, 123, 456, "789" })
    .Join("another_table", new FieldSelector("table_x", "field_x"), new FieldSelector("table_y", "field_y"))
    .OrderBy(new OrderBy {
        Field     = new FieldSelector("order_field"),
        Direction = OrderDirection.DESC
    })
    .GroupBy("group_first")
    .GroupBy("group_second")
    .Limit(100);

// Render raw SQL query
Console.WriteLine(veryComplexQuery.Render());

// Render prepared statement query (with placeholders)
Console.WriteLine(veryComplexQuery.RenderPrepared());
```

## 📦 Dependencies

- [Unleasharp.DB.Base](https://github.com/TraberSoftware/Unleasharp.DB.Base) - Base abstraction layer
- [Npgsql](https://github.com/npgsql/npgsql) - PostgreSQL driver for .NET

## 📋 Version Compatibility

This library targets .NET 8.0 and later versions. For specific version requirements, please check the package dependencies.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

*For more information about Unleasharp.DB.Base, visit: [Unleasharp.DB.Base](https://github.com/TraberSoftware/Unleasharp.DB.Base)*