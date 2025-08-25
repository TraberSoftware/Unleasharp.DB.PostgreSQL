using Npgsql;
using System;
using Unleasharp.DB.Base;

namespace Unleasharp.DB.PostgreSQL;

/// <summary>
/// Manager class for PostgreSQL database connections that provides access to query builders
/// for constructing and executing SQL queries.
/// </summary>
public class ConnectorManager : 
    ConnectorManager<ConnectorManager, Connector, NpgsqlConnectionStringBuilder, NpgsqlConnection, QueryBuilder, Query>
{
    public NpgsqlDataSource        DataSource        { get; private set; }
    public NpgsqlDataSourceBuilder DataSourceBuilder { get; private set; } = new NpgsqlDataSourceBuilder();

    #region Default constructors
    /// <inheritdoc />
    public ConnectorManager() : base() { }

    /// <inheritdoc />
    public ConnectorManager(NpgsqlConnectionStringBuilder stringBuilder) : base(stringBuilder) {
        this.SetDataSourceBuilder(new NpgsqlDataSourceBuilder(stringBuilder.ConnectionString));
    }

    /// <inheritdoc />
    public ConnectorManager(string connectionString)                     : base(connectionString) {
        this.SetDataSourceBuilder(new NpgsqlDataSourceBuilder(connectionString));
    }
    #endregion

    #region Custom constructors
    public ConnectorManager(NpgsqlDataSourceBuilder builder) {
        this.ConnectionString        = builder.ConnectionString;
        this.ConnectionStringBuilder = builder.ConnectionStringBuilder;

        SetDataSourceBuilder(builder);
    }

    public ConnectorManager(string connectionString, NpgsqlDataSourceBuilder builder) : base(connectionString) {
        this.SetDataSourceBuilder(builder);
    }

    public ConnectorManager(NpgsqlConnectionStringBuilder stringBuilder, NpgsqlDataSourceBuilder builder) : base(stringBuilder) {
        this.SetDataSourceBuilder(builder);
    }
    #endregion

    #region DataSource setup
    public void SetDataSourceBuilder(NpgsqlDataSourceBuilder builder) {
        this.DataSourceBuilder = builder;
        this.UpdateDataSource();
    }

    public void UpdateDataSource() {
        this.DataSource = this.DataSourceBuilder.Build();
    }

    public ConnectorManager WithMappedEnum<EnumType>() where EnumType : struct, Enum {
        string enumTypeName = typeof(EnumType).Name.ToLowerInvariant();

        this.DataSourceBuilder.MapEnum<EnumType>(enumTypeName);
        this.UpdateDataSource();

        return this;
    }

    public ConnectorManager WithDataSourceBuilderSetup(Action<NpgsqlDataSourceBuilder> action) {
        if (action == null) {
            return this;
        }

        action.Invoke(this.DataSourceBuilder);
        this.UpdateDataSource();

        return this;
    }
    #endregion

    #region Overrides
    /// <inheritdoc />
    protected override Connector __GenerateDBTypeInstance() {
        if (this.DataSource != null) {
            return new Connector(this.DataSource.CreateConnection());
        }

        return null;
    }
    #endregion
}
