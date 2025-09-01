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
    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectorManager"/> class using the specified data source builder.
    /// </summary>
    /// <remarks>This constructor sets up the connection string and connection string builder based on the
    /// provided <paramref name="builder"/>. It also initializes the data source builder for further
    /// configuration.</remarks>
    /// <param name="builder">The <see cref="NpgsqlDataSourceBuilder"/> used to configure the connection settings.</param>
    public ConnectorManager(NpgsqlDataSourceBuilder builder) {
        this.ConnectionString        = builder.ConnectionString;
        this.ConnectionStringBuilder = builder.ConnectionStringBuilder;

        SetDataSourceBuilder(builder);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectorManager"/> class with the specified connection string and
    /// data source builder.
    /// </summary>
    /// <remarks>The <paramref name="builder"/> is used to customize the configuration of the data source.
    /// Ensure that the builder is properly configured before passing it to this constructor.</remarks>
    /// <param name="connectionString">The connection string used to configure the database connection.</param>
    /// <param name="builder">The <see cref="NpgsqlDataSourceBuilder"/> used to build the data source for the connection.</param>
    public ConnectorManager(string connectionString, NpgsqlDataSourceBuilder builder) : base(connectionString) {
        this.SetDataSourceBuilder(builder);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectorManager"/> class with the specified connection string
    /// builder and data source builder.
    /// </summary>
    /// <param name="stringBuilder">An <see cref="NpgsqlConnectionStringBuilder"/> instance that specifies the connection string settings for the
    /// database connection.</param>
    /// <param name="builder">An <see cref="NpgsqlDataSourceBuilder"/> instance used to configure and build the data source for database
    /// connections.</param>
    public ConnectorManager(NpgsqlConnectionStringBuilder stringBuilder, NpgsqlDataSourceBuilder builder) : base(stringBuilder) {
        this.SetDataSourceBuilder(builder);
    }
    #endregion

    #region DataSource setup
    /// <summary>
    /// Configures the data source builder for the current instance.
    /// </summary>
    /// <remarks>This method sets the provided <see cref="NpgsqlDataSourceBuilder"/> as the data source
    /// builder and updates the data source configuration accordingly. Ensure that the <paramref name="builder"/> is
    /// properly initialized before calling this method.</remarks>
    /// <param name="builder">The <see cref="NpgsqlDataSourceBuilder"/> instance used to configure the data source. This parameter cannot be
    /// <see langword="null"/>.</param>
    public void SetDataSourceBuilder(NpgsqlDataSourceBuilder builder) {
        this.DataSourceBuilder = builder;
        this.UpdateDataSource();
    }

    /// <summary>
    /// Updates the current data source by rebuilding it using the configured data source builder.
    /// </summary>
    /// <remarks>This method replaces the existing data source with a new instance created by the  <see
    /// cref="DataSourceBuilder"/>. Ensure that the builder is properly configured  before calling this method to avoid
    /// unexpected results.</remarks>
    public void UpdateDataSource() {
        this.DataSource = this.DataSourceBuilder.Build();
    }

    /// <summary>
    /// Configures the data source to map the specified enum type for use in the connector.
    /// </summary>
    /// <remarks>This method maps the specified enum type to the data source using its name in lowercase. It
    /// is typically used to ensure that the enum type is recognized and handled correctly by the data source.</remarks>
    /// <typeparam name="EnumType">The enum type to be mapped. Must be a value type that implements <see cref="System.Enum"/>.</typeparam>
    /// <returns>The current <see cref="ConnectorManager"/> instance, allowing for method chaining.</returns>
    public ConnectorManager WithMappedEnum<EnumType>() where EnumType : struct, Enum {
        string enumTypeName = typeof(EnumType).Name.ToLowerInvariant();

        this.DataSourceBuilder.MapEnum<EnumType>(enumTypeName);
        this.UpdateDataSource();

        return this;
    }

    /// <summary>
    /// Configures the data source builder using the specified setup action.
    /// </summary>
    /// <remarks>This method allows customization of the <see cref="NpgsqlDataSourceBuilder"/> used by the
    /// connector.  After the action is invoked, the data source is updated to reflect the changes.</remarks>
    /// <param name="action">An <see cref="Action{T}"/> delegate that takes an <see cref="NpgsqlDataSourceBuilder"/> as a parameter  and
    /// performs setup operations on it. If <paramref name="action"/> is <see langword="null"/>, no changes are made.</param>
    /// <returns>The current instance of <see cref="ConnectorManager"/>, allowing for method chaining.</returns>
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
