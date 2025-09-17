using Npgsql;
using System;
using Unleasharp.DB.Base;

namespace Unleasharp.DB.PostgreSQL;

/// <summary>
/// Represents a connector for managing connections to a PostgreSQL database.
/// </summary>
/// <remarks>This class provides functionality to establish, manage, and terminate connections to a PostgreSQL
/// database. It extends the base functionality provided by <see cref="Unleasharp.DB.Base.Connector{TConnector,
/// TConnectionStringBuilder}"/>. Use this class to interact with a PostgreSQL database by providing a connection string or a
/// pre-configured <see cref="NpgsqlConnection"/>.</remarks>
public class Connector : Unleasharp.DB.Base.Connector<Connector, NpgsqlConnection, NpgsqlConnectionStringBuilder> {
    #region Default constructors
    /// <inheritdoc />
    public Connector(NpgsqlConnectionStringBuilder stringBuilder) : base(stringBuilder)    { }
    /// <inheritdoc />
    public Connector(string connectionString)                     : base(connectionString) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="Connector"/> class using the specified PostgreSQL connection.
    /// </summary>
    /// <param name="connection">The <see cref="NpgsqlConnection"/> instance to be used by the connector. Cannot be <see langword="null"/>.</param>
    public Connector(NpgsqlConnection connection) {
        this.Connection = connection;
    }
    #endregion
}
