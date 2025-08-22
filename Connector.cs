﻿using Npgsql;
using System;
using Unleasharp.DB.Base;

namespace Unleasharp.DB.PostgreSQL;

public class Connector : Unleasharp.DB.Base.Connector<Connector, NpgsqlConnectionStringBuilder> {

    public NpgsqlConnection Connection { get; private set; }

    #region Default constructors
    public Connector(NpgsqlConnectionStringBuilder stringBuilder) : base(stringBuilder)    { }

    public Connector(string connectionString)                     : base(connectionString) { }

    public Connector(NpgsqlConnection connection) {
        this.Connection = connection;
    }
	#endregion

	#region Connection management
    /// <inheritdoc />
    protected override bool _Connected() {
        switch (this.Connection.State) {
            // If any of this cases, the connection is open
            case System.Data.ConnectionState.Open:
            case System.Data.ConnectionState.Fetching:
            case System.Data.ConnectionState.Executing:
                return true;
        }

        return false;
    }

    /// <inheritdoc />
    protected override bool _Connect(bool force = false) {
        if (this.Connection == null) {
            this.Connection = new NpgsqlConnection(this.StringBuilder.ConnectionString);
        }

        if (
            !this._Connected()     // If not connected, it should be obvious to create the connection
            ||                     //
            (                      //
                force              // Reaching this statement means the connection is open but we are forcing the connection to be closed first
                &&                 //
                this._Disconnect() // Appending the disconnect disables the need to actively check again if connection is open to be closed
            ) 
        ) { 
            this.Connection.Open();

            this.ConnectionTimestamp = DateTime.UtcNow;
        }

        return this._Connected();
    }

    /// <inheritdoc />
    protected override bool _Disconnect() {
        if (this.Connection != null) {
            this.Connection.Close();
        }

        return this._Connected();
    }
    #endregion
}
