﻿using Npgsql;
using NpgsqlTypes;
using System;
using System.Data;
using Unleasharp.DB.Base.ExtensionMethods;
using Unleasharp.ExtensionMethods;

namespace Unleasharp.DB.PostgreSQL;

public class QueryBuilder : Base.QueryBuilder<QueryBuilder, Connector, Query, NpgsqlConnection, NpgsqlConnectionStringBuilder> {
    public QueryBuilder(Connector dbConnector) : base(dbConnector) { }

    public QueryBuilder(Connector dbConnector, Query query) : base(dbConnector, query) { }

    #region Query execution
    protected override bool _Execute() {
        this.DBQuery.RenderPrepared();

        try {
            using (NpgsqlCommand queryCommand = new NpgsqlCommand(this.DBQuery.QueryPreparedString, this.Connector.Connection)) {
                this._PrepareDbCommand(queryCommand);

                switch (this.DBQuery.QueryType) {
                    case Base.QueryBuilding.QueryType.COUNT:
                        if (queryCommand.ExecuteScalar().TryConvert<int>(out int scalarCount)) {
                            this.TotalCount = scalarCount;
                        }
                        break;
                    case Base.QueryBuilding.QueryType.SELECT:
                        using (NpgsqlDataReader queryReader = queryCommand.ExecuteReader()) {
                            this._HandleQueryResult(queryReader);
                        }
                        break;
                    case Base.QueryBuilding.QueryType.INSERT:
                        if (this.DBQuery.QueryValues.Count == 1) {
                            this.LastInsertedId = queryCommand.ExecuteScalar();
                            this.AffectedRows = this.LastInsertedId != null ? 1 : 0;
                        }
                        else {
                            this.AffectedRows = queryCommand.ExecuteNonQuery();
                        }
                        break;
                    case Base.QueryBuilding.QueryType.UPDATE:
                    default:
                        this.AffectedRows = queryCommand.ExecuteNonQuery();
                        break;
                }

                return true;
            }
        }
        catch (Exception ex) {
            this._OnQueryException(ex);
        }

        return false;
    }

    protected override T _ExecuteScalar<T>() {
        this.DBQuery.RenderPrepared();

        try {
            using (NpgsqlCommand queryCommand = new NpgsqlCommand(this.DBQuery.QueryPreparedString, this.Connector.Connection)) {
                this._PrepareDbCommand(queryCommand);
                if (queryCommand.ExecuteScalar().TryConvert<T>(out T scalarResult)) {
                    return scalarResult;
                }
            }
        }
        catch (Exception ex) {
            this._OnQueryException(ex);
        }

        return default(T);
    }

    private void _PrepareDbCommand(NpgsqlCommand queryCommand) {
        foreach (string queryPreparedDataKey in this.DBQuery.QueryPreparedData.Keys) {
            object? value = this.DBQuery.QueryPreparedData[queryPreparedDataKey].Value;

            if (value != null && value is NpgsqlParameter) {
                NpgsqlParameter sqlParameter = (NpgsqlParameter)value;
                sqlParameter.ParameterName = queryPreparedDataKey;

                queryCommand.Parameters.Add(value);
                continue;
            }
            else {
                try {
                    NpgsqlDbType? npgsqlDbType = this.DBQuery.GetPostgreSQLDataType(value.GetType().GetColumnType());
                    queryCommand.Parameters.Add(new NpgsqlParameter {
                        ParameterName = queryPreparedDataKey,
                        Value         = value,
                        NpgsqlDbType  = npgsqlDbType.Value
                    });
                    continue;
                }
                catch (Exception ex) { }
            }
            queryCommand.Parameters.Add(new NpgsqlParameter(queryPreparedDataKey, value));
        }
        queryCommand.Prepare();
    }

    private void _HandleQueryResult(NpgsqlDataReader queryReader) {
        this.Result = new DataTable();

        // Get schema information for all columns
        DataTable schemaTable = queryReader.GetSchemaTable();

        // Build column list with unique names
        for (int i = 0; i < queryReader.FieldCount; i++) {
            DataRow schemaRow = schemaTable.Rows[i];

            string columnName = (string)schemaRow["ColumnName"];     // Alias (or same as base if no alias)
            string baseTable  = schemaRow["BaseTableName"] ?.ToString();
            string baseColumn = schemaRow["BaseColumnName"]?.ToString();

            // If we have a base table, make a safe unique name
            string safeName;
            if (!string.IsNullOrEmpty(baseTable) && !string.IsNullOrEmpty(baseColumn)) {
                safeName = $"{baseTable}::{baseColumn}";
            }
            else {
                safeName = columnName;
            }

            // If still duplicated, add a suffix
            string finalName = safeName;
            int suffix = 1;
            while (this.Result.Columns.Contains(finalName)) {
                finalName = $"{safeName}_{suffix++}";
            }

            this.Result.Columns.Add(new DataColumn(finalName, queryReader.GetFieldType(i)));
        }

        // Load rows
        object[] rowData = new object[this.Result.Columns.Count];
        this.Result.BeginLoadData();
        while (queryReader.Read()) {
            queryReader.GetValues(rowData);
            this.Result.LoadDataRow(rowData, true);

            rowData = new object[this.Result.Columns.Count]; // reset
        }
        this.Result.EndLoadData();
    }
    #endregion
}
