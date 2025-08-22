using Npgsql;
using Npgsql.Internal.Postgres;
using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
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
                        return true;
                    case Base.QueryBuilding.QueryType.SELECT:
						using (NpgsqlDataReader queryReader = queryCommand.ExecuteReader()) {
                            this._HandleQueryResult(queryReader);
                        }
                        return true;
                    case Base.QueryBuilding.QueryType.UPDATE:
                    default:
						this.AffectedRows = queryCommand.ExecuteNonQuery();

                        return true;
                }
            }
        }
        catch (Exception ex) {
            this._OnQueryException(ex);
        }

        return false;
    }

    private void _PrepareDbCommand(NpgsqlCommand queryCommand) {
		foreach (string queryPreparedDataKey in this.DBQuery.QueryPreparedData.Keys) {
            object? value = this.DBQuery.QueryPreparedData[queryPreparedDataKey].Value;

			if (value != null && value is NpgsqlParameter) {
				queryCommand.Parameters.Add(value);
                continue;
			}
			queryCommand.Parameters.Add(new NpgsqlParameter(queryPreparedDataKey, value));
		}
		queryCommand.Prepare();
	}

	private void _HandleQueryResult(NpgsqlDataReader queryReader) {
        this.Result = new DataTable();

        for (int i = 0; i < queryReader.FieldCount; i++) {
            this.Result.Columns.Add(new DataColumn(queryReader.GetName(i), queryReader.GetFieldType(i)));
        }

        object[] rowData = new object[this.Result.Columns.Count];

        this.Result.BeginLoadData();
        while (queryReader.Read()) {
            queryReader.GetValues(rowData);
            this.Result.LoadDataRow(rowData, true);

            // Reinstanciate the row data holder
            rowData = new object[this.Result.Columns.Count];
        }
        this.Result.EndLoadData();
    }
    #endregion
}
