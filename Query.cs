﻿using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Unleasharp.DB.Base;
using Unleasharp.DB.Base.ExtensionMethods;
using Unleasharp.DB.Base.QueryBuilding;
using Unleasharp.DB.Base.SchemaDefinition;
using Unleasharp.DB.PostgreSQL.ExtensionMethods;
using Unleasharp.ExtensionMethods;

namespace Unleasharp.DB.PostgreSQL;

public class Query : Unleasharp.DB.Base.Query<Query> {
    #region Custom PostgreSQL query data
    #endregion

    public const string FieldDelimiter = "\"";
    public const string ValueDelimiter = "'";

    #region Public query building methods overrides
    public override Query Set<T>(Expression<Func<T, object>> expression, dynamic value, bool escape = true) {
        Type   tableType         = typeof(T);
        string dbColumnName      = ExpressionHelper.ExtractColumnName<T>(expression);
        string classPropertyName = ExpressionHelper.ExtractClassFieldName<T>(expression);
        string tableName         = tableType.GetTableName();

        MemberInfo member    = tableType.GetMember(classPropertyName).FirstOrDefault();

        if (member == null) {
            return this.Set(dbColumnName, value, escape);
        }

        NpgsqlParameter param = this.__GetMemberInfoNpgsqlParameter(value, member);

        return this.Set(new Where<Query> {
            Field       = new FieldSelector(dbColumnName),
            Value       = param,
            EscapeValue = true
        });
    }

    public override Query Value<T>(T row, bool skipNullValues = true) where T : class {
        Type rowType = row.GetType();

        if (rowType.IsClass) {
            List<NpgsqlParameter> rowValues = new List<NpgsqlParameter>();

            foreach (FieldInfo field in rowType.GetFields()) {
                rowValues.Add(this.__GetMemberInfoNpgsqlParameter(field.GetValue(row), field));
            }
            foreach (PropertyInfo property in rowType.GetProperties()) {
                rowValues.Add(this.__GetMemberInfoNpgsqlParameter(property.GetValue(row), property));
            }

            return this.Value(
                rowValues
                    .Where(rowValue => rowValue != null)
                    .ToDictionary(
                        rowValue => rowValue.ParameterName, 
                        rowValue => rowValue as dynamic
                    )
            );
        }

        return this.Value(row.ToDynamicDictionary());
    }


    public virtual Query Values<T>(List<T> rows, bool skipNullValues = true) where T : class {
        foreach (T row in rows) {
            this.Value<T>(row, skipNullValues);
        }

        return (Query) this;
    }

    private NpgsqlParameter __GetMemberInfoNpgsqlParameter(object? value, MemberInfo memberInfo) {
        string          classFieldName   = memberInfo.Name;
        string          dbFieldName      = classFieldName;
        Type            memberInfoType   = memberInfo.GetDataType();
        Column?         column           = memberInfo.GetCustomAttribute<Column>();
        ColumnDataType? columnDataType   = null;
        NpgsqlDbType?   dbColumnDataType = null;

        if (value == null) {
            value = DBNull.Value;
        }

        // Don't set null values to Primary Key columns
        // HOWEVER, be careful when mixing null and not-null values of Primary Key columns in the same insert
        if ((column != null && (column.PrimaryKey && column.NotNull)) && (value == null || value == DBNull.Value)) {
            return null;
        }

        if (Nullable.GetUnderlyingType(memberInfoType) != null) {
            memberInfoType = Nullable.GetUnderlyingType(memberInfoType);
        }

        if (column != null) {
            dbFieldName = column.Name;

            if (!string.IsNullOrEmpty(column.DataTypeString)) {
                columnDataType = this.GetColumnDataType(column.DataTypeString);
            }
            if (column.DataType != null) {
                columnDataType = column.DataType;
            }

            if (columnDataType == null) {
                columnDataType = memberInfoType.GetColumnType();
            }

            dbColumnDataType = this.GetPostgreSQLDataType(columnDataType.Value) ?? dbColumnDataType;
        }

        if (dbColumnDataType.HasValue && !memberInfoType.IsEnum) {
            return new NpgsqlParameter {
                ParameterName = dbFieldName,
                Value         = value,
                NpgsqlDbType  = dbColumnDataType.Value,
                SourceColumn  = dbFieldName,
            };
        }

        return new NpgsqlParameter(dbFieldName, value);
    }
    #endregion

    #region Query building
    #region Query building - Create
    public Query CreateEnumType(Type enumType) {
        this.SetQueryType(QueryType.CREATE);

        List<string> enumValues = new List<string>();
        foreach (Enum enumValue in Enum.GetValues(enumType)) {
            enumValues.Add(this.__RenderWhereValue(enumValue.GetPgName(), true));
        }

        this.QueryPreparedString = $"CREATE TYPE {Query.FieldDelimiter}{enumType.Name.ToLowerInvariant()}{Query.FieldDelimiter} AS ENUM({string.Join(',', enumValues)})";
        this.QueryRenderedString = QueryPreparedString;

        this.Untouch();
        return this;
    }

    public Query CreateEnumType<EnumType>() where EnumType : Enum {
        return this.CreateEnumType(typeof(EnumType));
    }
    #endregion
    #endregion

    #region Query rendering
    #region Query fragment rendering
    public override void _RenderPrepared() {
        this._Render();

        string rendered = this.QueryPreparedString;

        foreach (string preparedDataItemKey in this.QueryPreparedData.Keys.Reverse()) {
            PreparedValue preparedDataItemValue = this.QueryPreparedData[preparedDataItemKey];
            object?       value                 = preparedDataItemValue.Value is NpgsqlParameter ? (((NpgsqlParameter)preparedDataItemValue.Value).Value) : preparedDataItemValue.Value;
            bool          escape                = preparedDataItemValue.Value is NpgsqlParameter ? true : preparedDataItemValue.EscapeValue;
            string        renderedValue         = "NULL";

            if (value != null && value != DBNull.Value) {
                renderedValue = true switch {
                    true when value is Enum   => this.__RenderWhereValue(((Enum)value).GetPgName(), escape),
                    true when value is byte[] => this.__RenderWhereValue($"0x{Convert.ToHexString((byte[])value)}", escape),
                                            _ => this.__RenderWhereValue(value, escape)
                };
            }
            rendered = rendered.Replace(preparedDataItemKey, renderedValue);
        }

        this.QueryRenderedString = rendered;
    }

    public string RenderSelect(Select<Query> fragment) {
        if (fragment.Subquery != null) {
            return "(" + fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render() + ")";
        }

        return fragment.Field.Render() + (!string.IsNullOrWhiteSpace(fragment.Alias) ? $" AS {fragment.Alias}" : "");
    }

    public string RenderFrom(From<Query> fragment) {
        if (fragment.Subquery != null) {
            return "(" + fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render() + ")";
        }

        string rendered = string.Empty;

        if (!string.IsNullOrWhiteSpace(fragment.Table)) {
            if (fragment.EscapeTable) {
                rendered = FieldDelimiter + fragment.Table + FieldDelimiter;
            }
            else {
                rendered = fragment.Table;
            }
        }

        return rendered + (fragment.TableAlias != string.Empty ? $" {fragment.TableAlias}" : "");
    }

    public string RenderJoin(Join<Query> fragment) {
        return $"{(fragment.EscapeTable ? FieldDelimiter + fragment.Table + FieldDelimiter : fragment.Table)} ON {this.RenderWhere(fragment.Condition)}";
    }

    public string RenderGroupBy(GroupBy fragment) {
        List<string> toRender = new List<string>();

        if (!string.IsNullOrWhiteSpace(fragment.Field.Table)) {
            if (fragment.Field.Escape) {
                toRender.Add(FieldDelimiter + fragment.Field.Table + FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Field.Table);
            }
        }

        if (!string.IsNullOrWhiteSpace(fragment.Field.Field)) {
            if (fragment.Field.Escape) {
                toRender.Add(FieldDelimiter + fragment.Field.Field + FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Field.Field);
            }
        }

        return String.Join('.', toRender);
    }

    public string RenderWhere(Where<Query> fragment) {
        if (fragment.Subquery != null) {
            return $"{fragment.Field.Render()} {fragment.Comparer.GetDescription()} ({fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render()})";
        }

        List<string> toRender = new List<string>();

        toRender.Add(fragment.Field.Render());

        // We are comparing fields, not values
        if (fragment.ValueField != null) {
            toRender.Add(fragment.ValueField.Render());
        }
        else {
            if (fragment.Value == null) {
                fragment.Comparer = WhereComparer.IS;
                toRender.Add("NULL");
            }
            else {
                if (fragment.EscapeValue) {
                    toRender.Add(this.PrepareQueryValue(fragment.Value, fragment.EscapeValue));
                }
                else {
                    toRender.Add(this.__RenderWhereValue(fragment.Value, false));
                }
            }
        }

        return String.Join(fragment.Comparer.GetDescription(), toRender);
    }

    public string RenderWhereIn(WhereIn<Query> fragment) {
        if (fragment.Subquery != null) {
            return $"{fragment.Field.Render()} IN ({fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render()})";
        }

        if (fragment.Values == null || fragment.Values.Count == 0) {
            return String.Empty;
        }

        List<string> toRender = new List<string>();

        foreach (dynamic fragmentValue in fragment.Values) {
            if (fragment.EscapeValue) {
                toRender.Add(this.PrepareQueryValue(fragmentValue, fragment.EscapeValue));
            }
            else {
                toRender.Add(__RenderWhereValue(fragmentValue, fragment.EscapeValue));
            }
        }

        return $"{fragment.Field.Render()} IN ({String.Join(",", toRender)})";
    }

    public string RenderFieldSelector(FieldSelector fragment) {
        List<string> toRender = new List<string>();

        if (!string.IsNullOrWhiteSpace(fragment.Table)) {
            if (fragment.Escape) {
                toRender.Add(FieldDelimiter + fragment.Table + FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Table);
            }
        }

        if (!string.IsNullOrWhiteSpace(fragment.Field)) {
            if (fragment.Escape) {
                toRender.Add(FieldDelimiter + fragment.Field + FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Field);
            }
        }

        return String.Join('.', toRender);
    }
    #endregion

    #region Query sentence rendering
    protected override string _RenderCountSentence() {
        return "SELECT COUNT(*)";
    }

    protected override string _RenderSelectSentence() {
        List<string> rendered = new List<string>();

        if (this.QuerySelect.Count > 0) {
            foreach (Select<Query> queryFragment in this.QuerySelect) {
                rendered.Add(this.RenderSelect(queryFragment));
            }
        }
        else {
            rendered.Add("*");
        }

        return "SELECT " + string.Join(',', rendered);
    }

    protected override string _RenderFromSentence() {
        List<string> rendered = new List<string>();

        foreach (From<Query> queryFragment in this.QueryFrom) {
            rendered.Add(this.RenderFrom(queryFragment));
        }

        return (rendered.Count > 0 ? "FROM " + string.Join(',', rendered) : "");
    }

    protected override string _RenderJoinSentence() {
        List<string> rendered = new List<string>();
        foreach (Join<Query> queryFragment in this.QueryJoin) {
            rendered.Add(this.RenderJoin(queryFragment));
        }

        return (rendered.Count > 0 ? "JOIN " + string.Join(',', rendered) : "");
    }

    protected override string _RenderWhereSentence() {
        List<string> rendered = new List<string>();
        foreach (Where<Query> queryFragment in this.QueryWhere) {
            if (rendered.Any()) {
                rendered.Add(queryFragment.Operator.GetDescription());
            }
            rendered.Add(this.RenderWhere(queryFragment));
        }
        foreach (WhereIn<Query> queryFragment in this.QueryWhereIn) {
            if (rendered.Any()) {
                rendered.Add(queryFragment.Operator.GetDescription());
            }
            rendered.Add(this.RenderWhereIn(queryFragment));
        }

        return (rendered.Count > 0 ? "WHERE " + string.Join(' ', rendered) : "");
    }

    protected override string _RenderGroupSentence() {
        List<string> rendered = new List<string>();

        foreach (GroupBy queryFragment in this.QueryGroup) {
            rendered.Add(this.RenderGroupBy(queryFragment));
        }

        return (rendered.Count > 0 ? "GROUP BY " + string.Join(',', rendered) : "");
    }

    protected override string _RenderHavingSentence() {
        List<string> rendered = new List<string>();

        foreach (Where<Query> queryFragment in this.QueryHaving) {
            rendered.Add(this.RenderWhere(queryFragment));
        }

        return (rendered.Count > 0 ? "HAVING " + string.Join(',', rendered) : "");
    }

    protected override string _RenderOrderSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryOrder != null) {
            foreach (OrderBy queryOrderItem in this.QueryOrder) {
                List<string> renderedSubset = new List<string>();

                renderedSubset.Add(queryOrderItem.Field.Render());

                if (queryOrderItem.Direction != OrderDirection.NONE) {
                    renderedSubset.Add(queryOrderItem.Direction.GetDescription());
                }

                rendered.Add(string.Join(' ', renderedSubset));
            }
        }

        return (rendered.Count > 0 ? "ORDER BY " + string.Join(',', rendered) : "");
    }

    protected override string _RenderLimitSentence() {
        List<string> rendered = new List<string>();
        if (this.QueryLimit != null) {
            if (this.QueryLimit.Count > 0) {
                rendered.Add(this.QueryLimit.Count.ToString());
            }
            if (this.QueryLimit.Offset >= 0) {
                rendered.Add($" OFFSET {this.QueryLimit.Offset.ToString()}");
            }
        }

        return (rendered.Count > 0 ? "LIMIT " + string.Join(' ', rendered) : "");
    }


    protected override string _RenderDeleteSentence() {
        From<Query> from = this.QueryFrom.FirstOrDefault();

        if (from != null) {
            return $"DELETE FROM {from.Table}{(!string.IsNullOrWhiteSpace(from.TableAlias) ? $" AS {from.TableAlias}" : "")}";
        }

        return string.Empty;
    }
    protected override string _RenderUpdateSentence() { 
        From<Query> from = this.QueryFrom.FirstOrDefault();

        if (from != null) {
            return $"UPDATE {this.RenderFrom(from)}";
        }

        return string.Empty;
    }

    protected override string _RenderSetSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryOrder != null) {
            foreach (Where<Query> querySetItem in this.QuerySet) {
                querySetItem.Comparer = WhereComparer.EQUALS;

                rendered.Add(this.RenderWhere(querySetItem));
            }
        }

        return (rendered.Count > 0 ? "SET " + string.Join(',', rendered) : "");
    }

    protected override string _RenderInsertIntoSentence() { 
        From<Query> from = this.QueryFrom.FirstOrDefault();

        if (from != null) {
            return $"INSERT INTO {Query.FieldDelimiter}{from.Table}{Query.FieldDelimiter} ({string.Join(',', this.QueryColumns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))})";
        }

        return string.Empty;
    }

    protected override string _RenderInsertValuesSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryValues != null) {
            foreach (Dictionary<string, dynamic> queryValue in QueryValues) {
                List<string> toRender = new List<string>();

                // In order to get a valid query, insert the values in the same column order
                foreach (string queryColumn in this.QueryColumns) {
                    if (queryValue.ContainsKey(queryColumn) && queryValue[queryColumn] != null) {
                        if (queryValue[queryColumn] != null && queryValue[queryColumn] is NpgsqlParameter) {
                            string preparedQuerylabel = this.GetNextPreparedQueryValueLabel();
                            (queryValue[queryColumn] as NpgsqlParameter).ParameterName = preparedQuerylabel;

                            toRender.Add(this.PrepareQueryValue(queryValue[queryColumn], false));
                            continue;
                        }
                    
                        if (queryValue[queryColumn] != null && queryValue[queryColumn] is Enum) {
                            toRender.Add(this.PrepareQueryValue(queryValue[queryColumn], false));
                        }
                        else {
                            toRender.Add(this.PrepareQueryValue(queryValue[queryColumn], true));
                        }
                    }
                    else {
                        toRender.Add("NULL");
                    }
                }

                rendered.Add($"({string.Join(",", toRender)})");
            }
        }

        return (rendered.Count > 0 ? "VALUES " + string.Join(',', rendered) : "");
    }


    protected override string _RenderCreateSentence<T>() {
        return this._RenderCreateSentence(typeof(T));
    }

    protected override string _RenderCreateSentence(Type tableType) {
        Table typeTable = tableType.GetCustomAttribute<Table>();
        if (typeTable == null) {
            throw new InvalidOperationException("Missing [Table] attribute");
        }

        StringBuilder rendered = new StringBuilder();

        rendered.Append("CREATE ");
        if (typeTable.Temporary) {
            rendered.Append("TEMPORARY ");
        }

        rendered.Append("TABLE ");
        if (typeTable.IfNotExists) {
            rendered.Append("IF NOT EXISTS ");
        }
        rendered.Append($"{Query.FieldDelimiter}{typeTable.Name}{Query.FieldDelimiter} (");

        IEnumerable<string?> tableColumnDefinitions = this.__GetTableColumnDefinitions(tableType);
        IEnumerable<string?> tableKeyDefinitions    = this.__GetTableKeyDefinitions(tableType);

        rendered.Append(string.Join(",", tableColumnDefinitions.Concat(tableKeyDefinitions ?? Enumerable.Empty<string>())));
        rendered.Append(")");

        // Table options
        var tableOptions = tableType.GetCustomAttributes<TableOption>();
        foreach (TableOption tableOption in tableOptions) {
            rendered.Append($" {tableOption.Name}={tableOption.Value}");
        }

        return rendered.ToString();
    }

    private IEnumerable<string?> __GetTableColumnDefinitions(Type tableType) {
        PropertyInfo[] tableProperties = tableType.GetProperties();
        FieldInfo   [] tableFields     = tableType.GetFields();

        return tableProperties.Select(tableProperty => {
            return this.__GetColumnDefinition(tableProperty, tableProperty.GetCustomAttribute<Column>());
        }).Where(renderedColumn => renderedColumn != null);
    }


    private IEnumerable<string?> __GetTableKeyDefinitions(Type tableType) {
        List<string> definitions = new List<string>();

        foreach (UniqueKey uKey in tableType.GetCustomAttributes<UniqueKey>()) {
            definitions.Add(
                $"CONSTRAINT {Query.FieldDelimiter}uk_{uKey.Name}{Query.FieldDelimiter} UNIQUE " +
                $"({string.Join(", ", uKey.Columns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))})"
            );
        }
        foreach (ForeignKey fKey in tableType.GetCustomAttributes<ForeignKey>()) {
            definitions.Add(
                $"CONSTRAINT {Query.FieldDelimiter}fk_{fKey.Name}{Query.FieldDelimiter} FOREIGN KEY " +
                $"({string.Join(", ", fKey.Columns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))}) " + 
                $" REFERENCES {Query.FieldDelimiter}{fKey.ReferencedTable}{Query.FieldDelimiter}" +
                $"({string.Join(", ", fKey.ReferencedColumns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))})" + 
                $"{(!string.IsNullOrWhiteSpace(fKey.OnDelete) ? $" ON DELETE {fKey.OnDelete}" : "")}" + 
                $"{(!string.IsNullOrWhiteSpace(fKey.OnUpdate) ? $" ON UPDATE {fKey.OnUpdate}" : "")}" 
            );
        }

        return definitions;
    }

    private string? __GetColumnDefinition(PropertyInfo property, Column tableColumn) {
        if (tableColumn == null) {
            return null;
        }

        Type columnType = property.PropertyType;
        if (Nullable.GetUnderlyingType(columnType) != null) {
            columnType = Nullable.GetUnderlyingType(columnType);
        }

        string columnDataTypeString = tableColumn.DataTypeString ?? this.GetColumnDataTypeString(tableColumn.DataType);
        if (tableColumn.PrimaryKey) {
            columnDataTypeString = columnDataTypeString switch {
                "SMALLINT" => "SMALLSERIAL",
                "INTEGER"  =>      "SERIAL",
                "BIGINT"   =>   "BIGSERIAL",
            };
        }
        if (columnType.IsEnum) {
            columnDataTypeString = $"{columnType.Name.ToLowerInvariant()}";
        }

        StringBuilder columnBuilder = new StringBuilder($"{Query.FieldDelimiter}{tableColumn.Name}{Query.FieldDelimiter} {columnDataTypeString}");
        if (tableColumn.Length > 0)
            columnBuilder.Append($" ({tableColumn.Length}{(tableColumn.Precision > 0 ? $",{tableColumn.Precision}" : "")})");

        if (tableColumn.Unique)
            columnBuilder.Append(" UNIQUE");
        if (tableColumn.PrimaryKey)
            columnBuilder.Append(" PRIMARY KEY");
        if (tableColumn.NotNull && !tableColumn.PrimaryKey) 
            columnBuilder.Append(" NOT NULL");
        if (tableColumn.Default != null)
            columnBuilder.Append($" DEFAULT {tableColumn.Default}");
        if (tableColumn.Check != null)
            columnBuilder.Append($" CHECK ({tableColumn.Check})");
        if (tableColumn.Comment != null)
            columnBuilder.Append($" COMMENT '{tableColumn.Comment}'");
        return columnBuilder.ToString();
    }

    protected override string _RenderSelectExtraSentence() {
        return string.Empty;
    }
    #endregion

    #endregion

    #region Helper functions
    private string __RenderWhereValue(dynamic value, bool escape) {
        if (value is string
            ||
            value is DateTime
        ) {
            if (escape) {
                return $"{ValueDelimiter}{value}{ValueDelimiter}";
            }
        }
        if (value is Enum) {
            return $"{ValueDelimiter}{((Enum)value).GetDescription()}{ValueDelimiter}";
        }

        return value.ToString();
    }

    public string GetColumnDataTypeString(ColumnDataType? type) {
        return type switch {
            ColumnDataType.Boolean   => "BOOLEAN",
            ColumnDataType.Int16     => "SMALLINT",
            ColumnDataType.Int       => "INTEGER",
            ColumnDataType.Int32     => "INTEGER",
            ColumnDataType.Int64     => "BIGINT",
            ColumnDataType.UInt16    => "SMALLINT",
            ColumnDataType.UInt32    => "INTEGER",
            ColumnDataType.UInt      => "INTEGER",
            ColumnDataType.UInt64    => "BIGINT",
            ColumnDataType.Decimal   => "NUMERIC",
            ColumnDataType.Float     => "REAL",
            ColumnDataType.Double    => "DOUBLE PRECISION",
            ColumnDataType.Text      => "TEXT",
            ColumnDataType.Char      => "CHAR",
            ColumnDataType.Varchar   => "VARCHAR",
            ColumnDataType.Enum      => "VARCHAR",
            ColumnDataType.Date      => "DATE",
            ColumnDataType.DateTime  => "TIMESTAMP",
            ColumnDataType.Time      => "TIME",
            ColumnDataType.Timestamp => "TIMESTAMPTZ",
            ColumnDataType.Binary    => "BYTEA",
            ColumnDataType.Guid      => "UUID",
            ColumnDataType.Json      => "JSONB",
            ColumnDataType.Xml       => "XML",

            _ => throw new NotSupportedException($"PostgreSQL does not support {type}")
        };
    }

    public NpgsqlDbType? GetPostgreSQLDataType(ColumnDataType type) {
        return type switch {
            ColumnDataType.Boolean   => NpgsqlDbType.Boolean,
            ColumnDataType.Int16     => NpgsqlDbType.Smallint,
            ColumnDataType.Int       => NpgsqlDbType.Integer,
            ColumnDataType.Int32     => NpgsqlDbType.Integer,
            ColumnDataType.Int64     => NpgsqlDbType.Bigint,
            ColumnDataType.UInt16    => NpgsqlDbType.Smallint,
            ColumnDataType.UInt32    => NpgsqlDbType.Integer,
            ColumnDataType.UInt      => NpgsqlDbType.Integer,
            ColumnDataType.UInt64    => NpgsqlDbType.Bigint,
            ColumnDataType.Decimal   => NpgsqlDbType.Numeric,
            ColumnDataType.Float     => NpgsqlDbType.Real,
            ColumnDataType.Double    => NpgsqlDbType.Double,
            ColumnDataType.Text      => NpgsqlDbType.Text,
            ColumnDataType.Char      => NpgsqlDbType.Char,
            ColumnDataType.Varchar   => NpgsqlDbType.Varchar,
            ColumnDataType.Enum      => NpgsqlDbType.Integer,
            ColumnDataType.Date      => NpgsqlDbType.Date,
            ColumnDataType.DateTime  => NpgsqlDbType.Timestamp,
            ColumnDataType.Time      => NpgsqlDbType.Time,
            ColumnDataType.Timestamp => NpgsqlDbType.TimestampTz,
            ColumnDataType.Binary    => NpgsqlDbType.Bytea,
            ColumnDataType.Guid      => NpgsqlDbType.Uuid,
            ColumnDataType.Json      => NpgsqlDbType.Jsonb,
            ColumnDataType.Xml       => NpgsqlDbType.Xml,

            _ => null
        };
    }
    #endregion
}
