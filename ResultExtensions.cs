using System;
using TinySql.Metadata;

namespace TinySql
{
    public static class ResultExtensions
    {


        public static SqlBuilder Update(this RowData row, bool OnlyChanges = false, bool OutputPrimaryKey = false, string[] OutputFields = null)
        {
            if (OnlyChanges && !row.HasChanges)
            {
                return null;
            }
            SqlBuilder builder = SqlBuilder.Update();
            string TableName = row.Table;
            string Schema = null;
            if (TableName.IndexOf('.') > 0)
            {
                Schema = TableName.Substring(0, TableName.IndexOf('.'));
                TableName = TableName.Substring(TableName.IndexOf('.') + 1);
            }
            UpdateTable up = builder.Table(TableName, Schema);
            Metadata.MetadataTable mt = row.Metadata;
            if (OnlyChanges)
            {
                foreach (string key in row.ChangedValues.Keys)
                {
                    object o;
                    MetadataColumn c;
                    if (row.ChangedValues.TryGetValue(key, out o) && mt.Columns.TryGetValue(key,out c) && !c.IsReadOnly)
                    {
                        SqlStatementExtensions.Set(up, key, c.SqlDataType, o, c.DataType, c.Length,c.Precision, c.Scale);
                    }
                    else
                    {
                        throw new InvalidOperationException("Cannot get the changed column " + key);
                    }
                }
            }
            else
            {
                foreach (string key in row.Columns)
                {
                    MetadataColumn c;
                    if (mt.Columns.TryGetValue(key, out c) && !c.IsReadOnly)
                    {
                        SqlStatementExtensions.Set(up, key, c.SqlDataType, row.Column(key), c.DataType, c.Length,c.Precision, c.Scale);
                    }
                    
                }
            }

            if (OutputPrimaryKey)
            {
                TableParameterField tpf = up.Output();
                foreach (MetadataColumn key in mt.PrimaryKey.Columns)
                {
                    SqlStatementExtensions.Column(tpf, key.Name, key.SqlDataType, key.Length, key.Precision, key.Scale);
                }
            }
            if (OutputFields != null && OutputFields.Length > 0)
            {
                TableParameterField tpf = up.Output();
                foreach (string s in OutputFields)
                {
                    MetadataColumn c = mt[s];
                    SqlStatementExtensions.Column(tpf, s, c.SqlDataType, c.Length, c.Precision, c.Scale);
                }
            }
            builder.WhereConditions = row.PrimaryKey(builder);
            return builder;

        }

        public static SqlBuilder Update(this SqlBuilder builder, RowData row, string[] Output = null)
        {
            if (!row.HasChanges)
            {
                return builder;
            }
            string TableName = row.Table;
            string Schema = null;
            if (TableName.IndexOf('.') > 0)
            {
                Schema = TableName.Substring(0, TableName.IndexOf('.'));
                TableName = TableName.Substring(TableName.IndexOf('.') + 1);
            }
            UpdateTable up = builder.Table(TableName, Schema);
            Metadata.MetadataTable mt = row.Metadata;
            foreach (string key in row.ChangedValues.Keys)
            {
                MetadataColumn c = mt[key];
                SqlStatementExtensions.Set(up, key, c.SqlDataType, row.ChangedValues[key], c.DataType, c.Length, c.Scale);
            }
            if (Output != null && Output.Length > 0)
            {
                TableParameterField tpf = up.Output();
                foreach (string s in Output)
                {
                    MetadataColumn c = mt[s];
                    SqlStatementExtensions.Column(tpf, s, c.SqlDataType, c.Length, c.Scale);
                }
            }
            builder.WhereConditions = row.PrimaryKey(builder);
            return builder;
        }
    }
}
