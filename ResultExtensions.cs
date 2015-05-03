using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TinySql.Metadata;

namespace TinySql
{
    public static class ResultExtensions
    {
        public static SqlBuilder Update(this SqlBuilder builder, RowData row, string[] Output = null)
        {
            if (!row.HasChanges)
            {
                return builder;
            }
            UpdateTable up = builder.Table(row.Table);
            Metadata.MetadataTable mt = row.Parent.Metadata;
            foreach (string key in row.ChangedValues.Keys)
            {
                MetadataColumn c = mt[key];
                SqlStatementExtensions.Set(up,key, c.SqlDataType, row.ChangedValues[key],c.DataType, c.Length, c.Scale);
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
