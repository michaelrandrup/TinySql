using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TinySql
{
    public static class TypeBuilder
    {
        public static SqlBuilder Select(Type ObjectType, string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false)
        {
            SqlBuilder builder = SqlBuilder.Select(Top, Distinct);
            Table BaseTable = null;
            if (string.IsNullOrEmpty(TableName))
            {
                BaseTable = builder.From(ObjectType.Name);
            }
            else
            {
                BaseTable = builder.From(TableName);
            }
            if (Properties == null)
            {
                Properties = ObjectType.GetProperties().Select(x => x.Name).Union(ObjectType.GetFields().Select(x => x.Name)).ToArray();
            }
            if (ExcludeProperties == null)
            {
                ExcludeProperties = new string[0];
            }
            foreach (string Name in Properties.Except(ExcludeProperties))
            {
                BaseTable.Column(Name);
            }
            return builder;
        }

        public static SqlBuilder Select<T>(string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false)
        {
            return Select(typeof(T), TableName, Properties, ExcludeProperties, Top, Distinct);
        }

    }
}
