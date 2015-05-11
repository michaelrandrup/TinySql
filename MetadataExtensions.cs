using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TinySql.Metadata;
using TinySql;

namespace TinySql
{

   

    
    
    public class MetadataHelper
    {
        internal MetadataTable Model { get; set; }
        internal Table Table { get; set; }
    }

    public class MetadataHelper<TClass> : MetadataHelper
    {
        internal TClass Class { get; set; }
        internal Type ClassType
        {
            get { return typeof(TClass); }
        }
        // internal Stack<Table> FromTables = new Stack<Table>();
        internal Stack<MetadataHelper> Helpers = new Stack<MetadataHelper>();
    }

    

    public static class MetadataExtensions
    {
        #region Initialize the helpers

        internal static string GetMetaTableName(Table t)
        {
            string s = t.FullName;
            return s.Contains('.') ? s : "dbo." + s;
        }

        internal static string GetMemberName<TClass,TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass,TProperty>> Property)
        {
            if (Property.Body.NodeType == ExpressionType.MemberAccess)
            {
                return (Property.Body as MemberExpression).Member.Name;
            }
            return null;
        }
        internal static string GetMemberName<TClass, TProperty>(Expression<Func<TClass, TProperty>> Property)
        {
            if (Property.Body.NodeType == ExpressionType.MemberAccess)
            {
                return (Property.Body as MemberExpression).Member.Name;
            }
            return null;
        }

        public static MetadataHelper<TClass> WithMetadata<TClass>(this SqlBuilder Builder, string TableName = null, string Schema = null, string Alias = null)
        {
            
                
            MetadataHelper<TClass> helper = new MetadataHelper<TClass>();
            helper.Model = Builder.Metadata.FindTable(TableName == null ? helper.ClassType.Name : TableName);
            helper.Table = Builder.From(helper.Model.Name, Alias, helper.Model.Schema == "dbo" ? null : helper.Model.Schema);
            return helper;
        }

        public static MetadataHelper WithMetadata(this Table table)
        {
            MetadataDatabase mdb = table.Builder.Metadata;
            if (mdb != null)
            {
                MetadataTable mt = mdb.FindTable(table.FullName);
                if (mt != null)
                {
                    return new MetadataHelper() { Table = table, Model = mt };
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The Table '{0}' was not found in metadata", table.FullName));
                }
            }
            else
            {
                throw new InvalidOperationException("The SqlBuilder does not contain metadata");
            }
        }

        public static SqlBuilder Builder(this MetadataHelper helper)
        {
            return helper.Table.Builder();
        }

        #endregion

        #region Metadata

        public static string[] TitleColumns = new string[] { "name", "title", "description", "fullname", "navn","titel","beskrivelse" };
        public static string GuessTitleColumn(this MetadataTable Table)
        {
            if (!string.IsNullOrEmpty(Table.TitleColumn))
            {
                return Table.TitleColumn;
            }
            else
            {
            
                foreach (string s in TitleColumns)
                {
                    MetadataColumn mc = Table.Columns.Values.FirstOrDefault(x => x.Name.IndexOf(s, StringComparison.OrdinalIgnoreCase) >= 0);
                    if (mc != null) { return mc.Name; }
                }
            }
            return Table.PrimaryKey.Columns.First().Name;
        }

        public static SqlBuilder ToSqlBuilder(this MetadataColumn ForeignKeyColumn)
        {
            MetadataForeignKey FK = ForeignKeyColumn.Parent.FindForeignKeys(ForeignKeyColumn).First();
            MetadataTable PK = SqlBuilder.DefaultMetadata.FindTable(FK.ReferencedSchema + "." + FK.ReferencedTable);
            string namecol = PK.GuessTitleColumn();
            string[] valuecols = FK.ColumnReferences.Where(x => !x.Column.IsComputed).Select(x => x.ReferencedColumn.Name).ToArray();

            SqlBuilder Builder = SqlBuilder.Select()
                .From(PK.Name,null,PK.Schema)
                .Column(namecol)
                .Columns(valuecols)
                .Builder();
            List<MetadataColumnReference> mcrs = FK.ColumnReferences.Where(x => x.Column.IsComputed).ToList();
            if (mcrs.Count > 0)
            {
                MetadataColumnReference first = mcrs.First();
                Builder.From(PK.Name, null, PK.Schema)
                    .Where(PK.Name, first.ReferencedColumn.Name, SqlOperators.Equal, (object)first.Column.Name.Trim('\"'))
                    .Builder();
                foreach (MetadataColumnReference mcr in mcrs.Skip(1))
                {
                    Builder.From(PK.Name, null, PK.Schema)
                    .Where(PK.Name, mcr.ReferencedColumn.Name, SqlOperators.Equal, (object)mcr.Column.Name.Trim('\"'))
                    .Builder();
                }
            }
            

            return Builder;
        }

        public static SqlBuilder ToSqlBuilder(this MetadataTable table, string ListName)
        {
            List<string> columns = new List<string>(table.PrimaryKey.Columns.Select(x => x.Name));
            List<string> columnDef = null;
            List<MetadataColumn> mcs = new List<MetadataColumn>();
            if (table.ListDefinitions.TryGetValue(ListName, out columnDef))
            {
                mcs = table.Columns.Values.Where(x => columnDef.Contains(x.Name)).ToList();
                columns.AddRange(mcs.Select(x => x.Name));
            }

            SqlBuilder Builder =  SqlBuilder.Select()
                .From(table.Name, null, table.Schema).Builder();

            foreach (MetadataColumn mc in mcs.Where(x => x.IsForeignKey))
            {
                Builder.BaseTable().WithMetadata().AutoJoin(mc.Name);
            }

            Builder.From(table.Name, null, table.Schema).Columns(columns.ToArray());
           

            
            return Builder;


        }


        #endregion

        #region Columns

        public static MetadataHelper<TClass> AllColumns<TClass>(this MetadataHelper<TClass> helper)
        {
            PropertyInfo[] props = helper.ClassType.GetProperties();
            for (int i = 0; i < props.Length; i++)
            {
                PropertyInfo p = props[i];
                if (p.CanWrite)
                {
                    if (helper.Model.Columns.ContainsKey(p.Name))
                    {
                        helper.Table.Column(p.Name);
                    }
                }
            }
            return helper;


            throw new NotImplementedException();
        }
        public static MetadataHelper<TClass> Column<TClass,TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass,TProperty>> Property, string MapColumn = null)
        {

            string col = helper.GetMemberName(Property);
            string Alias = null;
            if (MapColumn != null)
            {
                Alias = col;
            }
            helper.Table.Column(MapColumn == null ? col : MapColumn, Alias);
            return helper;
        }

        public static MetadataHelper<TClass> Column<TClass, Tother, TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass, TProperty>> Property, Expression<Func<Tother, TProperty>> IncludeFrom)
        {
            string col = helper.GetMemberName(Property);
            string other = GetMemberName<Tother, TProperty>(IncludeFrom);
            string MapColumn = col.Equals(other) ? null : col;
            return helper.Column(Property,MapColumn);
        }

        #endregion

        public static MetadataHelper<TClass> InnerJoin<TClass,TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass,TProperty>> Property)
        {
            Table to = InnerJoin(helper, helper.GetMemberName(Property));
            MetadataTable model = to.Builder.Metadata.FindTable(to.Name);
            MetadataHelper<TClass> newHelper = new MetadataHelper<TClass>()
            {
                 Model = model,
                  Table = to
            };
            newHelper.Helpers.Push(helper);
            return newHelper;
        }

        public static MetadataHelper<T> ToTable<T,TClass>(this MetadataHelper<TClass> helper)
        {
            MetadataHelper<T> newHelper = new MetadataHelper<T>()
            {
                Helpers = helper.Helpers,
                Model = helper.Model
            };
            helper.Helpers.Push(helper);
            return newHelper;
        }

        

        public static MetadataHelper<TClass> From<TClass>(this MetadataHelper<TClass> helper, string TableName = null)
        {
            MetadataHelper previous = helper.Helpers.Pop();
            if (TableName != null)
            {
                while (previous.Table.Name != TableName && helper.Helpers.Count > 0)
                {
                    previous = helper.Helpers.Pop();
                }
            }
            return (MetadataHelper<TClass>)previous;
        }


        #region Conditions
        public static Table WherePrimaryKey(this MetadataHelper helper, object[] keys)
        {
            MetadataColumn key = helper.Model.PrimaryKey.Columns.First();

            if (helper.Table.Builder.WhereConditions.Conditions.Count == 0)
            {
                helper.Table.Where(helper.Model.Name,key.Name, SqlOperators.Equal,keys[0]);
            }
            for (int i = 1; i < helper.Model.PrimaryKey.Columns.Count; i++)
            {
                helper.Table.Builder.WhereConditions.And(helper.Model.Name, helper.Model.PrimaryKey.Columns[i].Name, SqlOperators.Equal, keys[i]);
            }
            return helper.Table;
        }

        #endregion


        #region Join statements

        public static Table AutoJoin(this MetadataHelper helper, string ForeignKeyField)
        {
            MetadataColumn FromField = null;
            if (!helper.Model.Columns.TryGetValue(ForeignKeyField, out FromField))
            {
                throw new ArgumentException("The Field " + ForeignKeyField + " was not found", "FromField");
            }
            if (FromField.Nullable)
            {
                return LeftJoin(helper, ForeignKeyField);
            }
            else
            {
                return InnerJoin(helper, ForeignKeyField);
            }
        }
        public static Table InnerJoin(this MetadataHelper helper, string ForeignKeyField)
        {
            return JoinInternal(helper, ForeignKeyField, Join.JoinTypes.Inner);
        }

        public static Table LeftJoin(this MetadataHelper helper, string ForeignKeyField)
        {
            return JoinInternal(helper, ForeignKeyField, Join.JoinTypes.LeftOuter);
        }
        public static Table RightJoin(this MetadataHelper helper, string ForeignKeyField)
        {
            return JoinInternal(helper, ForeignKeyField, Join.JoinTypes.RightOuter);
        }
        public static Table CrossJoin(this MetadataHelper helper, string ForeignKeyField)
        {
            return JoinInternal(helper, ForeignKeyField, Join.JoinTypes.Cross);
        }

        public static Table InnerJoin(this MetadataHelper helper, string ToTable, string ToSchema = null)
        {
            return JoinInternal(helper, ToTable, ToSchema == null ? "dbo" : ToSchema, Join.JoinTypes.Inner);
        }

        public static Table LeftJoin(this MetadataHelper helper, string ToTable, string ToSchema = null)
        {
            return JoinInternal(helper, ToTable, ToSchema == null ? "dbo" : ToSchema, Join.JoinTypes.LeftOuter);
        }

        public static Table RightJoin(this MetadataHelper helper, string ToTable, string ToSchema = null)
        {
            return JoinInternal(helper, ToTable, ToSchema == null ? "dbo" : ToSchema, Join.JoinTypes.RightOuter);
        }

        public static Table CrossJoin(this MetadataHelper helper, string ToTable, string ToSchema = null)
        {
            return JoinInternal(helper, ToTable, ToSchema == null ? "dbo" : ToSchema, Join.JoinTypes.Cross);
        }

        private static Table JoinInternal(MetadataHelper helper, string Totable, string ToSchema, Join.JoinTypes JoinType)
        {
            if (helper.Model.PrimaryKey.Columns.Count != 1)
            {
                throw new InvalidOperationException("Only tables with one primary key field is supported");
            }
            MetadataColumn FromField = helper.Model.PrimaryKey.Columns.First();
            MetadataTable mt = helper.Table.Builder.Metadata.FindTable(ToSchema + "." + Totable);
            JoinConditionGroup jcg = To(helper.Table.Builder, helper.Table, helper.Model, FromField, mt, JoinType, false);
            return jcg.ToTable();
        }

        private static Table JoinInternal(MetadataHelper helper, string ForeignKeyField, Join.JoinTypes JoinType)
        {
            MetadataColumn FromField = null;
            if (!helper.Model.Columns.TryGetValue(ForeignKeyField, out FromField))
            {
                throw new ArgumentException("The Field " + ForeignKeyField + " was not found", "FromField");
            }
            List<MetadataForeignKey> Fks = new List<MetadataForeignKey>(helper.Model.FindForeignKeys(FromField));
            if (Fks.Count != 1)
            {
                throw new ArgumentException("The Field " + ForeignKeyField + " points to more than one table", "FromField");
            }
            MetadataForeignKey FK = Fks.First();
            string table = FK.ReferencedSchema + "." + FK.ReferencedTable;
            MetadataTable ToTable = helper.Table.Builder.Metadata.FindTable(table);
            if (ToTable == null)
            {
                throw new InvalidOperationException("The table '" + table + "' was not found in metadata");
            }
            JoinConditionGroup jcg = To(helper.Table.Builder, helper.Table, helper.Model, FromField, ToTable, JoinType, true);
            
            Table toTable = jcg.ToTable();

            if (FromField.IncludeColumns != null)
            {
                foreach (string include in FromField.IncludeColumns)
                {
                    string iName = include;
                    string iAlias = null;
                    if (include.IndexOf('=')>0)
                    {
                        iName = include.Split('=')[0];
                        iAlias = include.Split('=')[1];
                    }
                    toTable.Column(iName, iAlias);
                }
            }

            return toTable;

        }

        private static JoinConditionGroup To(SqlBuilder Builder, Table FromSqlTable, MetadataTable FromTable, MetadataColumn FromField, MetadataTable ToTable, Join.JoinTypes JoinType, bool PreferForeignKeyOverPrimaryKey = true)
        {
            MetadataDatabase mdb = Builder.Metadata;
            List<MetadataForeignKey> Fks = null;
            MetadataForeignKey FK = null;
            Join j = null;
            MetadataColumnReference mcr = null;
            JoinConditionGroup jcg = null;

            if (FromField.IsPrimaryKey)
            {
                if (!FromField.IsForeignKey || !PreferForeignKeyOverPrimaryKey)
                {
                    Fks = ToTable.ForeignKeys.Values.Where(x => x.ReferencedTable.Equals(FromTable.Name) && x.ReferencedSchema.Equals(FromTable.Schema) && x.ColumnReferences.Any(y => y.ReferencedColumn.Equals(FromField))).ToList();
                    if (Fks.Count != 1)
                    {
                        throw new InvalidOperationException(string.Format("The column '{0}' is referenced by {1} keys in the table {2}. Expected 1. Make the join manually",
                            FromField.Name, Fks.Count, ToTable.Fullname));
                    }
                    FK = Fks.First();
                    j = SqlStatementExtensions.MakeJoin(JoinType, FromSqlTable, ToTable.Name, null, ToTable.Schema.Equals("dbo") ? null : ToTable.Schema);

                    mcr = FK.ColumnReferences.First();
                    jcg = j.On(FromField.Name, SqlOperators.Equal, mcr.Name);
                    return jcg;
                }
            }
            if (FromField.IsForeignKey)
            {
                Fks = new List<MetadataForeignKey>(FromTable.FindForeignKeys(FromField, ToTable.Name));
                if (Fks.Count != 1)
                {
                    throw new InvalidOperationException(string.Format("The column '{0}' resolves to {1} keys in the table {2}. Expected 1. Make the join manually",
                            FromField.Name, Fks.Count, ToTable.Fullname));
                }
                FK = Fks.First();
                j = SqlStatementExtensions.MakeJoin(JoinType, FromSqlTable, ToTable.Name, null, ToTable.Schema.Equals("dbo") ? null : ToTable.Schema);
                mcr = FK.ColumnReferences.First();
                jcg = j.On(FromField.Name, SqlOperators.Equal, mcr.ReferencedColumn.Name);
                
                if (FK.ColumnReferences.Count > 1)
                {
                    foreach (MetadataColumnReference mcr2 in FK.ColumnReferences.Skip(1))
                    {
                        if (mcr2.Name.StartsWith("\""))
                        {
                            // its a value reference
                            // jcg.And(FK.ReferencedTable, mcr2.ReferencedColumn.Name, SqlOperators.Equal, mcr2.Name.Trim('\"'),null);

                            decimal d;
                            object o;
                            if (decimal.TryParse(mcr2.Name.Trim('\"'),out d))
                            {
                                o = d;
                            }
                            else
                            {
                                o = (object)mcr2.Name.Trim('\"');
                            }

                            jcg.And(mcr2.ReferencedColumn.Name, SqlOperators.Equal,o, null);
                        }
                        else
                        {
                            jcg.And(mcr2.Column.Name, SqlOperators.Equal, mcr2.ReferencedColumn.Name);
                        }
                    }
                }
                return jcg;
            }
            throw new ArgumentException(string.Format("The Column '{0}' in the table '{1}' must be a foreign key or primary key", FromField.Name, FromTable.Fullname), "FromField");
        }

        #endregion






    }
}
