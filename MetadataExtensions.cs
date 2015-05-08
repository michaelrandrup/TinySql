using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TinySql.Metadata;

namespace TinySql
{

    #region Model Helper classes
    //public class TableModel<TModel>
    //{
    //    public TableModel(string Alias = null, string Schema = null)
    //    {
    //        this.TableName = typeof(TModel).Name;
    //        this.Alias = Alias;
    //        this.Schema = Schema;
    //    }
    //    public string TableName { get; internal set; }
    //    public string Schema { get; set; }
    //    public string Alias { get; set; }
    //    public Table Model { get; internal set; }

    //    public MetadataTable GetTable(string Name = null, string Schema = "dbo")
    //    {
    //        if (string.IsNullOrEmpty(Name))
    //        {
    //            string Fullname = !string.IsNullOrEmpty(Model.Schema) ? Model.FullName : Schema + "." + Model.Name;
    //            return Model.Builder.Metadata[Fullname];
    //        }
    //        else
    //        {
    //            if (string.IsNullOrEmpty(Schema))
    //            {
    //                return Model.Builder.Metadata.FindTable(Name);
    //            }
    //            else
    //            {
    //                return Model.Builder.Metadata[Schema + "." + Name];
    //            }
    //        }
    //    }

    //    public MetadataColumn GetColumn<TModel, TProperty>(Expression<Func<TModel, TProperty>> Field, bool UseInheritance = true, string FromTable = null)
    //    {
    //        string Name = "";
    //        string table = "";
    //        if (Field.Body.NodeType == ExpressionType.MemberAccess)
    //        {
    //            Name = ((MemberExpression)Field.Body).Member.Name;
    //            if (FromTable == null)
    //            {
    //                FromTable = ((MemberExpression)Field.Body).Member.DeclaringType.Name;
    //            }

    //        }
    //        else
    //        {
    //            throw new ArgumentException(string.Format("The Expression type '{0}' must be MemberAccess", Field.Body.NodeType), "Field");
    //        }
    //        FromTable = !UseInheritance || TableName.Equals(FromTable) ? null : FromTable;
    //        MetadataTable mt = GetTable(FromTable);
    //        return mt.Columns[Name];
    //    }

    //    public T BuildField<T>() where T : class
    //    {
    //        T field = Activator.CreateInstance<T>();
    //        (field as Field).Table = Model;
    //        (field as Field).Builder = Model.Builder;
    //        return field;
    //    }
    //}

    //public class JoinModel<TModel>
    //{
    //    public JoinModel(TableModel<TModel> fromTable, Join join, MetadataForeignKey FK)
    //    {
    //        FromTable = fromTable;
    //        Model = join;
    //        ForeignKey = FK;
    //    }

    //    public TableModel<TModel> FromTable { get; private set; }
    //    public Join Model { get; private set; }

    //    public MetadataForeignKey ForeignKey { get; private set; }
    //    public string Alias { get; set; }

    //    private TableModel<TModel> _ToTable = null;
    //    public TableModel<TModel> ToTable
    //    {
    //        get
    //        {
    //            if (_ToTable == null)
    //            {
    //                _ToTable = new TableModel<TModel>(Alias, ForeignKey.ReferencedSchema);
    //                _ToTable.TableName = ForeignKey.ReferencedTable;
    //                _ToTable.Model = Model.ToTable;
    //            }
    //            return _ToTable;
    //        }
    //    }




    //}

    
    
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

    #endregion

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
            string table = Fks.First().ReferencedSchema + "." + Fks.First().ReferencedTable;
            MetadataTable ToTable = helper.Table.Builder.Metadata.FindTable(table);
            if (ToTable == null)
            {
                throw new InvalidOperationException("The table '" + table + "' was not found in metadata");
            }
            JoinConditionGroup jcg = To(helper.Table.Builder, helper.Table, helper.Model, FromField, ToTable, JoinType, true);
            return jcg.ToTable();
        }

        private static JoinConditionGroup To(SqlBuilder Builder, Table FromSqlTable, MetadataTable FromTable, MetadataColumn FromField, MetadataTable ToTable, Join.JoinTypes JoinType, bool PreferForeignKeyOverPrimaryKey = true)
        {
            MetadataDatabase mdb = Builder.Metadata;
            List<MetadataForeignKey> Fks = null;
            MetadataForeignKey FK = null;
            Join j = null;
            MetadataColumnReference mcr = null;
            JoinConditionGroup jcg = null;
            bool joined = false;

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
                jcg = j.On(mcr.Name, SqlOperators.Equal, mcr.ReferencedColumn.Name);
                return jcg;
            }
            throw new ArgumentException(string.Format("The Column '{0}' in the table '{1}' must be a foreign key or primary key", FromField.Name, FromTable.Fullname), "FromField");
        }



        //public static TableModel<TModel> From<TModel>(this SqlBuilder builder, string Alias = null, string Schema = null)
        //{
        //    TableModel<TModel> table = new TableModel<TModel>(Alias, Schema);
        //    table.Model = builder.From(table.TableName, Alias, Schema);
        //    return table;
        //}

        //public static TableModel<TModel> Column<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string FromTable, string FromtableSchema = null, string Alias = null)
        //{
        //    MetadataColumn col = table.GetColumn(Field, true, FromTable);
        //    if (col != null)
        //    {
        //        Field field = table.BuildField<Field>();
        //        col.PopulateField<Field>(field);
        //        table.Model.FieldList.Add(field);
        //    }
        //    else
        //    {
        //        table.Model.Column(Field.Body.Type.Name, Alias);
        //    }
        //    return table;
        //}



        //public static TableModel<TModel> Column<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null)
        //{
        //    MetadataColumn col = table.GetColumn(Field);
        //    if (col != null)
        //    {
        //        Field field = table.BuildField<Field>();
        //        col.PopulateField<Field>(field);
        //        table.Model.FieldList.Add(field);
        //    }
        //    else
        //    {
        //        table.Model.Column(Field.Body.Type.Name, Alias);
        //    }
        //    return table;
        //}

        //public static void PrimaryKeyJoin<TModel>(this TableModel<TModel> table)
        //{

        //}

        //public static JoinModel<TModel> InnerJoin<TModel>(this TableModel<TModel> table, string TableName, string Schema = null, string Alias = null)
        //{
        //    MetadataTable FKTable = null;
        //    if (string.IsNullOrEmpty(Schema))
        //    {
        //        FKTable = table.Model.Builder.Metadata.FindTable(TableName);
        //    }
        //    else
        //    {
        //        FKTable = table.Model.Builder.Metadata[Schema + "." + TableName];
        //    }
        //    return InnerJoin<TModel>(table, FKTable, Alias);
        //}
        //public static JoinModel<TModel> InnerJoin<TModel>(this TableModel<TModel> table, MetadataTable FKTable, string Alias = null)
        //{

        //    TableModel<TModel> FromTable = From<TModel>(table.Model.Builder, Alias, FKTable.Schema);
        //    FromTable.TableName = FKTable.Name;
        //    MetadataTable mt = table.GetTable();
        //    string PK = mt.PrimaryKey.Name;
        //    MetadataForeignKey FK = null;
        //    foreach (MetadataForeignKey key in FKTable.ForeignKeys.Values)
        //    {
        //        if (key.ReferencedKey.Equals(PK))
        //        {
        //            FK = key;
        //            break;
        //        }
        //    }
        //    if (FK == null)
        //    {
        //        throw new ArgumentException(string.Format("Table '{0}' does not reference {1}", FKTable.Name, mt.Name), "FKTable");
        //    }
        //    return JoinInternal<TModel>(table, FK, Join.JoinTypes.Inner, Alias);
        //}

        //public static JoinModel<TModel> InnerJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        //{
        //    return JoinInternal(table, Field, Join.JoinTypes.Inner, Alias, ForeignKey);
        //}

        //public static JoinModel<TModel> LeftJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        //{
        //    return JoinInternal(table, Field, Join.JoinTypes.LeftOuter, Alias, ForeignKey);
        //}

        //public static JoinModel<TModel> RightJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        //{
        //    return JoinInternal(table, Field, Join.JoinTypes.RightOuter, Alias, ForeignKey);
        //}
        //public static JoinModel<TModel> CrossJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        //{
        //    return JoinInternal(table, Field, Join.JoinTypes.Cross, Alias, ForeignKey);
        //}


        //private static JoinModel<TModel> JoinInternal<TModel>(TableModel<TModel> FromTable, MetadataForeignKey FK, Join.JoinTypes JoinType, string Alias = null)
        //{
        //    Join join = SqlStatementExtensions.MakeJoin(JoinType, FromTable.Model, FK.ReferencedTable, Alias, FK.ReferencedSchema);

        //    MetadataColumnReference mcr = FK.ColumnReferences.First();
        //    Field lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
        //    if (lf == null)
        //    {
        //        lf = new Field()
        //        {
        //            Table = FromTable.Model,
        //            Builder = FromTable.Model.Builder
        //        };
        //        mcr.Column.PopulateField<Field>(lf);
        //    }
        //    Field rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
        //    if (rf == null)
        //    {
        //        rf = new Field()
        //        {
        //            Table = join.ToTable,
        //            Builder = join.Builder
        //        };
        //        mcr.ReferencedColumn.PopulateField<Field>(rf);
        //    }

        //    SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.None);
        //    foreach (MetadataColumnReference cols in FK.ColumnReferences.Skip(1))
        //    {
        //        lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
        //        rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
        //        if (lf == null)
        //        {
        //            lf = new Field()
        //            {
        //                Table = FromTable.Model,
        //                Builder = FromTable.Model.Builder,
        //            };
        //            cols.Column.PopulateField<Field>(lf);
        //            if (rf == null)
        //            {
        //                rf = new Field()
        //                {
        //                    Table = join.ToTable,
        //                    Builder = join.Builder
        //                };
        //                cols.ReferencedColumn.PopulateField<Field>(rf);
        //            }
        //            SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.And);
        //        }
        //    }

        //    JoinModel<TModel> model = new JoinModel<TModel>(FromTable, join, FK);
        //    return model;
        //}

        //private static JoinModel<TModel> JoinInternal<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> ForeignKeyField, Join.JoinTypes JoinType = Join.JoinTypes.Inner, string Alias = null, string ForeignKey = null)
        //{
        //    MetadataColumn col = table.GetColumn(ForeignKeyField, false);
        //    MetadataTable mt = table.GetTable();
        //    MetadataForeignKey FK = null;
        //    if (col.IsForeignKey)
        //    {
        //        if (!string.IsNullOrEmpty(ForeignKey))
        //        {
        //            if (!mt.ForeignKeys.TryGetValue(ForeignKey, out FK))
        //            {
        //                throw new ArgumentException(string.Format("Unable to get a foreign key with the name '{0}'", ForeignKey), "ForeignKey");
        //            }
        //        }
        //        else
        //        {
        //            IEnumerable<MetadataForeignKey> FKs = mt.FindForeignKeys(col);
        //            if (FKs.Count() != 1)
        //            {
        //                throw new ArgumentException(string.Format("{0} ForeignKeys for the Column {1} found. Cannot infer which one to use. Use the ForeignKey parameter to specify an exact Foreign key", FKs.Count(), col.Name), "Field");
        //            }
        //            else
        //            {
        //                FK = FKs.First();
        //            }
        //        }
        //        Join join = SqlStatementExtensions.MakeJoin(JoinType, table.Model, FK.ReferencedTable, Alias, FK.ReferencedSchema);

        //        MetadataColumnReference mcr = FK.ColumnReferences.First();
        //        Field lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
        //        if (lf == null)
        //        {
        //            lf = new Field()
        //            {
        //                Table = table.Model,
        //                Builder = table.Model.Builder,
        //            };
        //            mcr.Column.PopulateField<Field>(lf);
        //        }
        //        Field rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
        //        if (rf == null)
        //        {
        //            rf = new Field()
        //            {
        //                Table = join.ToTable,
        //                Builder = join.Builder
        //            };
        //            mcr.ReferencedColumn.PopulateField<Field>(rf);
        //        }

        //        SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.None);
        //        foreach (MetadataColumnReference cols in FK.ColumnReferences.Skip(1))
        //        {
        //            lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
        //            rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
        //            if (lf == null)
        //            {
        //                lf = new Field()
        //                {
        //                    Table = table.Model,
        //                    Builder = table.Model.Builder,
        //                };
        //                cols.Column.PopulateField<Field>(lf);
        //                if (rf == null)
        //                {
        //                    rf = new Field()
        //                    {
        //                        Table = join.ToTable,
        //                        Builder = join.Builder
        //                    };
        //                    cols.ReferencedColumn.PopulateField<Field>(rf);
        //                }
        //                SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.And);
        //            }
        //        }

        //        JoinModel<TModel> model = new JoinModel<TModel>(table, join, FK);
        //        return model;
        //    }
        //    else
        //    {
        //        throw new ArgumentException(string.Format("The field '{0}' is not a foreign key", col.Name), "ForeignKey");
        //    }
        //}




    }
}
