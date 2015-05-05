using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinySql.Metadata;

namespace TinySql
{

    #region Model Helper classes
    public class TableModel<TModel>
    {
        public TableModel(string Alias = null, string Schema = null)
        {
            this.TableName = typeof(TModel).Name;
            this.Alias = Alias;
            this.Schema = Schema;
        }
        public string TableName { get; internal set; }
        public string Schema { get; set; }
        public string Alias { get; set; }
        public Table Model { get; internal set; }

        public MetadataTable GetTable(string Name = null, string Schema = "dbo")
        {
            if (string.IsNullOrEmpty(Name))
            {
                string Fullname = !string.IsNullOrEmpty(Model.Schema) ? Model.FullName : Schema + "." + Model.Name;
                return Model.Builder.Metadata[Fullname];
            }
            else
            {
                if (string.IsNullOrEmpty(Schema))
                {
                    return Model.Builder.Metadata.FindTable(Name);
                }
                else
                {
                    return Model.Builder.Metadata[Schema + "." + Name];
                }
            }
        }

        public MetadataColumn GetColumn<TModel, TProperty>(Expression<Func<TModel, TProperty>> Field, bool UseInheritance = true, string FromTable = null)
        {
            string Name = "";
            string table = "";
            if (Field.Body.NodeType == ExpressionType.MemberAccess)
            {
                Name = ((MemberExpression)Field.Body).Member.Name;
                if (FromTable == null)
                {
                    FromTable = ((MemberExpression)Field.Body).Member.DeclaringType.Name;
                }

            }
            else
            {
                throw new ArgumentException(string.Format("The Expression type '{0}' must be MemberAccess", Field.Body.NodeType), "Field");
            }
            FromTable = !UseInheritance || TableName.Equals(FromTable) ? null : FromTable;
            MetadataTable mt = GetTable(FromTable);
            return mt.Columns[Name];
        }

        public T BuildField<T>() where T : class
        {
            T field = Activator.CreateInstance<T>();
            (field as Field).Table = Model;
            (field as Field).Builder = Model.Builder;
            return field;
        }
    }

    public class JoinModel<TModel>
    {
        public JoinModel(TableModel<TModel> fromTable, Join join, MetadataForeignKey FK)
        {
            FromTable = fromTable;
            Model = join;
            ForeignKey = FK;
        }

        public TableModel<TModel> FromTable { get; private set; }
        public Join Model { get; private set; }

        public MetadataForeignKey ForeignKey { get; private set; }
        public string Alias { get; set; }

        private TableModel<TModel> _ToTable = null;
        public TableModel<TModel> ToTable
        {
            get
            {
                if (_ToTable == null)
                {
                    _ToTable = new TableModel<TModel>(Alias, ForeignKey.ReferencedSchema);
                    _ToTable.TableName = ForeignKey.ReferencedTable;
                    _ToTable.Model = Model.ToTable;
                }
                return _ToTable;
            }
        }




    }

    #endregion

    public static class MetadataExtensions
    {

        public static TableModel<TModel> From<TModel>(this SqlBuilder builder, string Alias = null, string Schema = null)
        {
            TableModel<TModel> table = new TableModel<TModel>(Alias, Schema);
            table.Model = builder.From(table.TableName, Alias, Schema);
            return table;
        }

        public static TableModel<TModel> Column<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string FromTable, string FromtableSchema = null, string Alias = null)
        {
            MetadataColumn col = table.GetColumn(Field, true, FromTable);
            if (col != null)
            {
                Field field = table.BuildField<Field>();
                col.PopulateField<Field>(field);
                table.Model.FieldList.Add(field);
            }
            else
            {
                table.Model.Column(Field.Body.Type.Name, Alias);
            }
            return table;
        }



        public static TableModel<TModel> Column<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null)
        {
            MetadataColumn col = table.GetColumn(Field);
            if (col != null)
            {
                Field field = table.BuildField<Field>();
                col.PopulateField<Field>(field);
                table.Model.FieldList.Add(field);
            }
            else
            {
                table.Model.Column(Field.Body.Type.Name, Alias);
            }
            return table;
        }

        public static void PrimaryKeyJoin<TModel>(this TableModel<TModel> table)
        {

        }

        public static JoinModel<TModel> InnerJoin<TModel>(this TableModel<TModel> table, string TableName, string Schema = null, string Alias = null)
        {
            MetadataTable FKTable = null;
            if (string.IsNullOrEmpty(Schema))
            {
                FKTable = table.Model.Builder.Metadata.FindTable(TableName);
            }
            else
            {
                FKTable = table.Model.Builder.Metadata[Schema + "." + TableName];
            }
            return InnerJoin<TModel>(table, FKTable, Alias);
        }
        public static JoinModel<TModel> InnerJoin<TModel>(this TableModel<TModel> table, MetadataTable FKTable, string Alias = null)
        {

            TableModel<TModel> FromTable = From<TModel>(table.Model.Builder, Alias, FKTable.Schema);
            FromTable.TableName = FKTable.Name;
            MetadataTable mt = table.GetTable();
            string PK = mt.PrimaryKey.Name;
            MetadataForeignKey FK = null;
            foreach (MetadataForeignKey key in FKTable.ForeignKeys.Values)
            {
                if (key.ReferencedKey.Equals(PK))
                {
                    FK = key;
                    break;
                }
            }
            if (FK == null)
            {
                throw new ArgumentException(string.Format("Table '{0}' does not reference {1}", FKTable.Name, mt.Name), "FKTable");
            }
            return JoinInternal<TModel>(table, FK, Join.JoinTypes.Inner, Alias);
        }

        public static JoinModel<TModel> InnerJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        {
            return JoinInternal(table, Field, Join.JoinTypes.Inner, Alias, ForeignKey);
        }

        public static JoinModel<TModel> LeftJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        {
            return JoinInternal(table, Field, Join.JoinTypes.LeftOuter, Alias, ForeignKey);
        }

        public static JoinModel<TModel> RightJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        {
            return JoinInternal(table, Field, Join.JoinTypes.RightOuter, Alias, ForeignKey);
        }
        public static JoinModel<TModel> CrossJoin<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> Field, string Alias = null, string ForeignKey = null)
        {
            return JoinInternal(table, Field, Join.JoinTypes.Cross, Alias, ForeignKey);
        }


        private static JoinModel<TModel> JoinInternal<TModel>(TableModel<TModel> FromTable, MetadataForeignKey FK, Join.JoinTypes JoinType, string Alias = null)
        {
            Join join = SqlStatementExtensions.MakeJoin(JoinType, FromTable.Model, FK.ReferencedTable, Alias, FK.ReferencedSchema);

            MetadataColumnReference mcr = FK.ColumnReferences.First();
            Field lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
            if (lf == null)
            {
                lf = new Field()
                {
                    Table = FromTable.Model,
                    Builder = FromTable.Model.Builder
                };
                mcr.Column.PopulateField<Field>(lf);
            }
            Field rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = join.ToTable,
                    Builder = join.Builder
                };
                mcr.ReferencedColumn.PopulateField<Field>(rf);
            }

            SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.None);
            foreach (MetadataColumnReference cols in FK.ColumnReferences.Skip(1))
            {
                lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
                rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
                if (lf == null)
                {
                    lf = new Field()
                    {
                        Table = FromTable.Model,
                        Builder = FromTable.Model.Builder,
                    };
                    cols.Column.PopulateField<Field>(lf);
                    if (rf == null)
                    {
                        rf = new Field()
                        {
                            Table = join.ToTable,
                            Builder = join.Builder
                        };
                        cols.ReferencedColumn.PopulateField<Field>(rf);
                    }
                    SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.And);
                }
            }

            JoinModel<TModel> model = new JoinModel<TModel>(FromTable, join, FK);
            return model;
        }

        private static JoinModel<TModel> JoinInternal<TModel, TProperty>(this TableModel<TModel> table, Expression<Func<TModel, TProperty>> ForeignKeyField, Join.JoinTypes JoinType = Join.JoinTypes.Inner, string Alias = null, string ForeignKey = null)
        {
            MetadataColumn col = table.GetColumn(ForeignKeyField, false);
            MetadataTable mt = table.GetTable();
            MetadataForeignKey FK = null;
            if (col.IsForeignKey)
            {
                if (!string.IsNullOrEmpty(ForeignKey))
                {
                    if (!mt.ForeignKeys.TryGetValue(ForeignKey, out FK))
                    {
                        throw new ArgumentException(string.Format("Unable to get a foreign key with the name '{0}'", ForeignKey), "ForeignKey");
                    }
                }
                else
                {
                    IEnumerable<MetadataForeignKey> FKs = mt.FindForeignKeys(col);
                    if (FKs.Count() != 1)
                    {
                        throw new ArgumentException(string.Format("{0} ForeignKeys for the Column {1} found. Cannot infer which one to use. Use the ForeignKey parameter to specify an exact Foreign key", FKs.Count(), col.Name), "Field");
                    }
                    else
                    {
                        FK = FKs.First();
                    }
                }
                Join join = SqlStatementExtensions.MakeJoin(JoinType, table.Model, FK.ReferencedTable, Alias, FK.ReferencedSchema);

                MetadataColumnReference mcr = FK.ColumnReferences.First();
                Field lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
                if (lf == null)
                {
                    lf = new Field()
                    {
                        Table = table.Model,
                        Builder = table.Model.Builder,
                    };
                    mcr.Column.PopulateField<Field>(lf);
                }
                Field rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(mcr.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
                if (rf == null)
                {
                    rf = new Field()
                    {
                        Table = join.ToTable,
                        Builder = join.Builder
                    };
                    mcr.ReferencedColumn.PopulateField<Field>(rf);
                }

                SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.None);
                foreach (MetadataColumnReference cols in FK.ColumnReferences.Skip(1))
                {
                    lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.Column.Name, StringComparison.InvariantCultureIgnoreCase)));
                    rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(cols.ReferencedColumn.Name, StringComparison.InvariantCultureIgnoreCase)));
                    if (lf == null)
                    {
                        lf = new Field()
                        {
                            Table = table.Model,
                            Builder = table.Model.Builder,
                        };
                        cols.Column.PopulateField<Field>(lf);
                        if (rf == null)
                        {
                            rf = new Field()
                            {
                                Table = join.ToTable,
                                Builder = join.Builder
                            };
                            cols.ReferencedColumn.PopulateField<Field>(rf);
                        }
                        SqlStatementExtensions.OnInternal(join.Conditions, lf, SqlOperators.Equal, rf, BoolOperators.And);
                    }
                }

                JoinModel<TModel> model = new JoinModel<TModel>(table, join, FK);
                return model;
            }
            else
            {
                throw new ArgumentException(string.Format("The field '{0}' is not a foreign key", col.Name), "ForeignKey");
            }
        }




    }
}
