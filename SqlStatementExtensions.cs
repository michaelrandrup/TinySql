using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using TinySql.Metadata;

namespace TinySql
{
    public class TableHelper<TModel>
    {
        public TModel Model;
        public Table table;
    }
    public static class SqlStatementExtensions
    {
        #region Support functions
        public static Table FindTable(this SqlBuilder builder, string TableNameOrAlias, string Schema = null)
        {
            Table found = builder.Tables.FirstOrDefault(x => x.Name.Equals(TableNameOrAlias, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableNameOrAlias, StringComparison.InvariantCultureIgnoreCase)));
            if (found != null)
            {
                return found;
            }
            else if (builder.ParentBuilder != null)
            {
                SqlBuilder b = builder.ParentBuilder;
                while (b != null && found == null)
                {
                    found = builder.Tables.FirstOrDefault(x => x.Name.Equals(TableNameOrAlias, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableNameOrAlias, StringComparison.InvariantCultureIgnoreCase)));
                    b = b.ParentBuilder;
                }
            }
            return found;
        }

        public static Field FindField(this Table table, string NameOrAlias)
        {
            return table.FieldList.FirstOrDefault(x => x.Name.Equals(NameOrAlias, StringComparison.OrdinalIgnoreCase) || (x.Alias != null && x.Alias.Equals(NameOrAlias, StringComparison.OrdinalIgnoreCase)));
        }

        public static Table Property<TModel, TProperty>(this TableHelper<TModel> helper, Expression<Func<TModel, TProperty>> expression)
        {
            Expression exp = expression.ReduceExtensions();
            return helper.table;
        }


        #endregion

        #region Stored Procedures

        public static StoredProcedure Parameter(this StoredProcedure proc, string Name, SqlDbType SqlDataType, object Value, Type DataType, int MaxLength = -1, int Scale = -1, int Precision = -1, bool IsOutput = false)
        {
            proc.Parameters.Add(new ParameterField()
            {
                Builder = proc.Builder,
                Name = Name,
                ParameterName = "@" + Name,
                MaxLength = MaxLength,
                Scale = Scale,
                Precision = Precision,
                SqlDataType = SqlDataType,
                DataType = DataType,
                Value = Value,
                IsOutput = IsOutput
            });
            return proc;
        }

        public static SqlBuilder Builder(this StoredProcedure proc)
        {
            return proc.Builder.Builder();
        }

        public static StoredProcedure Parameter<T>(this StoredProcedure proc, string Name, SqlDbType SqlDataType, T Value, int MaxLength = -1, int Scale = -1, int Precision = -1)
        {

            return Parameter(proc, Name, SqlDataType, Value, typeof(T), MaxLength, Scale, Precision);
        }

        public static StoredProcedure Output<T>(this StoredProcedure proc, string Name, SqlDbType SqlDataType, int MaxLength = -1, int Scale = -1, int Precision = -1)
        {

            return Parameter(proc, Name, SqlDataType, null, typeof(T), MaxLength, Scale, Precision, true);
        }



        #endregion

        #region Update statement

        public static UpdateTable Table(this SqlBuilder builder, string TableName, string Schema = null)
        {
            if (builder.Tables.Count > 0 && builder.BaseTable() is UpdateTable)
            {
                return builder.BaseTable() as UpdateTable;
            }
            else
            {
                UpdateTable t = new UpdateTable(builder, TableName, Schema);
                builder.Tables.Add(t);
                return t;
            }
        }

        public static UpdateTable Set(this UpdateTable table, string FieldName, object Value, SqlDbType SqlDataType, Type DataType, int MaxLength = -1, int Precision = -1, int Scale = -1)
        {
            table.FieldList.Add(new ParameterField()
            {
                Builder = table.Builder,
                Name = FieldName,
                ParameterName = "@" + FieldName,
                MaxLength = MaxLength,
                Precision = Precision,
                Scale = Scale,
                SqlDataType = SqlDataType,
                DataType = DataType,
                Value = Value


            });
            return table;
        }

        public static UpdateTable Set<T>(this UpdateTable table, string FieldName, T Value, SqlDbType DataType, int MaxLength = -1, int Scale = -1)
        {
            table.FieldList.Add(new ParameterField<T>()
            {
                Builder = table.Builder,
                Name = FieldName,
                ParameterName = "@" + FieldName,
                MaxLength = MaxLength,
                Scale = Scale,
                SqlDataType = DataType,
                FieldValue = Value
            });
            return table;
        }

        public static TableParameterField Output(this UpdateTable table, string ParameterName = null)
        {
            if (ParameterName == null)
            {
                ParameterName = "output" + table.Name.Replace(".", "");
            }
            table.Output = new TableParameterField()
            {
                Name = ParameterName,
                ParameterName = "@" + ParameterName,
                ParameterTable = new Table(table.Builder, "inserted", ""),
                Builder = table.Builder,
                Table = table
            };
            return table.Output;
        }

        public static TableParameterField PrimaryKey(this TableParameterField table)
        {
            MetadataTable mt = table.Table.Builder.Metadata.FindTable(table.Table.FullName);
            if (mt == null)
            {
                throw new InvalidOperationException("Metadata for the table " + table.Table.FullName + " could not be loaded");
            }
            foreach (MetadataColumn col in mt.PrimaryKey.Columns)
            {
                table.Column(col.Name, col.SqlDataType, col.Length, col.Precision, col.Scale);
            }
            return table;
        }
        public static TableParameterField Column(this TableParameterField table, string FieldName, SqlDbType DataType, int MaxLength = -1, int Precision = -1, int Scale = -1)
        {

            table.ParameterTable.FieldList.Add(new Field()
            {
                Builder = table.Builder,
                Name = FieldName,
                MaxLength = MaxLength,
                Precision = Precision,
                Scale = Scale,
                SqlDataType = DataType,
                Table = table.ParameterTable
            }.PopulateField()
            );
            return table;
        }

        public static UpdateTable UpdateTable(this TableParameterField table)
        {
            return table.Builder.BaseTable() as UpdateTable;
        }



        #endregion

        #region Insert Statement

        public static TableParameterField Output(this InsertIntoTable table, string ParameterName = null)
        {
            if (ParameterName == null)
            {
                ParameterName = "output" + table.Name;
            }
            table.Output = new TableParameterField()
            {
                ParameterName = "@" + ParameterName,
                Name = ParameterName,
                ParameterTable = new Table(table.Builder, "inserted", ""),
                Builder = table.Builder,
                Table = table
            };
            return table.Output;
        }

        public static InsertIntoTable InsertTable(this TableParameterField table)
        {
            return table.Table as InsertIntoTable;
        }



        public static InsertIntoTable Into(this SqlBuilder builder, string TableName, string Schema = null)
        {
            if (builder.Tables.Count > 0 && builder.BaseTable() is InsertIntoTable)
            {
                return builder.BaseTable() as InsertIntoTable;
            }
            else
            {
                InsertIntoTable t = new InsertIntoTable(builder, TableName);
                t.Schema = Schema;
                builder.Tables.Add(t);
                return t;
            }
        }

        public static InsertIntoTable Value<T>(this InsertIntoTable table, string FieldName, T Value, SqlDbType DataType, int MaxLength = -1, int Scale = -1)
        {
            Field f = new ParameterField<T>()
            {
                Builder = table.Builder,
                Name = FieldName,
                ParameterName = "@" + FieldName,
                MaxLength = MaxLength,
                Scale = Scale,
                SqlDataType = DataType,
                FieldValue = Value
            };

            table.FieldList.Add(f.PopulateField());
            return table;
        }



        #endregion

        #region Functions
        public static BuiltinFn Fn(this Table table)
        {
            return BuiltinFn.Fn(table.Builder, table);
        }
        public static Table ToTable(this BuiltinFn fn)
        {
            return fn.table;
        }


        #endregion

        #region Table

        public static SqlBuilder Builder(this Table table)
        {
            SqlBuilder b = table.Builder;
            while (b.ParentBuilder != null)
            {
                b = b.ParentBuilder;
            }
            return b;
        }

        public static SqlBuilder Builder(this WhereConditionGroup group)
        {
            return group.Builder.Builder();
        }
        public static SqlBuilder Builder(this SqlBuilder Builder)
        {
            SqlBuilder b = Builder;
            while (b.ParentBuilder != null)
            {
                b = b.ParentBuilder;
            }
            return b;
        }

        public static SqlBuilder Builder(this TableParameterField field)
        {
            return field.Builder.Builder();
        }



        public static Table From(this Table sql, string TableName, string Alias = null)
        {
            return sql.Builder.From(TableName, Alias);
        }
        public static Table From(this SqlBuilder sql, string TableName, string Alias = null, string Schema = null)
        {
            Table table = sql.FindTable(Alias ?? TableName, Schema);
            if (table != null && !sql.JoinConditions.Select(x => x.ToTable).Any(x => x.Equals(table)))
            {
                return table;
            }
            table = new Table(sql, TableName, string.IsNullOrEmpty(Alias) ? "t" + sql.Tables.Count.ToString() : Alias, Schema);
            sql.Tables.Add(table);
            return table;
        }
        public static Table ToTable(this JoinConditionGroup group)
        {
            return group.Join.ToTable;
        }

        public static Table Into(this Table table, string TempTable, bool OutputTable = true)
        {
            if (table.Builder.SelectIntoTable != null)
            {
                return table;
            }
            table.Builder.SelectIntoTable = new TempTable()
            {
                Builder = table.Builder,
                Name = TempTable
            };
            table.Builder.SelectIntoTable.AllColumns(true);
            return table;
        }

        public static TempTable OrderBy(this TempTable table, string FieldName, OrderByDirections Direction)
        {
            Field field = table.FindField(FieldName);
            if (field == null)
            {
                if (table.FieldList.Any(x => x.Name.Equals("*")))
                {
                    field = new Field()
                    {
                        Builder = table.Builder,
                        Table = table,
                        Alias = null,
                        Name = FieldName
                    };
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The field '{0}' was not found in the table '{1}'", FieldName, table.Name));
                }
            }
            if (!table.OrderByClause.Any(x => x.Field.ReferenceName.Equals(field.ReferenceName)))
            {
                table.OrderByClause.Add(new OrderBy()
                {
                    Field = field,
                    Direction = Direction
                });
            }
            return table;
        }

        public static Table OrderBy(this Table table, string FieldName, OrderByDirections Direction)
        {
            Field field = table.FindField(FieldName);
            if (field == null)
            {
                if (table.FieldList.Any(x => x.Name.Equals("*")))
                {
                    field = new Field()
                    {
                        Builder = table.Builder,
                        Table = table,
                        Alias = null,
                        Name = FieldName
                    };
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The field '{0}' was not found in the table '{1}'", FieldName, table.Name));
                }

            }
            table.Builder.OrderByClause.Add(new OrderBy()
                {
                    Field = field,
                    Direction = Direction
                });
            return table;

        }

        #endregion

        #region SELECT list

        internal static void AllColumns(Table sql, MetadataTable mt)
        {
            foreach (MetadataColumn col in mt.Columns.Values)
            {
                Column(sql, col);
            }
        }

        internal static void Column(Table sql, MetadataColumn col, string Alias = null)
        {
            if (sql.FindField(col.Name) == null)
            {
                Field f = new Field()
                {
                    Name = col.Name,
                    Alias = Alias,
                    Table = sql,
                    Builder = sql.Builder
                };
                col.PopulateField<Field>(f);
                sql.FieldList.Add(f);
            }
        }


        public static Table AllColumns(this Table sql, bool UseWildcardCharacter = false)
        {
            MetadataDatabase mdb = sql.Builder.Metadata;
            if (!UseWildcardCharacter && mdb != null)
            {
                MetadataTable mt = mdb.FindTable(sql.FullName);
                if (mt == null)
                {
                    throw new ArgumentException(string.Format("The table {0} cannot be resolved with metadata. Must use wildcard", sql.FullName), "UseWildcardCharacter");
                }
                AllColumns(sql, mt);
                return sql;
            }
            else
            {
                sql.FieldList.Add(new Field()
                {
                    Name = "*",
                    Alias = null,
                    Table = sql,
                    Builder = sql.Builder
                });
            }
            return sql;
        }

        public static Table ConcatColumns(this Table sql, string Alias, string Separator, params string[] Columns)
        {
            string prefix = sql.Alias;
            string FieldName = Columns[0];
            for (int i = 1; i < Columns.Length; i++)
            {
                FieldName += string.Format(" + '{0}' + [{1}].{2}", Separator, prefix, Columns[i]);
            }
            sql.FieldList.Add(new Field()
            {
                Name = FieldName,
                Alias = Alias,
                Table = sql,
                Builder = sql.Builder
            });
            return sql;
        }


        public static Table Columns(this Table sql, params string[] Fields)
        {
            for (int i = 0; i < Fields.Length; i++)
            {
                if (sql.FindField(Fields[i]) == null)
                {
                    Column(sql, Fields[i], null);
                }
            }
            return sql;
        }
        public static Table Column(this Table sql, string Field, string Alias = null)
        {
            Field f = new Field()
            {
                Name = Field,
                Alias = Alias,
                Table = sql,
                Builder = sql.Builder
            };
            MetadataDatabase mdb = sql.Builder.Metadata;
            if (mdb != null)
            {
                MetadataTable mt = mdb.FindTable(sql.FullName);
                if (mt != null)
                {
                    MetadataColumn mc = null;
                    if (mt.Columns.TryGetValue(Field, out mc))
                    {
                        mc.PopulateField<Field>(f);
                    }
                }
            }
            sql.FieldList.Add(f);
            return sql;
        }

        public static Table Column<T>(this Table table, T Value, string Alias)
        {
            table.FieldList.Add(new ValueField<T>()
                {
                    Alias = Alias,
                    FieldValue = Value,
                    Table = table,
                    Builder = table.Builder
                });
            return table;
        }


        #endregion

        #region JOIN conditions

        public static Table SubSelect(this Table table, string TableName)
        {
            MetadataTable mt = table.Builder.Metadata.FindTable(table.FullName);
            if (mt == null)
            {
                throw new InvalidOperationException("Metadata for the table " + table.FullName + " could not be found");
            }
            MetadataTable mtTo = table.Builder.Metadata.FindTable(TableName);
            List<MetadataForeignKey> Fks = mtTo.ForeignKeys.Values.Where(x => x.ReferencedTable == mt.Name && x.ReferencedSchema == mt.Schema).ToList();
            if (Fks.Count != 1)
            {
                throw new InvalidOperationException(string.Format("Extended one relationship to the table {0}. Found {1}.", mtTo.Fullname, Fks.Count));
            }

            MetadataForeignKey FK = Fks.First();
            return table.SubSelect(mtTo.Name, mt.PrimaryKey.Columns.First().Name, FK.ColumnReferences.First().Name, null, mt.Schema, null);
        }

        public static Table SubSelect(this Table table, string TableName, string FromField, string ToField, string Alias = null, string Schema = null, string BuilderName = null)
        {
            string key = table.Alias + "." + FromField + ":" + (Alias ?? (Schema != null ? Schema + "." : "") + TableName) + "." + ToField;
            SqlBuilder b;
            if (table.Builder.SubQueries.TryGetValue(key, out b))
            {
                return b.BaseTable();
            }
            string tmp = System.IO.Path.GetRandomFileName().Replace(".", "");
            TempTable into = table.Builder.SelectIntoTable;
            if (into == null)
            {
                table.Into(tmp, true);
                into = table.Builder.SelectIntoTable;
            }
            if (table.FindField(FromField) == null && !table.FieldList.Any(x => x.Name == "*"))
            {
                table.Column(FromField);
            }
            into.OrderBy(FromField, OrderByDirections.Asc);

            tmp = System.IO.Path.GetRandomFileName().Replace(".", "");
            b = SqlBuilder.Select()
                .From(TableName, Alias, Schema)
                .Column(ToField)
                .Into(tmp, true)
                .WhereExists(table.Builder.SelectIntoTable)
                .And(into, ToField, SqlOperators.Equal, FromField)
                .Builder.SelectIntoTable
                    .OrderBy(FromField, OrderByDirections.Asc)
                .Builder;

            b.BuilderName = BuilderName;
            table.Builder.AddSubQuery(key, b);
            return b.BaseTable();

        }


        public static Join InnerJoin(this Table sql, string TableName, string Alias = null, string Schema = null)
        {
            return MakeJoin(Join.JoinTypes.Inner, sql, TableName, Alias, Schema);
        }
        public static Join InnerJoin(this JoinConditionGroup group, string TableName, string Alias = null, string Schema = null)
        {
            return group.Join.FromTable.InnerJoin(TableName, Alias, Schema);
        }
        public static Join LeftOuterJoin(this Table sql, string TableName, string Alias = null, string Schema = null)
        {
            return MakeJoin(Join.JoinTypes.LeftOuter, sql, TableName, Alias, Schema);
        }
        public static Join LeftOuterJoin(this JoinConditionGroup group, string TableName, string Alias = null, string Schema = null)
        {
            return group.Join.FromTable.LeftOuterJoin(TableName, Alias, Schema);
        }
        public static Join RightOuterJoin(this Table sql, string TableName, string Alias = null, string Schema = null)
        {
            return MakeJoin(Join.JoinTypes.RightOuter, sql, TableName, Alias, Schema);
        }
        public static Join RightOuterJoin(this JoinConditionGroup group, string TableName, string Alias = null, string Schema = null)
        {
            return group.Join.FromTable.RightOuterJoin(TableName);
        }
        public static Join CrossJoin(this Table sql, string TableName)
        {
            return MakeJoin(Join.JoinTypes.Cross, sql, TableName);
        }
        public static Join CrossJoin(this JoinConditionGroup group, string TableName, string Alias = null, string Schema = null)
        {
            return group.Join.FromTable.CrossJoin(TableName);
        }

        internal static Join MakeJoin(Join.JoinTypes JoinType, Table FromTable, string ToTable, string Alias = null, string Schema = null)
        {
            Table right = FromTable.Builder.Tables.FirstOrDefault(x => x.Name.Equals((Alias ?? ToTable), StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(Alias, StringComparison.InvariantCultureIgnoreCase)));
            if (FromTable.Builder.JoinConditions.Select(x => x.ToTable).Any(x => x.Equals(right)))
            {
                right = null;
            }


            if (right == null)
            {
                right = FromTable.Builder.From(ToTable, Alias, Schema);
            }

            Join join = new Join()
            {
                JoinType = JoinType,
                FromTable = FromTable,
                ToTable = right,
                Builder = FromTable.Builder
            };
            FromTable.Builder.JoinConditions.Add(join);
            return join;
        }

        public static ExistsConditionGroup And(this ExistsConditionGroup group, string FromField, SqlOperators Operator, string ToField)
        {
            return ExistsConditionInternal(group, FromField, Operator, ToField, group.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None);
        }

        public static ExistsConditionGroup And(this ExistsConditionGroup group, Table InTable, string InField, SqlOperators Operator, string ToField)
        {
            return ExistsConditionInternal(group, InTable, InField, Operator, ToField, group.Conditions.Count == 0 ? BoolOperators.None : BoolOperators.And);
        }

        public static ExistsConditionGroup And<T>(this ExistsConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T Value)
        {
            return WhereExistsInternal<T>(group, TableName, FieldName, Operator, BoolOperators.And, Value);
        }
        public static ExistsConditionGroup Or<T>(this ExistsConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T Value)
        {
            return WhereExistsInternal<T>(group, TableName, FieldName, Operator, BoolOperators.Or, Value);
        }

        private static ExistsConditionGroup WhereExistsInternal<T>(ExistsConditionGroup group, string TableName, string FieldName, SqlOperators Operator, BoolOperators LinkType, T value)
        {
            Table t = null;
            if (group.InTable.Name.Equals(TableName, StringComparison.InvariantCultureIgnoreCase) || (group.InTable.Alias != null && group.InTable.Alias.Equals(TableName, StringComparison.InvariantCultureIgnoreCase)))
            {
                t = group.InTable;
            }
            if (t == null)
                t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(TableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableName, StringComparison.InvariantCultureIgnoreCase)));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The EXISTS table '{0}' does not exist", TableName));
            }

            ValueField<T> fv = new ValueField<T>()
            {
                Table = t,
                Name = FieldName,
                Builder = group.Builder,
                FieldValue = value

            };
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? LinkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                leftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }

        public static ExistsConditionGroup Or(this ExistsConditionGroup group, string FromField, SqlOperators Operator, string ToField)
        {
            return ExistsConditionInternal(group, FromField, Operator, ToField, group.Conditions.Count > 0 ? BoolOperators.Or : BoolOperators.None);
        }

        public static ExistsConditionGroup AndGroup(this ExistsConditionGroup group)
        {
            return ExistsGroupInternal(group, BoolOperators.And);
        }

        private static ExistsConditionGroup ExistsGroupInternal(ExistsConditionGroup group, BoolOperators ConditionLink)
        {
            ExistsConditionGroup g = new ExistsConditionGroup()
            {
                ConditionLink = ConditionLink,
                Parent = group.Parent,
                Builder = group.Builder,
                FromTable = group.FromTable,
                InTable = group.InTable,
                Negated = group.Negated
            };
            group.SubConditions.Add(g);
            return g;
        }
        public static ExistsConditionGroup OrGroup(this ExistsConditionGroup group)
        {
            return ExistsGroupInternal(group, BoolOperators.Or);
        }

        private static ExistsConditionGroup ExistsConditionInternal(ExistsConditionGroup group, Table InTable, string InField, SqlOperators Operator, string ToField, BoolOperators LinkType)
        {
            // Field lf = FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(FromField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(FromField, StringComparison.InvariantCultureIgnoreCase)));
            Field lf = InTable.FindField(InField);
            if (lf == null || group.FromTable == null)
            {
                lf = new Field()
                {
                    Table = InTable,
                    Builder = group.Builder,
                    Name = InField,
                    Alias = null
                };
            }
            Table t = group.Builder.FindTable(group.FromTable);
            Field rf = null;
            if (t != null)
            {
                rf = t.FindField(ToField);
            }
            // Field rf = group.InTable.FieldList.FirstOrDefault(x => x.Name.Equals(ToField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(ToField, StringComparison.InvariantCultureIgnoreCase)));
            // Field rf = group.InTable.FieldList.FirstOrDefault(x => x.Name.Equals(ToField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(ToField, StringComparison.InvariantCultureIgnoreCase)));
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = t,
                    Builder = group.Builder,
                    Name = ToField,
                    Alias = null
                };
            }

            FieldCondition condition = new FieldCondition()
            {
                Builder = group.Builder,
                LeftTable = InTable,
                leftField = lf,
                RightTable = t,
                RightField = rf,
                Condition = Operator,
                ParentGroup = group,
                ConditionLink = LinkType
            };
            group.Conditions.Add(condition);
            return group;
        }

        private static ExistsConditionGroup ExistsConditionInternal(ExistsConditionGroup group, string FromField, SqlOperators Operator, string ToField, BoolOperators LinkType)
        {
            Table FromTable = null;
            if (group.FromTable == null)
            {
                FromTable = group.Builder.Tables.First();
            }
            else
            {
                FromTable = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(group.FromTable) || (x.Alias != null && x.Alias.Equals(group.FromTable)));
            }
            return ExistsConditionInternal(group, group.InTable, FromField, Operator, ToField, LinkType);


        }



        public static JoinConditionGroup And(this JoinConditionGroup group, string FromField, SqlOperators Operator, string ToField)
        {
            return OnInternal(group, FromField, Operator, ToField, BoolOperators.And);
        }
        public static JoinConditionGroup Or(this JoinConditionGroup group, string FromField, SqlOperators Operator, string ToField)
        {
            return OnInternal(group, FromField, Operator, ToField, BoolOperators.Or);
        }

        public static JoinConditionGroup AndGroup(this JoinConditionGroup group)
        {
            JoinConditionGroup g = new JoinConditionGroup()
            {
                ConditionLink = BoolOperators.And,
                Parent = group.Parent,
                Join = group.Join
            };
            group.SubConditions.Add(g);
            return g;
        }

        public static JoinConditionGroup OrGroup(this JoinConditionGroup group)
        {
            JoinConditionGroup g = new JoinConditionGroup()
            {
                ConditionLink = BoolOperators.Or,
                Parent = group.Parent,
                Join = group.Join
            };
            group.SubConditions.Add(g);
            return g;
        }

        public static JoinConditionGroup On(this Join join, string FromField, SqlOperators Operator, string ToField)
        {
            return OnInternal(join.Conditions, FromField, Operator, ToField, BoolOperators.None);
        }

        public static WhereConditionGroup Where<T>(this Table table, string TableName, string FieldName, SqlOperators Operator, T value)
        {
            return (WhereConditionGroup)WhereInternal<T>(table.Builder.WhereConditions, TableName, FieldName, Operator, value);
        }

        public static ExistsConditionGroup WhereExists(this Table table, string InTable)
        {
            return ExistsInternal(table.Builder.WhereConditions, InTable, table.Alias, table.Builder.WhereConditions.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, false);
        }

        public static ExistsConditionGroup WhereExists(this Table table, Table InTable)
        {
            return ExistsInternal(table.Builder.WhereConditions, InTable.Alias, table.Alias, table.Builder.WhereConditions.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, false);
        }

        public static ExistsConditionGroup WhereNotExists(this Table table, string InTable)
        {
            return ExistsInternal(table.Builder.WhereConditions, InTable, table.Alias, table.Builder.WhereConditions.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, true);
        }

        public static ExistsConditionGroup AndExists(this WhereConditionGroup group, string InTable, string FromTable = null)
        {
            return ExistsInternal(group, InTable, FromTable, group.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, false);
        }
        public static ExistsConditionGroup AndNotExists(this WhereConditionGroup group, string InTable, string Fromtable = null)
        {
            return ExistsInternal(group, InTable, Fromtable, group.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, true);
        }

        public static WhereConditionGroup EndExists(this ExistsConditionGroup group)
        {
            return (WhereConditionGroup)group.Parent;
        }

        private static ExistsConditionGroup ExistsInternal(WhereConditionGroup group, string InTable, string FromTable, BoolOperators ConditionLink, bool Negated)
        {
            Table t = new Table()
            {
                Name = InTable,
                Builder = group.Builder,
                Alias = null
            };
            ExistsConditionGroup exists = new ExistsConditionGroup()
            {
                Builder = group.Builder,
                ConditionLink = ConditionLink,
                Negated = Negated,
                Parent = group,
                InTable = t,
                FromTable = FromTable
            };
            group.SubConditions.Add(exists);

            return exists;
        }

        public static JoinConditionGroup And(this JoinConditionGroup group, string FieldName, SqlOperators Operator, object Value, string TableName = null)
        {
            if (string.IsNullOrEmpty(TableName))
            {
                TableName = group.Join.ToTable.Alias;
            }
            return (JoinConditionGroup)WhereInternalAll((ConditionGroup)group, TableName, FieldName, Operator, Value, BoolOperators.And);
        }

        public static JoinConditionGroup And<T>(this JoinConditionGroup group, string FieldName, SqlOperators Operator, T Value, string TableName = null)
        {
            if (string.IsNullOrEmpty(TableName))
            {
                TableName = group.Join.ToTable.Alias;
            }
            return (JoinConditionGroup)WhereInternalAll<T>((ConditionGroup)group, TableName, FieldName, Operator, Value, BoolOperators.And);
        }
        public static JoinConditionGroup Or<T>(this JoinConditionGroup group, string FieldName, SqlOperators Operator, T Value, string TableName = null)
        {
            if (string.IsNullOrEmpty(TableName))
            {
                TableName = group.Join.ToTable.Alias;
            }
            return (JoinConditionGroup)WhereInternalAll<T>((ConditionGroup)group, TableName, FieldName, Operator, Value, BoolOperators.Or);
        }


        public static WhereConditionGroup And<T>(this ConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T Value)
        {
            return (WhereConditionGroup)WhereInternal<T>(group, TableName, FieldName, Operator, Value, BoolOperators.And);
        }

        public static WhereConditionGroup Or<T>(this ConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T Value)
        {
            return (WhereConditionGroup)WhereInternal<T>(group, TableName, FieldName, Operator, Value, BoolOperators.Or);
        }

        public static WhereConditionGroup AndGroup(this WhereConditionGroup group)
        {
            WhereConditionGroup g = new WhereConditionGroup()
            {
                ConditionLink = BoolOperators.And,
                Parent = group,
                Builder = group.Builder
            };
            group.SubConditions.Add(g);
            return g;
        }

        public static WhereConditionGroup OrGroup(this WhereConditionGroup group)
        {
            WhereConditionGroup g = new WhereConditionGroup()
            {
                ConditionLink = BoolOperators.Or,
                Parent = group,
                Builder = group.Builder
            };
            group.SubConditions.Add(g);
            return g;
        }

        private static ConditionGroup WhereInternalAll<T>(ConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T value, BoolOperators LinkType = BoolOperators.None)
        {

            Table t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(TableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableName, StringComparison.InvariantCultureIgnoreCase)));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The WHERE condition table '{0}' does not exist", TableName));
            }
            ValueField<T> fv = new ValueField<T>()
            {
                Table = t,

                Name = FieldName,
                Builder = group.Builder,
                FieldValue = value

            };
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? LinkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                leftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }

        internal static ConditionGroup WhereInternalAll(ConditionGroup group, string TableName, string FieldName, SqlOperators Operator, object value, BoolOperators LinkType = BoolOperators.None)
        {

            Table t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(TableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableName, StringComparison.InvariantCultureIgnoreCase)));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The WHERE condition table '{0}' does not exist", TableName));
            }
            ValueField fv = new ValueField()
            {
                Table = t,
                Name = FieldName,
                Builder = group.Builder,
                Value = value
            };
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? LinkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                leftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }



        private static ConditionGroup WhereInternal<T>(ConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T value, BoolOperators LinkType = BoolOperators.None)
        {

            Table t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(TableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableName, StringComparison.InvariantCultureIgnoreCase)) || x.FullName.Equals(TableName, StringComparison.InvariantCultureIgnoreCase));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The WHERE condition table '{0}' does not exist", TableName));
            }
            Field f = t.FindField(FieldName);
            ValueField<T> fv = new ValueField<T>()
            {
                Table = t,
                Name = f != null ? f.Name : FieldName,
                Builder = group.Builder,
                FieldValue = value

            };
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? LinkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                leftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }

        internal static JoinConditionGroup OnInternal(JoinConditionGroup group, Field FromField, SqlOperators Operator, Field ToField, BoolOperators LinkType = BoolOperators.None)
        {
            Join join = group.Join;
            FieldCondition condition = new FieldCondition()
            {
                Builder = join.Builder,
                LeftTable = join.FromTable,
                leftField = FromField,
                RightTable = join.ToTable,
                RightField = ToField,
                Condition = Operator,
                ParentGroup = group,
                ConditionLink = LinkType
            };
            group.Conditions.Add(condition);
            return group;
        }

        internal static Field PopulateField(this Field f)
        {
            f.TryPopulateField();
            return f;
        }
        internal static bool TryPopulateField(this Field f)
        {
            if (f.Table == null)
            {
                return false;
            }
            MetadataDatabase mdb = f.Table.Builder.Metadata;
            if (mdb == null)
            {
                return false;
            }
            MetadataTable mt = mdb.FindTable(f.Table.FullName);
            if (mt == null)
            {
                return false;
            }
            MetadataColumn mc = null;
            if (!mt.Columns.TryGetValue(f.Name, out mc))
            {
                return false;
            }
            mc.PopulateField<Field>(f);
            return true;
        }

        internal static JoinConditionGroup OnInternal(JoinConditionGroup group, string FromField, SqlOperators Operator, string ToField, BoolOperators LinkType = BoolOperators.None)
        {
            Join join = group.Join;
            Field lf = join.FromTable.FindField(FromField);
            if (lf == null)
            {
                lf = new Field()
                {
                    Table = join.FromTable,
                    Builder = join.Builder,
                    Name = FromField,
                    Alias = null
                };
                lf.TryPopulateField();
            }
            Field rf = join.ToTable.FindField(ToField);
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = join.ToTable,
                    Builder = join.Builder,
                    Name = ToField,
                    Alias = null
                };
                rf.TryPopulateField();
            }
            return OnInternal(group, lf, Operator, rf, LinkType);
        }

        #endregion

        #region If Statements

        public static SqlBuilder Begin(this ConditionGroup group, SqlBuilder.StatementTypes StatementType)
        {
            if (group.Builder is IfStatement)
            {
                return Begin((IfStatement)group.Builder, StatementType);
            }
            else
            {
                throw new ArgumentException("The Begin() Extension can only be used for If statements", "End");
            }
        }

        public static SqlBuilder Begin(this IfStatement builder, SqlBuilder.StatementTypes StatementType)
        {
            builder.StatementBody.StatementType = StatementType;
            return builder.StatementBody;
        }
        public static IfStatement End(this SqlBuilder builder)
        {
            SqlBuilder sb = builder.ParentBuilder;
            while (sb != null && (!(sb is IfStatement) || (sb as IfStatement).BranchStatement != BranchStatements.If))
            {
                sb = sb.ParentBuilder;
            }
            if (sb != null)
            {
                return (IfStatement)sb;
            }
            else
            {
                throw new ArgumentException("The End() Extension can only be used for If statements", "End");
            }
        }

        public static IfStatement Else(this IfStatement builder)
        {
            return BranchInternal(builder, BranchStatements.Else);
        }

        public static IfStatement ElseIf(this IfStatement builder)
        {
            return BranchInternal(builder, BranchStatements.Else);
        }

        private static IfStatement BranchInternal(IfStatement builder, BranchStatements Branch)
        {
            IfStatement statement = new IfStatement()
            {
                BranchStatement = Branch,
                ParentBuilder = builder
            };
            builder.ElseIfStatements.Add(statement);
            return statement;
        }




        #endregion

    }
}
