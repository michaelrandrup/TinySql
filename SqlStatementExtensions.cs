using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace TinySql
{
    public static class SqlStatementExtensions
    {

        #region Update statement

        public static UpdateTable Table(this SqlBuilder builder, string TableName, string Schema = null)
        {
            if (builder.Tables.Count > 0 && builder.Tables[0] is UpdateTable)
            {
                return builder.Tables[0] as UpdateTable;
            }
            else
            {
                UpdateTable t = new UpdateTable(builder, TableName, Schema);
                builder.Tables.Add(t);
                return t;
            }
        }

        public static UpdateTable Set<T>(this UpdateTable table, string FieldName, SqlDbType DataType, T Value, int MaxLength = -1, int Scale = -1)
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

        public static UpdateTable OutputColumn(this UpdateTable table, string Field, string Alias = null)
        {
            if (table.Output == null)
            {
                table.Output = new Table(table.Builder, "inserted", null, null);
            }
            table.Output.Column(Field, Alias);
            return table;
        }

        #endregion

        #region Insert Statement

        public static InsertIntoTable Into(this SqlBuilder builder, string TableName)
        {
            if (builder.Tables.Count > 0 && builder.Tables[0] is InsertIntoTable)
            {
                return builder.Tables[0] as InsertIntoTable;
            }
            else
            {
                InsertIntoTable t = new InsertIntoTable(builder, TableName);
                builder.Tables.Add(t);
                return t;
            }
        }

        public static InsertIntoTable Value<T>(this InsertIntoTable table, string FieldName, SqlDbType DataType, T Value, int MaxLength = -1, int Scale = -1)
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

        public static InsertIntoTable ColumnOutput(this InsertIntoTable table, string Field, string Alias = null)
        {
            if (table.Output == null)
            {
                table.Output = new Table(table.Builder, "inserted", null, null);
            }
            table.Output.Column(Field, Alias);
            return table;
        }

        #endregion

        #region Table

        public static Table From(this Table sql, string TableName, string Alias = null)
        {
            return sql.Builder.From(TableName, Alias);
        }
        public static Table From(this SqlBuilder sql, string TableName, string Alias = null, string Schema = null)
        {
            Table table = sql.Tables.FirstOrDefault(x => x.Name.Equals((Alias ?? TableName), StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(TableName, StringComparison.InvariantCultureIgnoreCase)));
            if (table != null)
            {
                return table;
            }
            table = new Table(sql, TableName, string.IsNullOrEmpty(Alias) ? "t" + sql.Tables.Count.ToString() : Alias,Schema);
            sql.Tables.Add(table);
            return table;
        }
        public static Table ToTable(this JoinConditionGroup group)
        {
            return group.Join.ToTable;
        }

        #endregion

        #region SELECT list
        public static Table AllColumns(this Table sql)
        {
            sql.FieldList.Add(new Field()
            {
                Name = "*",
                Alias = null,
                Table = sql,
                Builder = sql.Builder
            });
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
                sql.FieldList.Add(new Field()
                {
                    Name = Fields[i],
                    Alias = null,
                    Table = sql,
                    Builder = sql.Builder
                });
            }
            return sql;
        }
        public static Table Column(this Table sql, string Field, string Alias = null)
        {
            sql.FieldList.Add(new Field()
            {
                Name = Field,
                Alias = Alias,
                Table = sql,
                Builder = sql.Builder
            });
            return sql;
        }
        #endregion

        #region JOIN conditions

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
            return group.Join.FromTable.LeftOuterJoin(TableName,Alias,Schema);
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

        private static Join MakeJoin(Join.JoinTypes JoinType, Table FromTable, string ToTable, string Alias = null, string Schema = null)
        {
            Table right = FromTable.Builder.Tables.FirstOrDefault(x => x.Name.Equals((Alias ?? ToTable), StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(Alias, StringComparison.InvariantCultureIgnoreCase)));
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


            Field lf = FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(FromField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(FromField, StringComparison.InvariantCultureIgnoreCase)));
            if (lf == null || group.FromTable == null)
            {
                lf = new Field()
                {
                    Table = FromTable,
                    Builder = group.Builder,
                    Name = FromField,
                    Alias = null
                };
            }
            Field rf = group.InTable.FieldList.FirstOrDefault(x => x.Name.Equals(ToField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(ToField, StringComparison.InvariantCultureIgnoreCase)));
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = group.InTable,
                    Builder = group.Builder,
                    Name = ToField,
                    Alias = null
                };
            }

            FieldCondition condition = new FieldCondition()
            {
                Builder = group.Builder,
                LeftTable = FromTable,
                leftField = lf,
                RightTable = group.InTable,
                RightField = rf,
                Condition = Operator,
                ParentGroup = group,
                ConditionLink = LinkType
            };
            group.Conditions.Add(condition);
            return group;
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
            return group;
        }



        private static ConditionGroup WhereInternal<T>(ConditionGroup group, string TableName, string FieldName, SqlOperators Operator, T value, BoolOperators LinkType = BoolOperators.None)
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
            return group;
        }


        private static JoinConditionGroup OnInternal(JoinConditionGroup group, string FromField, SqlOperators Operator, string ToField, BoolOperators LinkType = BoolOperators.None)
        {
            Join join = group.Join;
            Field lf = join.FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(FromField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(FromField, StringComparison.InvariantCultureIgnoreCase)));
            if (lf == null)
            {
                lf = new Field()
                {
                    Table = join.FromTable,
                    Builder = join.Builder,
                    Name = FromField,
                    Alias = null
                };
            }
            Field rf = join.ToTable.FieldList.FirstOrDefault(x => x.Name.Equals(ToField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(ToField, StringComparison.InvariantCultureIgnoreCase)));
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = join.ToTable,
                    Builder = join.Builder,
                    Name = ToField,
                    Alias = null
                };
            }

            FieldCondition condition = new FieldCondition()
            {
                Builder = join.Builder,
                LeftTable = join.FromTable,
                leftField = lf,
                RightTable = join.ToTable,
                RightField = rf,
                Condition = Operator,
                ParentGroup = group,
                ConditionLink = LinkType
            };
            group.Conditions.Add(condition);
            return group;

        }

        #endregion



    }
}
