using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Caching;
using System.Runtime.CompilerServices;
using System.Text;
using TinySql.Metadata;

namespace TinySql
{
    public class SqlBuilder
    {
        public static SqlBuilder Select(int? Top = null, bool Distinct = false)
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Select,
                Top = Top,
                Distinct = Distinct
            };
        }
        public static StoredProcedure StoredProcedure(string Name, string Schema = null)
        {

            SqlBuilder builder = new SqlBuilder()
            {
                StatementType = StatementTypes.Procedure
            };
            builder.Procedure = new StoredProcedure()
            {
                Builder = builder,
                Name = Name,
                Schema = Schema
            };
            return builder.Procedure;

        }
        public static SqlBuilder Insert()
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Insert
            };
        }

        public static SqlBuilder Update()
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Update
            };
        }

        public static SqlBuilder Delete()
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Delete
            };
        }

        public static IfStatement If()
        {
            return new IfStatement()
            {
                BranchStatement = BranchStatements.If,
                StatementType = StatementTypes.If
            };
        }

        public StoredProcedure Procedure { get; set; }

        private static string _DefaultConnection = null;

        public static string DefaultConnection
        {
            get { return _DefaultConnection; }
            set { _DefaultConnection = value; }
        }

        private string _ConnectionString = null;

        public string ConnectionString
        {
            get { return _ConnectionString ?? DefaultConnection; }
            set { _ConnectionString = value; }
        }

        public string CustomSql { get; set; } = null;

        public object[] Format;

        private MetadataDatabase _Metadata = null;
        public MetadataDatabase Metadata
        {
            get
            {
                MetadataDatabase mdb = _Metadata ?? this.Builder()._Metadata;
                return mdb ?? DefaultMetadata;
            }
            set { _Metadata = value; }
        }

        public static MetadataDatabase DefaultMetadata { get; set; }


        public TempTable SelectIntoTable { get; set; }

        private ConcurrentDictionary<string, SqlBuilder> _SubQueries = new ConcurrentDictionary<string, SqlBuilder>();
        public ConcurrentDictionary<string, SqlBuilder> SubQueries
        {
            get { return _SubQueries; }
            set { _SubQueries = value; }
        }
        public SqlBuilder AddSubQuery(string Name, SqlBuilder Builder)
        {
            if (Builder.StatementType != StatementTypes.Insert && Builder.StatementType != StatementTypes.Update)
            {
                // Only set the parent builder for statements that share parameter declarations
                Builder.ParentBuilder = this;
            }
            return _SubQueries.AddOrUpdate(Name, Builder, (k, v) => { return Builder; });

        }

        public SqlBuilder ParentBuilder { get; set; }

        private ConcurrentDictionary<string, string> Declarations = new ConcurrentDictionary<string, string>();

        public SqlBuilder TopBuilder
        {
            get
            {
                if (this.ParentBuilder == null)
                {
                    return this;
                }
                SqlBuilder sb = this.ParentBuilder;
                while (sb.ParentBuilder != null)
                {
                    sb = sb.ParentBuilder;
                }
                return sb;
            }
        }

        internal bool AddDeclaration(string DeclarationName, string Body)
        {
            return Declarations.TryAdd(DeclarationName, Body);
        }

        private List<OrderBy> _OrderByClause = new List<OrderBy>();

        public List<OrderBy> OrderByClause
        {
            get { return _OrderByClause; }
            set { _OrderByClause = value; }
        }

        public string BuilderName { get; set; }

        public SqlBuilder()
        {
            Initialize(null, null);
        }
        public SqlBuilder(string ConnectionString)
        {
            Initialize(ConnectionString, null);
        }

        public SqlBuilder(string ConnectionString, CultureInfo Culture)
        {
            Initialize(ConnectionString, Culture);
        }


        private void Initialize(string connectionString, CultureInfo culture)
        {
            WhereConditions.Builder = this;
            Culture = culture;
            ConnectionString = connectionString;
        }

        private static CacheItemPolicy _CachePolicy = new CacheItemPolicy() { AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration, SlidingExpiration = MemoryCache.NoSlidingExpiration };
        public static CacheItemPolicy CachePolicy
        {
            get { return SqlBuilder._CachePolicy; }
            set { SqlBuilder._CachePolicy = value; }
        }


        public static bool CacheSqlBuilder(string Key, SqlBuilder Builder, CacheItemPolicy Policy = null)
        {
            CacheItem item = MemoryCache.Default.AddOrGetExisting(new CacheItem(Key, Builder), (Policy ?? CachePolicy));
            return item.Value == null;
        }
        public static SqlBuilder CacheSqlBuilder(string Key)
        {
            CacheItem item = MemoryCache.Default.GetCacheItem(Key);
            return item != null ? (SqlBuilder)item.Value : null;
        }



        public string ToSql(params object[] Format)
        {
            if (!string.IsNullOrEmpty(this.CustomSql))
            {
                return this.CustomSql;
            }
            if (Format == null || Format.Length == 0)
            {
                return ToSql();
            }

            return string.Format(ToSql(), Format);
        }

        public virtual string ToSql()
        {
            if (!string.IsNullOrEmpty(this.CustomSql))
            {
                return this.CustomSql;
            }
            StringBuilder sb = new StringBuilder();
            string sql = "";

            switch (StatementType)
            {
                case StatementTypes.Select:
                    sql = SelectSql();
                    break;
                case StatementTypes.Insert:
                    sql = InsertSql();
                    break;
                case StatementTypes.Update:
                    sql = UpdateSql();
                    break;
                case StatementTypes.Delete:
                    sql = DeleteSql();
                    break;
                case StatementTypes.Procedure:
                    sql = this.Procedure.ToSql();
                    break;
                default:
                    break;
            }

            // Post SQL
            if (this.ParentBuilder == null)
            {
                // Top level, so write parameter declarations at the top
                foreach (string par in Declarations.Values)
                {
                    sb.AppendLine(par);
                }
            }
            sb.AppendLine(sql);

            return sb.ToString();
        }

        private string DeleteSql()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("DELETE  {0}\r\n", BaseTable().Alias);
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable().ReferenceName);
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            string where = WhereConditions.ToSql();
            if (!string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            return sb.ToString();
        }
        private string InsertSql()
        {
            StringBuilder sb = new StringBuilder();
            InsertIntoTable BaseTable = this.BaseTable() as InsertIntoTable;
            string set = "";
            string outputSelect = "";
            SqlBuilder tb = this.TopBuilder;
            foreach (ParameterField field in BaseTable.FieldList)
            {
                tb.AddDeclaration(field.ParameterName, field.DeclareParameter());
                set += field.SetParameter() + "\r\n";
            }

            if (BaseTable.Output != null && BaseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                tb.AddDeclaration(BaseTable.Output.ParameterName, BaseTable.Output.DeclareParameter());
                outputSelect += BaseTable.Output.SetParameter() + "\r\n";

            }

            sb.AppendLine(set);
            sb.AppendFormat(" INSERT  INTO {0}({1})\r\n", BaseTable.Alias, BaseTable.ToSql());
            if (BaseTable.Output != null && BaseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", BaseTable.Output.ToSql());
            }
            sb.AppendFormat("VALUES({0})\r\n", BaseTable.FieldParameters());
            if (!string.IsNullOrEmpty(outputSelect))
            {
                sb.AppendLine(outputSelect);
            }
            return sb.ToString();

        }

        public void CleanSelectList(bool RemoveDublicateFields = false)
        {
            List<string> clean = new List<string>();
            int idx = 0;
            foreach (Field f in Tables.SelectMany(x => x.FieldList))
            {
                string s = f.Alias != null ? f.Alias : f.Name;
                if (!clean.Contains(s))
                {
                    clean.Add(s);
                }
                else
                {
                    if (RemoveDublicateFields)
                    {
                        f.Table.FieldList.Remove(f);
                    }
                    else
                    {
                        f.Alias = f.Table.Name + "_" + f.Name;
                        clean.Add(f.Alias);
                        idx++;
                    }
                    
                }
            }
        }

        private string SelectSql()
        {
            StringBuilder sb = new StringBuilder();

            // Clean select list
            CleanSelectList(false);


            string selectList = BaseTable().ToSql();
            foreach (Table t in Tables.Skip(1))
            {
                string fields = t.ToSql();
                selectList += !string.IsNullOrEmpty(fields) ? ", " + fields : "";
            }
            //
            // SELECT
            //
            sb.AppendFormat("SELECT {1} {2}  {0}\r\n", selectList, Distinct ? "DISTINCT" : "", Top.HasValue ? "TOP " + Top.Value.ToString() : "");
            //
            // INTO
            //
            if (SelectIntoTable != null)
            {
                sb.AppendFormat("  INTO  {0}\r\n", SelectIntoTable.ReferenceName);
            }
            //
            // FROM
            //
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable().ReferenceName);
            //
            // JOINS
            //
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            //
            // WHERE
            //
            string where = WhereConditions.ToSql();
            if (where != "()" && !string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            //
            // TODO: Group by
            //

            // ORDER BY
            if (OrderByClause.Count > 0)
            {
                sb.AppendFormat(" ORDER  BY {0}", OrderByClause.First().ToSql());
                foreach (OrderBy order in OrderByClause.Skip(1))
                {
                    sb.AppendFormat(", {0}", order.ToSql());
                }
                sb.Append("\r\n");
            }


            //
            // Post SQL stuff
            // 
            if (this.SelectIntoTable != null && this.SelectIntoTable.OutputTable)
            {
                sb.AppendFormat("SELECT  {0} FROM {1}\r\n", SelectIntoTable.ToSql(), SelectIntoTable.ReferenceName);
                if (SelectIntoTable.OrderByClause.Count > 0)
                {
                    sb.AppendFormat(" ORDER  BY {0}", SelectIntoTable.OrderByClause.First().ToSql());
                    foreach (OrderBy order in SelectIntoTable.OrderByClause.Skip(1))
                    {
                        sb.AppendFormat(", {0}", order.ToSql());
                    }
                    sb.Append("\r\n");
                }
            }

            //
            // Sub Queries
            //
            if (_SubQueries.Count > 0)
            {
                foreach (SqlBuilder sub in _SubQueries.Values)
                {
                    sb.AppendFormat("\r\n-- Sub Query\r\n{0}\r\n", sub.ToSql(Format));
                }
            }

            return sb.ToString();
        }

        private string UpdateSql()
        {
            StringBuilder sb = new StringBuilder();
            UpdateTable BaseTable = this.BaseTable() as UpdateTable;
            string declare = "";
            string set = "";
            string outputSelect = "";
            SqlBuilder tb = this.TopBuilder;

            foreach (ParameterField field in BaseTable.FieldList.OfType<ParameterField>())
            {
                tb.AddDeclaration(field.ParameterName, field.DeclareParameter());
                set += field.SetParameter() + "\r\n";
            }
            if (BaseTable.Output != null && BaseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                tb.AddDeclaration(BaseTable.Output.ParameterName, BaseTable.Output.DeclareParameter());
                outputSelect += BaseTable.Output.SetParameter() + "\r\n";

            }

            sb.AppendLine(declare);
            sb.AppendLine(set);

            sb.AppendFormat("UPDATE  {0}\r\n", BaseTable.Name);
            sb.AppendFormat("   SET  {0}\r\n", BaseTable.ToSql());
            if (BaseTable.Output != null && BaseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", BaseTable.Output.ToSql());
            }
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable.ReferenceName);
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            string where = WhereConditions.ToSql();
            if (!string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            if (!string.IsNullOrEmpty(outputSelect))
            {
                sb.AppendLine(outputSelect);
            }
            return sb.ToString();
        }

        public enum StatementTypes
        {
            Select = 1,
            Insert = 2,
            Update = 3,
            Delete = 4,
            If = 5,
            Procedure = 6
        }

        private System.Globalization.CultureInfo _Culture = null;

        public System.Globalization.CultureInfo Culture
        {
            get { return _Culture ?? DefaultCulture; }
            set { _Culture = value; }
        }
        private static CultureInfo _DefaultCulture = System.Globalization.CultureInfo.GetCultureInfo(1033);
        public static CultureInfo DefaultCulture
        {
            get
            {
                return _DefaultCulture;
            }
            set
            {
                _DefaultCulture = value;
            }
        }


        public List<Table> Tables = new List<Table>();
        public WhereConditionGroup WhereConditions = new WhereConditionGroup();

        public virtual Table BaseTable()
        {
            return Tables.Count > 0 ? Tables[0] : null;
        }


        public List<Join> JoinConditions = new List<Join>();
        public StatementTypes StatementType
        {
            get;
            set;
        }

        public int? Top
        {
            get;
            set;
        }
        public bool Distinct
        {
            get;
            set;
        }
    }
}