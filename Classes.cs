﻿using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Transactions;
using System.Xml;
using TinySql.Metadata;

namespace TinySql
{



    public class ResultTable : IList<RowData>
    {
        private List<RowData> _Results = new List<RowData>();

        public ResultTable() { }
        public ResultTable(DataTable dt, DateHandlingEnum? DateHandling = null)
        {
            this.DateHandling = DateHandling;
            Initialize(dt);
        }

        public ResultTable(SqlBuilder builder, int TimeoutSeconds = 60, bool WithMetadata = true, DateHandlingEnum? DateHandling = null, string UseHiearchyField = null, params object[] Format)
        {
            DataSet ds = builder.DataSet(builder.ConnectionString, TimeoutSeconds, Format);
            Table bt = builder.BaseTable();
            if (bt != null)
            {
                this.Metadata = GetMetadataTable(builder.Metadata, bt.FullName);
            }

            this.WithMetadata = WithMetadata;
            this.DateHandling = DateHandling;

            Initialize(ds.Tables[0]);
            ResultTable Current = this;
            int CurrentTable = 0;
            if (builder.SubQueries.Count > 0 && this.Metadata == null)
            {
                throw new ArgumentException("The query contains sub-queries but no metadata has been specified. Use metadata to populate the sub-queries", "WithMetadata");
            }
            foreach (var kv in builder.SubQueries)
            {
                CurrentTable++;
                DataTable dt = ds.Tables[CurrentTable];
                List<DataColumn> pk = new List<DataColumn>();
                //foreach (MetadataColumn Column in this.Metadata.PrimaryKey.Columns)
                //{
                //    DataColumn col = dt.Columns[Column.Name];
                //    pk.Add(dt.Columns[Column.Name]);
                //}
                //dt.PrimaryKey = pk.ToArray();
                foreach (RowData rd in this)
                {
                    // DataView dv = new DataView(dt);
                    DataView dv = dt.DefaultView;
                    List<object> _pk = new List<object>();
                    string Sort = "";
                    foreach (MetadataColumn Column in this.Metadata.PrimaryKey.Columns)
                    {
                        _pk.Add(rd.Column(Column.Name));
                        Sort += (!string.IsNullOrEmpty(Sort) ? ", " : "") + Column.Name + " ASC";
                    }
                    if (!string.IsNullOrEmpty(UseHiearchyField))
                    {
                        Sort = UseHiearchyField + " ASC";
                    }
                    dv.Sort = Sort;
                    DataRowView[] filteredRows = dv.FindRows(_pk.ToArray());
                    SubTable(rd, kv.Value, filteredRows, ds, CurrentTable, this.Metadata.Name, this.WithMetadata, UseHiearchyField);
                }

            }
        }

        private MetadataTable GetMetadataTable(MetadataDatabase mdb, string TableName)
        {
            if (mdb != null && !string.IsNullOrEmpty(TableName))
            {
                return mdb.FindTable(TableName);
            }
            return null;
        }
        private void SubTable(RowData Parent, SqlBuilder builder, DataRowView[] rows, DataSet ds, int CurrentTable, string key, bool WithMetadata, string UseHierachyField = null)
        {
            ResultTable rt = new ResultTable();
            rt.WithMetadata = WithMetadata;
            Table bt = builder.BaseTable();
            if (bt != null)
            {
                rt.Metadata = GetMetadataTable(builder.Metadata, bt.FullName);
            }



            string PropName = builder.BuilderName ?? rt.Metadata.Name + "List";
            //if (!PropName.EndsWith("List"))
            //{
            //    PropName += "List";
            //}
            PropName = PropName.Replace(".", "").Replace(" ", "").Replace("-", "");

            rt.Initialize(rows, ds.Tables[CurrentTable]);
            if (!Parent.Column<ResultTable>(PropName, rt))
            {
                throw new InvalidOperationException("Unable to set the child Resulttable " + PropName);
            }
            foreach (var kv in builder.SubQueries)
            {
                CurrentTable++;
                DataTable dt = ds.Tables[CurrentTable];
                List<DataColumn> pk = new List<DataColumn>();
                foreach (RowData rd in rt)
                {
                    DataView dv = new DataView(dt);
                    List<object> _pk = new List<object>();
                    string Sort = "";
                    foreach (MetadataColumn Column in rt.Metadata.PrimaryKey.Columns)
                    {
                        _pk.Add(rd.Column(Column.Name));
                        Sort += (!string.IsNullOrEmpty(Sort) ? ", " : "") + Column.Name + " ASC";
                    }
                    if (!string.IsNullOrEmpty(UseHierachyField))
                    {
                        Sort = UseHierachyField + " ASC";
                    }
                    dv.Sort = Sort;
                    DataRowView[] filteredRows = dv.FindRows(_pk.ToArray());
                    SubTable(rd, kv.Value, filteredRows, ds, CurrentTable, rt.Metadata.Name, WithMetadata, UseHierachyField);
                }
            }




        }


        public ResultTable(MetadataTable mt, DataTable dt)
        {
            this.Metadata = mt;
            Initialize(dt);
        }

        private void Initialize(DataRowView[] rows, DataTable dt)
        {
            foreach (DataRowView row in rows)
            {
                _Results.Add(new RowData(this, row.Row, dt.Columns));
            }
        }

        private void Initialize(DataTable dt)
        {
            foreach (DataRow row in dt.Rows)
            {
                _Results.Add(new RowData(this, row, dt.Columns));
            }
        }

        public string Name { get; set; }

        private MetadataTable _Metadata = null;
        //[JsonIgnore]
        public MetadataTable Metadata
        {
            get { return _Metadata; }
            set { _Metadata = value; }
        }

        private bool _WithMetadata = true;

        public bool WithMetadata
        {
            get { return _WithMetadata; }
            set { _WithMetadata = value; }
        }





        public int IndexOf(RowData item)
        {
            return _Results.IndexOf(item);
        }

        public void Insert(int index, RowData item)
        {
            _Results.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            _Results.RemoveAt(index);
        }

        public RowData this[int index]
        {
            get
            {
                return _Results[index];
            }
            set
            {
                _Results[index] = value;
            }
        }

        public void Add(RowData item)
        {
            _Results.Add(item);
        }

        public void Clear()
        {
            _Results.Clear();
        }

        public bool Contains(RowData item)
        {
            return _Results.Contains(item);
        }

        public void CopyTo(RowData[] array, int arrayIndex)
        {
            _Results.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _Results.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(RowData item)
        {
            return _Results.Remove(item);
        }



        public IEnumerator<RowData> GetEnumerator()
        {
            return _Results.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public enum DateHandlingEnum : int
        {
            None = 0,
            ConvertToString = 1,
            ConvertToDate = 2
        }

        public static DateHandlingEnum DefaultDateHandling = DateHandlingEnum.None;
        private DateHandlingEnum? _DateHandling = null;

        public DateHandlingEnum? DateHandling
        {
            get { return _DateHandling ?? DefaultDateHandling; }
            set { _DateHandling = value; }
        }
    }

    public class RowData : DynamicObject, ICloneable
    {
        public static RowData Create(MetadataTable Table, bool CachePrimaryKey = false, object[] PK = null)
        {
            ConcurrentDictionary<string, object> values = new ConcurrentDictionary<string, object>();
            foreach (MetadataColumn mc in Table.Columns.Values)
            {
                object v = null;
                if (!mc.Nullable && mc.DataType.IsValueType)
                {
                    v = Activator.CreateInstance(mc.DataType);
                }

                if (!values.TryAdd(
                    mc.Name,
                    v
                    ))
                {
                    throw new ArgumentException("The value from the column" + mc.Name + " could not be added to the row");
                }
            }
            RowData row = new RowData(values, new ConcurrentDictionary<string, object>());
            row.LoadMetadata(Table, CachePrimaryKey);
            if (PK != null)
            {
                for (int i = 0; i < Table.PrimaryKey.Columns.Count; i++)
                {
                    row.OriginalValues.AddOrUpdate(Table.PrimaryKey.Columns[i].Name, PK[i], (k, v) => { return PK[i]; });
                }
            }
            return row;
        }

        public RowData(ResultTable Parent, DataRow dr, DataColumnCollection Columns)
        {
            //_OriginalValues = new ConcurrentDictionary<string, object>(16, Columns.Count);
            //_ChangedValues = new ConcurrentDictionary<string, object>();
            foreach (DataColumn Col in Columns)
            {
                _Columns.Add(Col.ColumnName);
                object o = dr.IsNull(Col) ? null : dr[Col];
                string key = Col.ColumnName.Replace(" ", "_");
                if (o != null && Parent.DateHandling != ResultTable.DateHandlingEnum.None && o.GetType() == typeof(DateTime))
                {
                    if (Parent.DateHandling == ResultTable.DateHandlingEnum.ConvertToString)
                    {
                        o = ((DateTime)o).ToString(SqlBuilder.DefaultCulture);
                    }
                    else if (Parent.DateHandling == ResultTable.DateHandlingEnum.ConvertToDate)
                    {
                        o = ((DateTime)o).ToString("G", SqlBuilder.DefaultCulture);
                    }

                }
                _OriginalValues.TryAdd(key, o);
                //_OriginalValues[key] = o;
                //if (!_OriginalValues.TryAdd(key, o))
                //{
                //    throw new InvalidOperationException(string.Format("Unable to set the RowData value {0} for Column {1}", o, Col.ColumnName));
                //}
            }
            LoadMetadata(Parent.Metadata, Parent.WithMetadata);
        }


        public RowData()
        {

        }

        public bool LoadMetadata()
        {
            if (this.Metadata != null)
            {
                return false;
            }
            string table = this.Table;
            if (string.IsNullOrEmpty(table))
            {
                return false;
            }
            MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(Table);
            if (mt == null)
            {
                return false;
            }
            LoadMetadata(mt, true);
            _Columns = new List<string>(_OriginalValues.Keys);
            return true;

        }


        public void LoadMetadata(MetadataTable mt, bool CachePrimaryKey = false)
        {
            if (mt == null) { return; }
            _OriginalValues.AddOrUpdate("__TABLE", mt.Fullname, (k, v) => { return mt.Fullname; });
            
            if (CachePrimaryKey && mt.PrimaryKey!= null)
            {
                _OriginalValues.AddOrUpdate("__PK", mt.PrimaryKey.Columns, (k, v) => { return mt.PrimaryKey.Columns; });
            }
        }

        public string Table
        {
            get
            {
                if (!_OriginalValues.ContainsKey("__TABLE"))
                {
                    return null;
                }
                return Convert.ToString(_OriginalValues["__TABLE"]);
            }
        }

        private List<MetadataColumn> InternalPK
        {
            get
            {
                if (!_OriginalValues.ContainsKey("__PK"))
                {
                    return null;
                }
                else
                {
                    return (List<MetadataColumn>)_OriginalValues["__PK"];
                }
            }
        }


        [JsonIgnore]
        public MetadataTable Metadata
        {
            get
            {
                List<MetadataColumn> pk = InternalPK;
                if (pk != null)
                {
                    return pk.First().Parent;
                }
                return null;
            }
        }


        public WhereConditionGroup PrimaryKey(SqlBuilder builder)
        {
            List<MetadataColumn> columns = InternalPK;
            string table = Table;
            if (columns == null || table == null)
            {
                return null;
            }

            WhereConditionGroup pk = new WhereConditionGroup();
            pk.Builder = builder;
            foreach (MetadataColumn c in columns)
            {
                object o = null;
                if (InternalGet(c.Name, out o))
                {
                    pk.And(table, c.Name, SqlOperators.Equal, o);
                }
            }
            return pk;
        }

        internal RowData(ConcurrentDictionary<string, object> originalValues, ConcurrentDictionary<string, object> changedValues)
        {
            _OriginalValues = originalValues;
            _ChangedValues = changedValues;
        }


        private ConcurrentDictionary<string, object> _OriginalValues = new ConcurrentDictionary<string, object>();
        public ConcurrentDictionary<string, object> OriginalValues
        {
            get { return _OriginalValues; }
            set { _OriginalValues = value; }
        }
        private ConcurrentDictionary<string, object> _ChangedValues = new ConcurrentDictionary<string, object>();
        public ConcurrentDictionary<string, object> ChangedValues
        {
            get { return _ChangedValues; }
            set { _ChangedValues = value; }
        }

        public bool HasChanges
        {
            get { return _ChangedValues.Count > 0; }
        }

        public object Column(string Name)
        {
            object o;
            if (InternalGet(Name, out o))
            {
                return o;
            }
            else
            {
                throw new ArgumentException("The Column name '" + Name + "' does not exist", "Name");
            }
        }

        public bool Column(string Name, object Value)
        {
            return InternalSet(Name, Value);
        }
        public bool Column<T>(string Name, T Value)
        {
            return InternalSet(Name, Value);
        }

        public T Column<T>(string Name)
        {
            object o = Column(Name);
            return (T)o;
        }

        // Explicit column functions
        public Nullable<Guid> GuidCol(string Name, bool enforceTypeSafety = false)
        {
            object o = Column(Name);
            if (o == null || o == DBNull.Value)
            {
                return null;
            }
            if (!enforceTypeSafety)
            {
                return (Nullable<Guid>)o;
            }
            else
            {
                Guid t;
                if (Guid.TryParse(o.ToString(), out t))
                {
                    return t;
                }
            }
            return null;
        }

        // Explicit column functions
        public Nullable<DateTime> DateCol(string Name, bool enforceTypeSafety = false)
        {
            object o = Column(Name);
            if (o == null || o == DBNull.Value)
            {
                return null;
            }
            if (!enforceTypeSafety)
            {
                return (Nullable<DateTime>)o;
            }
            else
            {
                DateTime t;
                if (DateTime.TryParse(o.ToString(), out t))
                {
                    return t;
                }
            }
            return null;
        }

        // Explicit column functions
        public Nullable<Decimal> DecCol(string Name, bool enforceTypeSafety = false)
        {
            object o = Column(Name);
            if (o == null || o == DBNull.Value)
            {
                return null;
            }
            if (!enforceTypeSafety)
            {
                return (Nullable<Decimal>)o;
            }
            else
            {
                Decimal t;
                if (Decimal.TryParse(o.ToString(), out t))
                {
                    return t;
                }
            }
            return null;
        }

        // Explicit column functions
        public Nullable<int> IntCol(string Name, bool enforceTypeSafety = false)
        {
            decimal? d = DecCol(Name, enforceTypeSafety);
            if (d.HasValue)
            {
                return Convert.ToInt32(d.Value);
            }
            else
            {
                return null;
            }
        }

        public Nullable<double> DblCol(string Name, bool enforceTypeSafety = false)
        {
            decimal? d = DecCol(Name, enforceTypeSafety);
            if (d.HasValue)
            {
                return Convert.ToDouble(d.Value);
            }
            else
            {
                return null;
            }
        }





        private List<string> _Columns = new List<string>();

        public List<string> Columns
        {
            get { return _Columns; }
            set { _Columns = value; }
        }

        
        //public string[] Columns
        
        //    get
        //    {
        //        return Columns.ToArray();
        //        List<string> keys = new List<string>();
        //        //for (int i = 0; i < _OriginalValues.Keys.Count; i++)
        //        //{
        //        //    if (!_OriginalValues.Keys[i].StartsWith("__"))
        //        //    {
        //        //        keys.Add(_OriginalValues.Keys[i]);
        //        //    }
        //        //}
        //        //return keys.ToArray();
        //        foreach (string key in _OriginalValues.Keys)
        //        {
        //            if (!key.StartsWith("__"))
        //            {
        //                keys.Add(key);
        //            }
        //        }
        //        return keys.ToArray();
        //    }
        //}



        public void AcceptChanges()
        {
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew))
            {
                foreach (string key in _ChangedValues.Keys)
                {
                    object o;
                    if (_ChangedValues.TryRemove(key, out o))
                    {
                        _OriginalValues.AddOrUpdate(key, o, (k, v) => { return o; });
                    }
                    else
                    {
                        throw new InvalidOperationException(string.Format("Unable to get the value from the property '{0}'", key));
                    }
                }
                if (_ChangedValues.Count > 0)
                {
                    throw new InvalidOperationException(string.Format("There are still {0} unaccepted values", _ChangedValues.Count));
                }
                trans.Complete();
            }
        }




        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return _OriginalValues.Keys.AsEnumerable();
            //foreach (string key in _OriginalValues.Keys)
            //{
            //    yield return key;
            //}
            //yield return "Parent";
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return InternalGet(binder.Name, out result);
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            return InternalSet(binder.Name, value);
        }

        private bool InternalSet(string Column, object value)
        {
            object o;
            if (!OriginalValues.ContainsKey(Column))
            {
                return _OriginalValues.TryAdd(Column, value);
            }
            if (!_OriginalValues.TryGetValue(Column, out o))
            {
                return false;

            }
            if ((o == null && value != null) || !o.Equals(value))
            {
                _ChangedValues.AddOrUpdate(Column, value, (key, existing) =>
                {
                    return value;
                });
            }
            return true;
        }

        private bool InternalGet(string Column, out object value)
        {
            if (_ChangedValues.TryGetValue(Column, out value))
            {
                return true;
            }
            return _OriginalValues.TryGetValue(Column, out value);
        }



        public object Clone()
        {
            return new RowData(_OriginalValues, _ChangedValues);
        }
    }

    public enum SqlOperators
    {
        // General
        Null,
        NotNull,
        // Math
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanEqual,
        LessThan,
        LessThanEqual,
        // String
        StartsWith,
        EndsWith,
        Contains,
        // Lists
        In,
        NotIn
    }

    public enum OrderByDirections
    {
        Asc,
        Desc
    }

    public enum BoolOperators
    {
        And,
        Or,
        None
    }

    public enum BranchStatements
    {
        If,
        ElseIf,
        Else
    }

    public class OrderBy
    {
        public Field Field { get; set; }
        public OrderByDirections Direction { get; set; }

        public string ToSql()
        {
            // return (Field.Table.Schema != null ? Field.Table.Schema + "." : "") + Field.Table.Name + ".[" + Field.Name + "] " + Direction.ToString().ToUpper();
            return Field.Table.Alias + ".[" + Field.Name + "] " + Direction.ToString().ToUpper();
        }
    }


    public class StoredProcedure
    {
        public SqlBuilder Builder { get; set; }

        private List<ParameterField> _Parameters = new List<ParameterField>();
        public string Name { get; set; }


        public string Schema { get; set; }
        public string ReferenceName
        {
            get
            {
                return string.IsNullOrEmpty(Schema) ? Name : Schema + "." + Name;
            }
        }

        public List<ParameterField> Parameters
        {
            get { return _Parameters; }
            set { _Parameters = value; }
        }

        public string ToSql()
        {
            SqlBuilder tb = this.Builder.Builder();
            StringBuilder sql = new StringBuilder();
            string set = "";
            string call = "";
            foreach (ParameterField field in this.Parameters)
            {
                tb.AddDeclaration(field.ParameterName, field.DeclareParameter());
                set += field.SetParameter() + "\r\n";
                call += (!string.IsNullOrEmpty(call) ? ", " : "") + field.ParameterName + (field.IsOutput ? " OUT" : "");
            }

            sql.AppendLine("-- Stored procedure");
            sql.AppendFormat("{0}\r\n", set);
            sql.AppendFormat("EXEC {0} {1}\r\n", this.ReferenceName, call);
            string output = "";
            foreach (ParameterField field in this.Parameters.Where(x => x.IsOutput == true))
            {
                output += string.IsNullOrEmpty(output) ? field.ParameterName : ", " + field.ParameterName;
            }
            if (!string.IsNullOrEmpty(output))
            {
                sql.AppendFormat("SELECT  {0}\r\n", output);
            }
            return sql.ToString();
        }
    }

    public class IfStatement : SqlBuilder
    {
        public IfStatement()
        {
            StatementBody = new SqlBuilder();
            BranchStatement = BranchStatements.If;
            ElseIfStatements = new List<IfStatement>();
            _Conditions.Builder = this;
            _Conditions.Parent = this;
            StatementBody.ParentBuilder = this;
        }

        private SqlBuilder _Builder = null;

        public SqlBuilder Builder
        {
            get { return _Builder; }
            set
            {
                _Builder = value;
            }
        }


        public SqlBuilder StatementBody { get; set; }

        public BranchStatements BranchStatement { get; set; }

        private IfElseConditionGroup _Conditions = new IfElseConditionGroup();

        public IfElseConditionGroup Conditions
        {
            get { return _Conditions; }
            set { _Conditions = value; }
        }

        public List<IfStatement> ElseIfStatements { get; set; }

        public override Table BaseTable()
        {
            return this.StatementBody.BaseTable();
        }
        public override string ToSql()
        {
            StringBuilder sql = new StringBuilder();
            if (BranchStatement == BranchStatements.Else)
            {
                sql.AppendFormat("ELSE\r\nBEGIN\r\n");
            }
            else
            {
                sql.AppendFormat("{0} {1}\r\n", BranchStatement == BranchStatements.ElseIf ? "ELSE IF " : BranchStatement.ToString().ToUpper() + " ", _Conditions.ToSql());
                sql.AppendLine("BEGIN");
            }

            sql.AppendFormat("{0}\r\n", StatementBody.ToSql());
            sql.AppendLine("END");
            foreach (IfStatement statement in ElseIfStatements)
            {
                sql.AppendLine(statement.ToSql());
            }
            return base.ToSql() + "\r\n" + sql.ToString();
        }



    }

    public class IfElseConditionGroup : WhereConditionGroup
    {
        public new IfStatement Parent { get; set; }
    }



    public class Join
    {
        private static string JoinClause(JoinTypes JoinType)
        {
            switch (JoinType)
            {
                case JoinTypes.Inner:
                    return "INNER JOIN";
                case JoinTypes.LeftOuter:
                    return "LEFT OUTER JOIN";
                case JoinTypes.RightOuter:
                    return "RIGHT OUTER JOIN";
                case JoinTypes.Cross:
                    return "CROSS JOIN";
                default:
                    return "";
            }
        }
        public Join()
        {
            Conditions.Join = this;
            Conditions.Builder = Builder;
        }
        public enum JoinTypes
        {
            Inner,
            LeftOuter,
            RightOuter,
            Cross
        }
        public JoinTypes JoinType { get; set; }
        public JoinConditionGroup Conditions = new JoinConditionGroup();
        public Table FromTable;
        public Table ToTable;
        private SqlBuilder _Builder = null;

        public SqlBuilder Builder
        {
            get { return _Builder; }
            set
            {
                _Builder = value;
                Conditions.Builder = value;
            }
        }


        public string ToSql()
        {
            if (JoinType != JoinTypes.Cross)
            {
                return string.Format("{0} {1} ON {2}", JoinClause(JoinType), ToTable.ReferenceName, Conditions.ToSql());
            }
            else
            {
                return string.Format("{0} {1}", JoinClause(JoinType), ToTable.ReferenceName);
            }

        }

    }

    public class PrimaryKey : ConditionGroup
    {
        public PrimaryKey() { }
        public PrimaryKey(SqlBuilder builder, Table parent)
        {
            Builder = builder;
            Parent = parent;
        }

        public new Table Parent;
        // private new BoolOperators ConditionLink = BoolOperators.None;
        private new List<ConditionGroup> SubConditions = new List<ConditionGroup>();
        public new List<FieldCondition> Conditions = new List<FieldCondition>();
        // public SqlBuilder Builder;
    }

    public class ConditionGroup
    {
        public BoolOperators ConditionLink = BoolOperators.None;
        public List<FieldCondition> Conditions = new List<FieldCondition>();
        public List<ConditionGroup> SubConditions = new List<ConditionGroup>();
        public ConditionGroup Parent;
        public SqlBuilder Builder;

        public virtual string ToSql()
        {
            if (Conditions.Count == 0 && SubConditions.Count == 0)
            {
                return "";
            }
            string sql = ConditionLink != BoolOperators.None ? " " + ConditionLink.ToString().ToUpper() + " (" : "(";
            foreach (FieldCondition condition in Conditions)
            {
                sql += condition.ToSql();
            }
            foreach (ConditionGroup group in SubConditions)
            {
                sql += group.ToSql();
            }
            sql += ")";
            return sql;
        }

    }

    public class WhereConditionGroup : ConditionGroup
    {

    }
    public class JoinConditionGroup : ConditionGroup
    {
        public Join Join;
    }

    public class ExistsConditionGroup : ConditionGroup
    {
        public Table InTable;
        public string FromTable;
        public bool Negated = true;
        public ExistsConditionGroup()
        {
            this.ConditionLink = BoolOperators.None;
        }

        public override string ToSql()
        {
            // (NOT) EXISTS (SELECT 1 FROM InTable WHERE (Con )
            string sql = ConditionLink == BoolOperators.None ? "" : " " + ConditionLink.ToString().ToUpper() + " ";
            sql += Negated ? "NOT EXISTS(SELECT 1 FROM {0} WHERE {1})" : "EXISTS(SELECT 1 FROM {0} WHERE {1})";
            BoolOperators op = this.ConditionLink;
            ConditionLink = BoolOperators.None;
            sql = string.Format(sql, InTable.Alias, base.ToSql());
            ConditionLink = op;
            return sql;

        }
    }


    public class FieldCondition
    {
        private static string GetOperator(SqlOperators Operator)
        {
            switch (Operator)
            {
                case SqlOperators.Null:
                    return "IS";
                case SqlOperators.NotNull:
                    return "IS NOT";
                case SqlOperators.Equal:
                    return "=";
                case SqlOperators.NotEqual:
                    return "!=";
                case SqlOperators.GreaterThan:
                    return ">";
                case SqlOperators.GreaterThanEqual:
                    return ">=";
                case SqlOperators.LessThan:
                    return "<";
                case SqlOperators.LessThanEqual:
                    return "<=";
                case SqlOperators.StartsWith:
                case SqlOperators.EndsWith:
                case SqlOperators.Contains:
                    return "LIKE";
                case SqlOperators.In:
                    return "IN";
                case SqlOperators.NotIn:
                    return "NOT IN";
                default:
                    return "";
            }
        }
        public BoolOperators ConditionLink = BoolOperators.None;
        public Table LeftTable;
        public Field leftField;
        public Table RightTable;
        public Field RightField;
        public SqlOperators Condition;
        public SqlBuilder Builder;
        public ConditionGroup ParentGroup;
        public List<ConditionGroup> SubConditions = new List<ConditionGroup>();

        public virtual string ToSql()
        {
            string sql = ConditionLink != BoolOperators.None ? " " + ConditionLink.ToString().ToUpper() + " " : "";
            if (Condition == SqlOperators.NotNull || Condition == SqlOperators.Null)
            {
                sql += string.Format("{0} {1} NULL", leftField.DeclarationName, GetOperator(Condition));
            }
            else if (Condition == SqlOperators.In || Condition == SqlOperators.NotIn)
            {
                sql += string.Format("{0} {1} ({2})", leftField.DeclarationName, GetOperator(Condition), leftField.ToSql());
            }
            else
            {
                if (RightField == null)
                {
                    string q = ((ValueField)leftField).Quotable;
                    string qs = q == "'" ? "N'" : "";
                    switch (Condition)
                    {
                        case SqlOperators.Equal:
                        case SqlOperators.NotEqual:
                        case SqlOperators.GreaterThan:
                        case SqlOperators.GreaterThanEqual:
                        case SqlOperators.LessThan:
                        case SqlOperators.LessThanEqual:
                            sql += string.Format("{0} {1} {3}{2}{4}", leftField.DeclarationName, GetOperator(Condition), leftField.ToSql(), qs, q);
                            break;
                        case SqlOperators.StartsWith:
                            sql += string.Format("{0} {1} '{2}%'", leftField.DeclarationName, GetOperator(Condition), leftField.ToSql());
                            break;
                        case SqlOperators.EndsWith:
                            sql += string.Format("{0} {1} '%{2}'", leftField.DeclarationName, GetOperator(Condition), leftField.ToSql());
                            break;
                        case SqlOperators.Contains:
                            sql += string.Format("{0} {1} '%{2}%'", leftField.DeclarationName, GetOperator(Condition), leftField.ToSql());
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    sql += string.Format("{0} {1} {2}", leftField.DeclarationName, GetOperator(Condition), RightField.ReferenceName);
                }
            }
            foreach (ConditionGroup group in SubConditions)
            {
                sql += group.ToSql();
            }
            return sql;
        }

    }



    public class TableParameterField : ParameterField
    {
        public TableParameterField()
        {
            ParameterTable = new Table();
        }
        //private new System.Data.SqlDbType SqlDataType;
        //private new int MaxLength = -1;
        //private new int Scale = -1;
        //private new bool IsOutput = false;
        public Table ParameterTable;
        public override string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} TABLE(", ParameterName);
            Field f = ParameterTable.FieldList.First();
            sql += f.Name + " " + f.GetSqlDataType();
            foreach (Field Column in ParameterTable.FieldList.Skip(1))
            {
                sql += "," + Column.Name + " " + Column.GetSqlDataType();
            }
            sql += ")";
            return sql;
        }

        public override string SetParameter()
        {
            return "SELECT  * FROM " + this.ParameterName;
        }

        public override string ToSql()
        {
            string sql = string.Format("{0} INTO {1} \r\n", this.ParameterTable.ToSql(), this.ParameterName);
            return sql;
        }
    }


    public class ParameterField : ValueField
    {
        public string ParameterName { get; set; }
        private bool _IsOutput = false;

        public bool IsOutput
        {
            get { return _IsOutput; }
            set { _IsOutput = value; }
        }

        public virtual string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} {1}", ParameterName, GetSqlDataType());
            //if (IsOutput)
            //{
            //    sql += " OUT";
            //}
            return sql;
        }



        public virtual string SetParameter()
        {
            if (this.Value == null || this.Value == DBNull.Value)
            {
                return string.Format("SET  {0} = NULL", ParameterName);
            }
            string q = GetQuotable(this.DataType);
            string qs = q == "'" ? "N'" : "";
            return string.Format("SET {0} = {2}{1}{3}", ParameterName, base.ToSql(), qs, q);
        }

        public override string ReferenceName
        {
            get
            {
                return this.Alias ?? this.Name;
            }
        }

    }



    public class ValueField : Field
    {
        // public new object Value;
        public Type DataType;

        public virtual string Quotable
        {
            get
            {
                return GetQuotable(Value.GetType());
            }
        }

        public override string ToSql()
        {
            string sql = null;
            object o = GetFieldValue(DataType == null ? Value.GetType() : DataType, Value, this.Builder.Culture);
            if (o == null)
            {
                sql = "NULL";
            }
            else
            {
                sql = Convert.ToString(o);
            }
            if (string.IsNullOrEmpty(Alias))
            {
                return sql;
            }
            else
            {
                string q = this.Quotable;
                sql = string.Format("{0}{1}{0} [{2}]", q, sql, Alias);
                return q.Length == 1 ? "N" + sql : sql;
            }
        }

    }

    public class ParameterField<T> : ParameterField
    {
        public ParameterField()
        {
            DataType = typeof(T);
        }

        public T FieldValue
        {
            get
            {
                return (T)Value;
            }
            set
            {
                Value = value;
            }
        }




    }

    public class ValueField<T> : ValueField
    {

        public override string Quotable
        {
            get
            {
                return GetQuotable(this.DataType);
            }
        }

        public ValueField()
        {
            DataType = typeof(T);
        }

        public T FieldValue
        {
            get
            {
                return (T)Value;
            }
            set
            {
                Value = value;
            }
        }



    }

    public class FunctionField : Field
    {
        //protected List<FieldBase> _Parameters = new List<FieldBase>();
        public List<FieldBase> Parameters = new List<FieldBase>();
        public string Schema { get; set; }
        public override string ToSql()
        {
            string sql = (!string.IsNullOrEmpty(Schema) ? Schema + "." : "") + Name + "(";
            if (Parameters.Count > 0)
            {
                sql += Parameters.First().ToSql();
            }
            foreach (FieldBase field in Parameters.Skip(1))
            {
                sql += ", " + field.ToSql();
            }
            sql += ")";
            if (!string.IsNullOrEmpty(Alias))
            {
                sql += " [" + Alias + "]";
            }
            return sql;
        }


    }

    public class BuiltinFn
    {
        private BuiltinFn()
        {

        }
        internal SqlBuilder builder;
        internal Table table;

        public BuiltinFn GetDate(string Alias = null)
        {
            table.FieldList.Add(
            new FunctionField()
            {
                Name = "GETDATE",
                Schema = null,
                Builder = builder,
                Table = table,
                Alias = Alias
            });
            return this;
        }

        public enum AggregateTypes : int
        {
            Sum = 1,
            Max = 2,
            Min = 3
        }
        public BuiltinFn Aggregate(AggregateTypes AggregateType, string ColumnOrAlias, string Alias = null)
        {

            return this;




        }


        public BuiltinFn Concat(string Alias = null, params FieldBase[] Values)
        {
            FunctionField fn = new FunctionField()
            {
                Name = "CONCAT",
                Schema = null,
                Builder = builder,
                Table = table,
                Alias = Alias
            };
            fn.Parameters.AddRange(Values);
            table.FieldList.Add(fn);
            return this;
        }



        internal static BuiltinFn Fn(SqlBuilder builder, Table table)
        {
            BuiltinFn fn = new BuiltinFn();
            fn.builder = builder;
            fn.table = table;
            return fn;
        }

    }





    public class ConstantField<T> : FunctionField
    {
        public static ConstantField<T> Constant(T Value)
        {
            ConstantField<T> c = new ConstantField<T>();
            c.Value = Value;
            return c;
        }
        public ConstantField()
        {
            this.Name = null;
            this.Schema = null;
        }
        public new T Value { get; set; }
        public override string ToSql()
        {
            string q = GetQuotable(typeof(T));
            string sql = string.Format("{0}{1}{0}", q, GetFieldValue(typeof(T), Value));
            return q.Length == 1 ? "N" + sql : sql;
        }

    }

    public abstract class FieldBase
    {
        public string Name { get; set; }
        public abstract string ToSql();
    }


    public class Field : FieldBase
    {
        public Field()
        {

        }

        public string Alias
        {
            get;
            set;
        }

        public virtual object Value
        {
            get;
            set;
        }
        public Table Table { get; set; }
        public SqlBuilder Builder { get; set; }

        public System.Data.SqlDbType SqlDataType;
        public int MaxLength = -1;
        private int _Scale = -1;
        public int Scale
        {
            get { return _Scale; }
            set { _Scale = value; }
        }
        private int _Precision = -1;
        public int Precision
        {
            get { return _Precision; }
            set { _Precision = value; }
        }
        public virtual string DeclarationName
        {
            get
            {
                string table = this.Table.Alias;
                return table + ".[" + this.Name + "]";
            }
        }
        public virtual string ReferenceName
        {
            get
            {
                string table = this.Table.Alias;
                return table + ".[" + (this.Alias ?? this.Name) + "]";
            }
        }

        public virtual string OutputName
        {
            get
            {
                return string.IsNullOrEmpty(Alias) ? Alias : Name;
            }
        }


        private bool IsDateData(SqlDbType SqlDataType)
        {
            return SqlDataType == SqlDbType.DateTime || SqlDataType == SqlDbType.DateTime2 || SqlDataType == SqlDbType.DateTimeOffset;
        }

        public string GetSqlDataType()
        {
            string sql = SqlDataType.ToString();
            //if (MaxLength != -1 && (SqlDataType != SqlDbType.DateTime || SqlDataType != SqlDbType.DateTime2 || SqlDataType != SqlDbType.DateTimeOffset))
            //{
            //    sql += "(" + (MaxLength == 0 ? "max" : MaxLength.ToString()) + (Scale != -1 ? "," + Scale : "") + ")";
            //}
            //else
            //{
            if (SqlDataType == SqlDbType.NVarChar || SqlDataType == SqlDbType.VarChar || SqlDataType == SqlDbType.Text || SqlDataType == SqlDbType.NText)
            {
                if (MaxLength <= 0)
                {
                    sql += "(MAX)";
                }
                else
                {
                    sql += string.Format("({0})", MaxLength);
                }
            }
            else if (SqlDataType == SqlDbType.Char || SqlDataType == SqlDbType.NChar)
            {
                if (MaxLength > 0)
                {
                    sql += string.Format("({0})", MaxLength);
                }
            }


            //else if (SqlDataType == SqlDbType.Xml)
            //{
            //    if (Value.GetType() == typeof(XmlDocument))
            //    {
            //        sql += string.Format("({0})", MaxLength);
            //    }
            //}
            else if (SqlDataType == SqlDbType.VarBinary)
            {
                if (Value != null)
                {

                    sql += "(" + ((byte[])Value).Length.ToString() + ")";
                }
            }
            else
            {
                if (Precision > 0 && !IsDateData(SqlDataType))
                {
                    sql += string.Format("({0}{1})", Precision, Scale >= 0 ? ", " + Scale.ToString() : "");
                }
            }
            //}
            return sql;
        }

        protected static string GetQuotable(Type DataType)
        {
            if (DataType == typeof(XmlDocument) || DataType == typeof(Guid) || DataType == typeof(string) || DataType == typeof(DateTime) || DataType == typeof(DateTimeOffset) || DataType == typeof(Guid?) || DataType == typeof(DateTime?) || DataType == typeof(DateTimeOffset?))
            {
                return "'";
            }
            else
            {
                return "";
            }
        }


        public static object GetFieldValue(Type DataType, object FieldValue, CultureInfo Culture = null)
        {
            if (Culture == null)
            {
                Culture = SqlBuilder.DefaultCulture;
            }
            string fv = "";
            if ((DataType == typeof(Nullable<>) || DataType == typeof(string)) && FieldValue == null)
            {
                // return "";
                return null;
            }
            if (DataType == typeof(byte[]))
            {
                if (FieldValue == null)
                {
                    return "";
                }
                else
                {
                    byte[] bytes = (byte[])FieldValue;
                    StringBuilder hex = new StringBuilder(bytes.Length * 2);
                    foreach (byte b in bytes)
                        hex.AppendFormat("{0:x2}", b);
                    return "0x" + hex.ToString();
                }
            }
            if (DataType == typeof(XmlDocument))
            {
                if (FieldValue == null)
                {
                    return "";
                }
                else
                {
                    return ((XmlDocument)FieldValue).OuterXml.Replace("'", "''");
                }
            }
            if (DataType == typeof(DateTime) || DataType == typeof(DateTime?)) 
            {
                return string.Format("{0:s}", DateTime.Parse(FieldValue.ToString()));
            }
            if (DataType == typeof(DateTimeOffset)  || DataType == typeof(DateTimeOffset?))
            {
                return string.Format("{0:s}", DateTimeOffset.Parse(FieldValue.ToString()));
            }
            if (DataType == typeof(IList) || DataType == typeof(List<>) || DataType.Name.Equals("List`1"))
            {
                IList list = (IList)FieldValue;
                var item = list[0];
                string q = GetQuotable(item.GetType());
                fv = string.Format("{0}{1}{0}", q, item);
                for (int i = 1; i < list.Count; i++)
                {
                    fv += string.Format(",{0}{1}{0}", q, list[i]);
                }
                return fv;
            }
            else if (DataType == typeof(bool) || DataType == typeof(bool?))
            {
                return FieldValue == null ? "" : Convert.ToBoolean(FieldValue) == true ? "1" : "0";
            }
            else if (DataType == typeof(double) || DataType == typeof(double?))
            {
                return Convert.ToDouble(FieldValue).ToString(Culture);
            }
            else if (DataType == typeof(decimal) || DataType == typeof(decimal?))
            {
                return Convert.ToDecimal(FieldValue).ToString(Culture);
            }
            return FieldValue == null ? "" : FieldValue.ToString().Replace("'", "''");
        }



        public override string ToSql()
        {
            string tableAlias = this.Table.Alias;
            // return (!string.IsNullOrEmpty(tableAlias) ? "[" + tableAlias + "]." : "") + this.Name + (string.IsNullOrEmpty(this.Alias) || Value != null ? "" : " AS [" + Alias + "]");
            return (!string.IsNullOrEmpty(tableAlias) ? tableAlias + "." : "") + this.Name + (string.IsNullOrEmpty(this.Alias) || Value != null ? "" : " AS [" + Alias + "]");
        }
    }

    public class UpdateTable : Table
    {
        public UpdateTable()
        {
            Key.Parent = this;
        }
        public UpdateTable(SqlBuilder parent, string name, string Schema = null)
            : base(parent, name, null, Schema)
        {
            Key.Builder = parent;
            Key.Parent = this;
        }
        public override SqlBuilder Builder
        {
            get
            {
                return base.Builder;
            }
            set
            {
                base.Builder = value;
                Key.Builder = value;
            }
        }
        public override string ToSql()
        {
            if (FieldList.Count == 0)
            {
                return "";
            }
            List<ParameterField> fields = FieldList.OfType<ParameterField>().ToList();
            string sql = fields.First().ReferenceName + " = " + fields.First().ParameterName;
            foreach (ParameterField p in fields.Skip(1))
            {
                sql += ", " + p.ReferenceName + " = " + p.ParameterName;
            }
            return sql;
        }

        public PrimaryKey Key = new PrimaryKey();
        public TableParameterField Output = new TableParameterField();
    }

    public class TempTable : Table
    {
        public override string ReferenceName
        {
            get
            {
                return "#" + Name;
            }
        }

        private List<OrderBy> _OrderByClause = new List<OrderBy>();

        public List<OrderBy> OrderByClause
        {
            get { return _OrderByClause; }
            set { _OrderByClause = value; }
        }


        private bool _OutputTable = true;

        public bool OutputTable
        {
            get { return _OutputTable; }
            set { _OutputTable = value; }
        }

        public override string Alias
        {
            get
            {
                return _Alias ?? "#" + Name;
            }
            set
            {
                base.Alias = value;
            }
        }

        public override string Name
        {
            get
            {
                return base.Name;
            }
            set
            {
                base.Name = value;
            }
        }

    }

    public class InsertIntoTable : Table
    {
        public InsertIntoTable() { }
        public InsertIntoTable(SqlBuilder parent, string name)
            : base(parent, name, null)
        {

        }

        public override string ToSql()
        {
            if (FieldList.Count == 0)
            {
                return "";
            }
            string sql = FieldList[0].ReferenceName;
            for (int i = 1; i < FieldList.Count; i++)
            {
                sql += ", " + FieldList[i].ReferenceName;
            }
            return sql;
        }

        public override string ReferenceName
        {
            get
            {
                return this.Name;
            }
        }

        public string FieldParameters()
        {
            List<ParameterField> parameters = FieldList.Cast<ParameterField>().ToList();
            string sql = parameters.First().ParameterName;
            foreach (ParameterField p in parameters.Skip(1))
            {
                sql += ", " + p.ParameterName;
            }
            return sql;
        }

        public TableParameterField Output = new TableParameterField();

    }

    public class BaseTable : Table
    {

    }

    public class Table
    {
        public Table()
        {

        }
        public Table(SqlBuilder parent, string name, string alias, string schema = null)
        {
            Builder = parent;
            Name = name;
            if (!string.IsNullOrEmpty(alias))
            {
                Alias = alias;
            }
            Schema = schema;
        }
        public virtual SqlBuilder Builder
        {
            get;
            set;
        }
        private string _Name = null;
        public virtual string Name
        {
            get
            {
                // return (Schema != null ? "[" + Schema + "]." : "") + _Name;
                return _Name;
            }
            set
            {
                _Name = value;
            }
        }

        public virtual string FullName
        {
            get
            {
                return (!string.IsNullOrEmpty(Schema) ? Schema + "." : "") + Name;
            }
        }

        public string Schema = null;

        protected string _Alias;
        public virtual string Alias
        {
            get
            {
                // return _Alias ?? (!string.IsNullOrEmpty(Schema) ? "[" + Schema + "]." : "") + Name;
                return _Alias ?? FullName;
            }

            set { _Alias = value; }
        }
        public List<Field> FieldList = new List<Field>();

        public virtual string ReferenceName
        {
            get
            {
                // return (Schema != null ? "[" + Schema + "]." : "") + Name + (!string.IsNullOrEmpty(_Alias) ? " [" + Alias + "]" : "");
                return (Schema != null ? Schema + "." : "") + Name + (!string.IsNullOrEmpty(_Alias) ? " " + Alias : "");
            }
        }
        public virtual string ToSql()
        {
            if (FieldList.Count == 0)
            {
                return "";
            }
            string sql = FieldList[0].ToSql();
            for (int i = 1; i < FieldList.Count; i++)
            {
                sql += ", " + FieldList[i].ToSql();
            }
            return sql;
        }
    }











}
