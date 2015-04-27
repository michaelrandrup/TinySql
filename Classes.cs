using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Caching;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using System.Xml;

namespace TinySql
{

    public class ResultTable : List<RowData>
    {
        public ResultTable() { }
        public ResultTable(DataTable dt)
        {
            foreach (DataColumn col in dt.Columns)
            {
                Columns.Add(col.ColumnName, col);
            }
            foreach (DataRow row in dt.Rows)
            {
                this.Add(new RowData(this, row));
            }
        }
        public Dictionary<string, DataColumn> Columns = new Dictionary<string, DataColumn>();

    }

    public class RowData : DynamicObject
    {
        public RowData(ResultTable Parent, DataRow dr)
        {
            this.Parent = Parent;
            this.row = dr;
        }
        private DataRow row;
        public ResultTable Parent;
        private string[] _columns;

        public string[] Columns
        {
            get
            {
                if (_columns == null)
                {

                }
                return _columns;
            }
            set { _columns = value; }
        }
        public override IEnumerable<string> GetDynamicMemberNames()
        {
            foreach (string key in Parent.Columns.Keys)
            {
                yield return key.Replace(" ", "_");
            }
        }
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = null;
            if (Parent.Columns.ContainsKey(binder.Name.Replace("_", " ")))
            {
                result = row[binder.Name.Replace("_", " ")];
                return true;
            }
            return false;
            // return base.TryGetMember(binder, out result);
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

    public enum BoolOperators
    {
        And,
        Or,
        None
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
            return string.Format("{0} {1} ON {2}", JoinClause(JoinType), ToTable.ReferenceName, Conditions.ToSql());
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
        private new BoolOperators ConditionLink = BoolOperators.None;
        private new List<ConditionGroup> SubConditions = new List<ConditionGroup>();
        public List<FieldCondition> Conditions = new List<FieldCondition>();
        public SqlBuilder Builder;
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
            if (Conditions.Count == 0)
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
                sql += string.Format("{0} {1} NULL", leftField.ReferenceName, GetOperator(Condition));
            }
            else if (Condition == SqlOperators.In || Condition == SqlOperators.NotIn)
            {
                sql += string.Format("{0} {1} ({2})", leftField.ReferenceName, GetOperator(Condition), leftField.ToSql());
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
                            sql += string.Format("{0} {1} {3}{2}{4}", leftField.ReferenceName, GetOperator(Condition), leftField.ToSql(), qs, q);
                            break;
                        case SqlOperators.StartsWith:
                            sql += string.Format("{0} {1} '{2}%'", leftField.ReferenceName, GetOperator(Condition), leftField.ToSql());
                            break;
                        case SqlOperators.EndsWith:
                            sql += string.Format("{0} {1} '%{2}'", leftField.ReferenceName, GetOperator(Condition), leftField.ToSql());
                            break;
                        case SqlOperators.Contains:
                            sql += string.Format("{0} {1} '%{2}%'", leftField.ReferenceName, GetOperator(Condition), leftField.ToSql());
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    sql += string.Format("{0} {1} {2}", leftField.ReferenceName, GetOperator(Condition), RightField.ReferenceName);
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
        private new System.Data.SqlDbType SqlDataType;
        private new int MaxLength = -1;
        private new int Scale = -1;
        private new bool IsOutput = false;
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
    

    public abstract class ParameterField : ValueField
    {
        public string ParameterName;
        public bool IsOutput = false;

        public virtual string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} {1}", ParameterName, GetSqlDataType());
            //if (MaxLength != -1)
            //{
            //    sql += "(" + (MaxLength == 0 ? "max" : MaxLength.ToString()) + (Scale != -1 ? "," + Scale : "") + ")";
            //}
            //else
            //{
            //    if (SqlDataType == SqlDbType.NVarChar || SqlDataType == SqlDbType.VarChar || SqlDataType == SqlDbType.Text)
            //    {
            //        sql += "(" + Value.ToString().Length + ")";
            //    }
            //    else if (SqlDataType == SqlDbType.VarBinary)
            //    {
            //        if (Value != null)
            //        {
            //            sql += "(" + ((byte[])Value).Length + ")";
            //        }
            //    }
            //}
            if (IsOutput)
            {
                sql += " OUT";
            }
            return sql;
        }

        public virtual string SetParameter()
        {
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

    public abstract class ValueField : Field
    {
        public new object Value;
        public Type DataType;

        public virtual string Quotable
        {
            get
            {
                return GetQuotable(Value.GetType());
            }
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

        
        //protected string GetSqlDataType()
        //{
        //    string sql = SqlDataType.ToString();
        //    if (MaxLength != -1)
        //    {
        //        sql += "(" + (MaxLength == 0 ? "max" : MaxLength.ToString()) + (Scale != -1 ? "," + Scale : "") + ")";
        //    }
        //    else
        //    {
        //        if (SqlDataType == SqlDbType.NVarChar || SqlDataType == SqlDbType.VarChar || SqlDataType == SqlDbType.Text)
        //        {
        //            sql += "(" + Value.ToString().Length + ")";
        //        }
        //        else if (SqlDataType == SqlDbType.VarBinary)
        //        {
        //            if (Value != null)
        //            {
        //                sql += "(" + ((byte[])Value).Length + ")";
        //            }
        //        }
        //    }
        //    return sql;
        //}

        protected static object GetFieldValue(Type DataType, object FieldValue, CultureInfo Culture = null)
        {
            if (Culture == null)
            {
                Culture = SqlBuilder.DefaultCulture;
            }
            string fv = "";
            if ((DataType == typeof(Nullable<>) || DataType == typeof(string)) && FieldValue == null)
            {
                return "";
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
            if (DataType == typeof(DateTime) || DataType == typeof(DateTimeOffset) || DataType == typeof(DateTime?) || DataType == typeof(DateTimeOffset?))
            {
                return string.Format("{0:s}", FieldValue);
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
            return GetFieldValue(DataType == null ? Value.GetType() : DataType, Value).ToString();
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

    public class Field
    {
        public Field()
        {

        }
        public string Name
        {
            get;
            internal set;
        }
        public string Alias
        {
            get;
            set;
        }

        public ValueField Value
        {
            get;
            set;
        }
        public Table Table { get; set; }
        public SqlBuilder Builder { get; set; }

        public System.Data.SqlDbType SqlDataType;
        public int MaxLength = -1;
        public int Scale = -1;

        

        public virtual string DeclarationName
        {
            get
            {
                return this.Name;
            }
        }
        public virtual string ReferenceName
        {
            get
            {
                string table = this.Table.Alias;
                if (table.StartsWith("["))
                {
                    return table + ".[" + (this.Alias ?? this.Name) + "]";
                }
                else
                {
                    return "[" + table + "].[" + (this.Alias ?? this.Name) + "]";
                }
                
            }
        }

        public string GetSqlDataType()
        {
            string sql = SqlDataType.ToString();
            if (MaxLength != -1)
            {
                sql += "(" + (MaxLength == 0 ? "max" : MaxLength.ToString()) + (Scale != -1 ? "," + Scale : "") + ")";
            }
            else
            {
                if (SqlDataType == SqlDbType.NVarChar || SqlDataType == SqlDbType.VarChar || SqlDataType == SqlDbType.Text || SqlDataType == SqlDbType.NText)
                {
                    sql += "(MAX)";
                }
                else if (SqlDataType == SqlDbType.VarBinary)
                {
                    if (Value != null)
                    {

                        sql += "(" + ((byte[])Value.Value).Length.ToString() + ")";
                    }
                }
            }
            return sql;
        }

        public virtual string ToSql()
        {
            string tableAlias = this.Table.Alias;
            return (!string.IsNullOrEmpty(tableAlias) ? "[" + tableAlias + "]." : "") + this.Name + (string.IsNullOrEmpty(this.Alias) || Value != null ? "" : " AS [" + Alias + "]");
        }
    }

    public class UpdateTable : Table
    {
        public UpdateTable()
        {
            Key.Parent = this;
        }
        public UpdateTable(SqlBuilder parent, string name, string Schema = null)
            : base(parent, name,null,Schema)
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
        public string Name
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

        public string Schema = null;

        private string _Alias;
        public string Alias
        {
            get
            {
                return _Alias ?? (!string.IsNullOrEmpty(Schema) ? "[" + Schema + "]." : "") + Name;
            }

            set { _Alias = value; }
        }
        public List<Field> FieldList = new List<Field>();

        public virtual string ReferenceName
        {
            get
            {
                return (Schema != null ? "[" + Schema + "]." : "") + Name + (!string.IsNullOrEmpty(_Alias) ? " [" + Alias + "]" : "");
                // return Name + (!string.IsNullOrEmpty(_Alias) ? " [" + Alias + "]" : "");
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
