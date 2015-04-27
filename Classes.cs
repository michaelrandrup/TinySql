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

    public static class TestSql
    {
        private class Person
        {
            public int PersonID;
            public string Fornavne;
            public string Efternavne;
            public int TelefonID;
            public string Telefonnummer;
            public string Email;
        }
        public static void ToSql()
        {
            SqlBuilder.DefaultConnection = "Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;";

            SqlBuilder builder = SqlBuilder.Select(15000).From("Personer").AllColumns()
                .ConcatColumns("Fulde navn", " ", "Fornavne", "Efternavne")
                .LeftOuterJoin("Kontaktinfo", "Telefon").On("PersonID", SqlOperators.Equal, "PersonID").And<int>("KontaktInfoTypeID", SqlOperators.Equal, 1)
                    .ToTable().Column("Tekst", "Telefonnummer").Column("KontaktInfoID", "TelefonID")
                .From("Personer").LeftOuterJoin("Kontaktinfo", "Email").On("PersonID", SqlOperators.Equal, "PersonID").And<int>("KontaktInfoTypeID", SqlOperators.Equal, 2)
                    .ToTable().Column("Tekst", "Email")
                .From("Personer").InnerJoin("SystemUser", "CreatedOn").On("CreatedBy", SqlOperators.Equal, "SystemUserID")
                    .ToTable().Columns("SystemUserID", "NTLoginName")
                .Where<int>("Personer", "StatusID", SqlOperators.Equal, 1)
                .And<DateTime>("Personer", "CreatedOn", SqlOperators.LessThan, new DateTime(2015, 1, 1))
                .AndExists("SystemUser")
                    .And("CreatedBy", SqlOperators.Equal, "SystemUserID")
                    .And<string>("SystemUser", "NTLoginName", SqlOperators.Contains, "{0}")
                .EndExists()
                //.OrGroup()
                //    .And<string>("PageDataFolder", "ParentFolderID", SqlOperators.NotNull, null)
                //    .And<string>("PageData", "ViewName", SqlOperators.StartsWith, "~/Cart")
                .Builder;


            if (SqlBuilder.CacheSqlBuilder("TESTSQL2", builder))
            {
                Console.WriteLine("TESTSQL INSERTED INTO CACHE");
            }
            else
            {
                Console.WriteLine("ALREADY IN CACHE");
            }
            string sql = builder.ToSql("alin");
            DateTime dtStart = DateTime.Now;
            ResultTable result = builder.Execute("Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;",30, "adam");
            Console.WriteLine("Execute: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, result.Count);
            //foreach (dynamic row in result)
            //{
            //    Console.WriteLine("Person {0}: {1}. Telefon {2}. Email {3}", row.PersonID, row.Fulde_navn,row.Telefonnummer,row.Email);
            //}

            if (SqlBuilder.CacheSqlBuilder("TESTSQL2", builder))
            {
                Console.WriteLine("TESTSQL INSERTED INTO CACHE");
            }
            else
            {
                Console.WriteLine("ALREADY IN CACHE");
            }

            SqlBuilder builder2 = SqlBuilder.CacheSqlBuilder("TESTSQL2");
            dtStart = DateTime.Now;
            result = builder2.Execute("Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;", 30,"adam");
            Console.WriteLine("Execute pass 2: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, result.Count);
            //foreach (dynamic row in result)
            //{
            //    Console.WriteLine("Person {0}: {1}. Telefon {2}. Email {3}", row.PersonID, row.Fulde_navn, row.Telefonnummer, row.Email);
            //}
            dtStart = DateTime.Now;
            List<Person> personList = builder2.List<Person>("Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;", 30, false, true, "adam");
            Console.WriteLine("ExecuteList: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personList.Count);
            // Console.WriteLine("PERSON OBJECT LIST");
            //foreach (Person person in personList.Take(10))
            //{
            //    Console.WriteLine("Person {0}: {1} {2}. Telefon {5}-{3}. Email {4}", person.PersonID, person.Fornavne, person.Efternavne, person.Telefonnummer, person.Email,person.TelefonID);
            //}

            dtStart = DateTime.Now;
            Dictionary<int, Person> personDictionary = builder2.Dictionary<int, Person>("PersonID", "Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;", 30, false, true, "adam");
            Console.WriteLine("ExecuteDictionary: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personDictionary.Count);
            // Console.WriteLine("PERSON OBJECT DICTIONARY");
            //foreach (KeyValuePair<int,Person> kvp in personDictionary)
            //{
            //    Console.WriteLine("Person {0}: {1} {2}. Telefon {3}. Email {4}", kvp.Key, kvp.Value.Fornavne, kvp.Value.Efternavne, kvp.Value.Telefonnummer, kvp.Value.Email);
            //}
            dtStart = DateTime.Now;
            DataTable dt = builder2.DataTable("Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;", 30,"adam");
            Console.WriteLine("ExecuteDataTable: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personDictionary.Count);

            dtStart = DateTime.Now;
            personList = builder2.List<Person>(dt, false, true);
            Console.WriteLine("ExecuteList From DataTable: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personList.Count);

            dtStart = DateTime.Now;
            personDictionary = builder2.Dictionary<int, Person>("PersonID", dt, false, true);
            Console.WriteLine("ExecuteDictionary From DataTable: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personDictionary.Count);


            SqlBuilder insert = SqlBuilder.Insert().Into("Personer")
                .Value<string>("Fornavne", SqlDbType.VarChar, "Michael")
                .Value<string>("Efternavne", SqlDbType.VarChar, "Randrup")
                .Value<DateTime>("CreatedOn", SqlDbType.DateTime, new DateTime(2015, 12, 24))
                .Value<DateTime>("OprettetDato", SqlDbType.DateTime, new DateTime(2015, 12, 24))
                .Value<double>("CreatedBy", SqlDbType.Decimal, 1, 18, 9)
                .Builder;

            sql = insert.ToSql();

            SqlBuilder update = SqlBuilder.Update()
                .Table("Personer")
                .Set<string>("Fornavne", SqlDbType.VarChar, "Nyt fornavn")
                .Set<string>("Efternavne", SqlDbType.VarChar, "Nyt efternavn " + DateTime.Now.ToString())
                .From("Personer")
                .InnerJoin("SystemUser").On("CreatedBy", SqlOperators.Equal, "SystemUserID").ToTable()
                .Where<string>("SystemUser", "NTLoginName", SqlOperators.Equal, "ADM")
                .And<DateTime>("Personer", "CreatedOn", SqlOperators.Equal, new DateTime(2015, 12, 24))
                .Builder;

            sql = update.ToSql();

            update = SqlBuilder.Update()
                .Table("Personer")
                .Set<string>("Fornavne", SqlDbType.VarChar, "Nyt fornavn")
                .Set<string>("Efternavne", SqlDbType.VarChar, "Nyt efternavn " + DateTime.Now.ToString())
                .Where<int>("Personer", "PersonID", SqlOperators.Equal, 1631529)
                .Builder;

            sql = update.ToSql();
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<Root><Node1 attribute='Hej'>Michael's XML</Node1></Root>");
            update = SqlBuilder.Update()
                .Table("Personer")
                .Set<string>("Fornavne", SqlDbType.VarChar, "Nyt fornavn")
                .Set<string>("Efternavne", SqlDbType.VarChar, "Nyt efternavn " + DateTime.Now.ToString())
                .Set<byte[]>("Encrypted", SqlDbType.VarBinary, new byte[] { 24, 2, 2, 200, 3, 57, 64 })
                .Set<XmlDocument>("Doc", SqlDbType.Xml, doc)
                .Where<int>("Personer", "PersonID", SqlOperators.Equal, 1631529)
                .Builder;

            sql = update.ToSql();

            SqlBuilder delete = SqlBuilder.Delete()
                .From("Personer")
                .InnerJoin("SystemUser").On("CreatedBy", SqlOperators.Equal, "SystemUserID").ToTable()
                .Where<DateTime>("Personer", "CreatedOn", SqlOperators.Equal, new DateTime(2015, 12, 24))
                .And<string>("SystemUser", "NTLoginName", SqlOperators.Equal, "ADM")
                .Builder;

            sql = delete.ToSql();

            delete = SqlBuilder.Delete()
                .From("Personer")
                .Where<int>("Personer", "PersonID", SqlOperators.Equal, 1631530)
                .Builder;

            sql = delete.ToSql();

            //
            // Exclude fields
            //
            dtStart = DateTime.Now;
            personList = Data.List<Person>("Personer", null, new string[] { "TelefonID", "Telefonnummer", "Email" }, 150000, false, "Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;", 30, false, true, "adam");
            Console.WriteLine("ExecuteList from Object: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personList.Count);
            foreach (Person person in personList.Take(10))
            {
                Console.WriteLine("Person {0}: {1} {2}. Telefon {5}-{3}. Email {4}", person.PersonID, person.Fornavne, person.Efternavne, person.Telefonnummer, person.Email, person.TelefonID);
            }
            //
            // Include related fields
            //
            dtStart = DateTime.Now;
            builder = TypeBuilder.Select<Person>("Personer", null, new string[] { "TelefonID", "Telefonnummer", "Email" }, 150000);
            builder.From("Personer")
                .LeftOuterJoin("Kontaktinfo", "Telefon").On("PersonID", SqlOperators.Equal, "PersonID").And<int>("KontaktInfoTypeID", SqlOperators.Equal, 1)
                    .ToTable().Column("Tekst", "Telefonnummer").Column("KontaktInfoID", "TelefonID")
                .From("Personer").LeftOuterJoin("Kontaktinfo", "Email").On("PersonID", SqlOperators.Equal, "PersonID").And<int>("KontaktInfoTypeID", SqlOperators.Equal, 2)
                    .ToTable().Column("Tekst", "Email");

            personList = builder.List<Person>("Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;", 30, false, true, "adam");
            Console.WriteLine("ExecuteList from Object with Joins: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, personList.Count);
            foreach (Person person in personList.Take(10))
            {
                Console.WriteLine("Person {0}: {1} {2}. Telefon {5}-{3}. Email {4}", person.PersonID, person.Fornavne, person.Efternavne, person.Telefonnummer, person.Email, person.TelefonID);
            }

            //dtStart = DateTime.Now;
            //Dictionary<int, DLogic.Personer> p = Data.All<int, DLogic.Personer>("PersonID", null, 20000, false, "Server=192.168.1.3;Database=INGAMHTEST;Integrated Security=SSPI;");
            //Console.WriteLine("AllRows: {0} for {1} rows", (DateTime.Now - dtStart).TotalMilliseconds, dt.Rows.Count);

            dtStart = DateTime.Now;
            // List<DLogic.Personer> personer = builder.ExecuteList<.


            int TestRows = 1000;
            Console.WriteLine("Creating Builders for {0} rows", TestRows);
            List<SqlBuilder> Builders = new List<SqlBuilder>();
            for (int i = 0; i < TestRows; i++)
            {
                Builders.Add(SqlBuilder.Insert().Into("Personer")
                .Value<string>("Fornavne", SqlDbType.VarChar, "Michael")
                .Value<string>("Efternavne", SqlDbType.VarChar, "Randrup")
                .Value<DateTime>("CreatedOn", SqlDbType.DateTime, new DateTime(2015, 12, 24))
                .Value<DateTime>("OprettetDato", SqlDbType.DateTime, new DateTime(2015, 12, 24))
                .Value<double>("CreatedBy", SqlDbType.Decimal, 1, 18, 9)
                .Builder);
            }
            Console.WriteLine("Builders created: {0} in {1} ms", TestRows, (DateTime.Now - dtStart).TotalMilliseconds);
            dtStart = DateTime.Now;
            int rows = Builders.ToArray().ExecuteNonQuery();
            Console.WriteLine("Rows INSERTED: {0} in {1} ms", rows, (DateTime.Now - dtStart).TotalMilliseconds);


            dtStart = DateTime.Now;
            builder = SqlBuilder.Select().From("Personer").AllColumns()
                .Where<DateTime>("Personer", "CreatedOn", SqlOperators.Equal, new DateTime(2015, 12, 24))
                .Builder;
            dt = builder.DataTable();
            Console.WriteLine("Rows SELECTED: {0} in {1} ms", dt.Rows.Count, (DateTime.Now - dtStart).TotalMilliseconds);

            dtStart = DateTime.Now;
            builder = SqlBuilder.Update().Table("Personer")
                .Set<string>("Efternavne", SqlDbType.VarChar, "Randrup")
                .Where<DateTime>("Personer", "CreatedOn", SqlOperators.Equal, new DateTime(2015, 12, 24))
                .Builder;
            rows = new SqlBuilder[] { builder }.ExecuteNonQuery(null, 0);
            Console.WriteLine("Rows UPDATED: {0} in {1} ms", rows, (DateTime.Now - dtStart).TotalMilliseconds);


            
            delete = SqlBuilder.Delete()
               .From("Personer")
               .InnerJoin("SystemUser").On("CreatedBy", SqlOperators.Equal, "SystemUserID").ToTable()
               .Where<DateTime>("Personer", "CreatedOn", SqlOperators.Equal, new DateTime(2015, 12, 24))
               .And<string>("SystemUser", "NTLoginName", SqlOperators.Equal, "ADM")
               .Builder;

            dtStart = DateTime.Now;
            rows = new SqlBuilder[] { delete }.ExecuteNonQuery(null, 0);
            Console.WriteLine("Rows DELETED: {0} in {1} s", rows, (DateTime.Now - dtStart).TotalSeconds);





        }
    }

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
        private new System.Data.SqlDbType SqlDataType;
        private new int MaxLength = -1;
        private new int Scale = -1;
        private new bool IsOutput = false;
        public List<Field> Columns = new List<Field>();
        public override string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} TABLE(", ParameterName);
            foreach (Field Column in Columns)
            {
                ValueField f;
                
                
            }
        }
    }
    

    public abstract class ParameterField : ValueField
    {
        public string ParameterName;
        public bool IsOutput = false;

        public virtual string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} {1}", ParameterName, SqlDataType);
            if (MaxLength != -1)
            {
                sql += "(" + (MaxLength == 0 ? "max" : MaxLength.ToString()) + (Scale != -1 ? "," + Scale : "") + ")";
            }
            else
            {
                if (SqlDataType == SqlDbType.NVarChar || SqlDataType == SqlDbType.VarChar || SqlDataType == SqlDbType.Text)
                {
                    sql += "(" + Value.ToString().Length + ")";
                }
                else if (SqlDataType == SqlDbType.VarBinary)
                {
                    if (Value != null)
                    {
                        sql += "(" + ((byte[])Value).Length + ")";
                    }
                }
            }
            if (IsOutput)
            {
                sql += " OUT";
            }
            return sql;
        }

        public string SetParameter()
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

        public virtual string ToSql()
        {
            return "[" + this.Table.Alias + "]." + this.Name + (string.IsNullOrEmpty(this.Alias) || Value != null ? "" : " AS [" + Alias + "]");
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
        public Table Output;
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

        public Table Output;

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
            Alias = alias;
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

        private static string _DefaultConnection = null;

        public static string DefaultConnection
        {
            get { return _DefaultConnection; }
            set { _DefaultConnection = value; }
        }

        public string ConnectionString { get; set; }

        public object[] Format;



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
            if (Format == null)
            {
                return ToSql();
            }
            
            return string.Format(ToSql(), Format);
        }

        public string ToSql()
        {
            switch (StatementType)
            {
                case StatementTypes.Select:
                    return SelectSql();
                case StatementTypes.Insert:
                    return InsertSql();
                case StatementTypes.Update:
                    return UpdateSql();
                case StatementTypes.Delete:
                    return DeleteSql();
                default:
                    throw new NotImplementedException();
            }
        }

        private string DeleteSql()
        {

            StringBuilder sb = new StringBuilder();
            Table BaseTable = this.Tables[0];

            sb.AppendFormat("DELETE  {0}\r\n", BaseTable.Name);
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
            return sb.ToString();
        }
        private string InsertSql()
        {
            StringBuilder sb = new StringBuilder();
            InsertIntoTable BaseTable = this.Tables[0] as InsertIntoTable;
            string declare = "";
            string set = "";
            foreach (ParameterField field in BaseTable.FieldList)
            {
                declare += field.DeclareParameter() + "\r\n";
                set += field.SetParameter() + "\r\n";
            }

            sb.AppendLine(declare);
            sb.AppendLine(set);
            sb.AppendFormat(" INSERT  INTO {0}({1})\r\n", BaseTable.Alias, BaseTable.ToSql());
            if (BaseTable.Output != null)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", BaseTable.Output.ToSql());
            }
            sb.AppendFormat("VALUES({0})", BaseTable.FieldParameters());
            return sb.ToString();

        }
        private string SelectSql()
        {
            StringBuilder sb = new StringBuilder();
            Table BaseTable = this.Tables[0];
            string selectList = BaseTable.ToSql();
            foreach (Table t in Tables.Skip(1))
            {
                selectList += ", " + t.ToSql();
            }
            sb.AppendFormat("SELECT {1} {2}  {0}\r\n", selectList, Distinct ? "DISTINCT" : "", Top.HasValue ? "TOP " + Top.Value.ToString() : "");
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable.ReferenceName);
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            string where = WhereConditions.ToSql();
            if (where != "()" && !string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            return sb.ToString();
        }

        private string UpdateSql()
        {
            StringBuilder sb = new StringBuilder();
            UpdateTable BaseTable = this.Tables[0] as UpdateTable;
            string declare = "";
            string set = "";
            foreach (ParameterField field in BaseTable.FieldList.OfType<ParameterField>())
            {
                declare += field.DeclareParameter() + "\r\n";
                set += field.SetParameter() + "\r\n";
            }

            sb.AppendLine(declare);
            sb.AppendLine(set);

            sb.AppendFormat("UPDATE  {0}\r\n", BaseTable.Name);
            sb.AppendFormat("   SET  {0}\r\n", BaseTable.ToSql());
            if (BaseTable.Output != null)
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
            return sb.ToString();
        }

        public enum StatementTypes
        {
            Select = 1,
            Insert = 2,
            Update = 3,
            Delete = 4
        }

        private System.Globalization.CultureInfo _Culture = null;

        public System.Globalization.CultureInfo Culture
        {
            get { return _Culture ?? DefaultCulture; }
            set { _Culture = value; }
        }
        public static CultureInfo DefaultCulture
        {
            get
            {
                return System.Globalization.CultureInfo.GetCultureInfo(1033);
            }
        }


        public List<Table> Tables = new List<Table>();
        public WhereConditionGroup WhereConditions = new WhereConditionGroup();



        public List<Join> JoinConditions = new List<Join>();
        public StatementTypes StatementType
        {
            get;
            private set;
        }

        public int? Top
        {
            get;
            private set;
        }
        public bool Distinct
        {
            get;
            private set;
        }




    }

  

    





}
