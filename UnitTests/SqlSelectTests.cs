using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Xml;
using System.Linq;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class SqlSelectTests : BaseTest
    {
        [TestMethod]
        public void SelectAllColumnsFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select().From("account", null).AllColumns().Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Account> list = builder.List<Account>(null, 30, true, true);
            Console.WriteLine("List<Account> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void SelectWithSubQuery()
        {
            SqlBuilder builder = SqlBuilder.Select(100)
                .From("Account")
                .AllColumns()
                .SubSelect("Contact", "AccountID", "AccountID", null)
                .Columns("ContactID", "Name", "Title")
                .Builder();
                

            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            System.Data.DataSet result = builder.DataSet();
            Console.WriteLine("Dataset with {0} tables executed in {1}ms", result.Tables.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            for (int i = 0; i < result.Tables.Count; i++)
            {
                Console.WriteLine("Table {0} contains {1} rows", i, result.Tables[i].Rows.Count);
            }
        }

       

        [TestMethod]
        public void SelectAllBusinessEntitiesWithNestedSubQueriesAndJoin()
        {
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .Columns("AccountID","Name","Address1","PostalCode","City")
                .SubSelect("Contact", "AccountID", "AccountID")
                .Columns("ContactID","Name","Title","Telephone","Mobile","WorkEmail","PrivateEmail")
                .InnerJoin("ListMember").On("ContactID", SqlOperators.Equal, "ContactID").ToTable()
                .Columns("ListMemberID", "ListMemberStatusID")
                .From("Contact")
                .SubSelect("ListMember","ContactID","ContactID")
                .Columns("ContactID","ListMemberID","ListmemberStatusID","ListID")
                .SubSelect("List", "ListID", "ListID")
                .Columns("ListID","Name")
                .Builder();

            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            System.Data.DataSet result = builder.DataSet();
            Console.WriteLine("Dataset with {0} tables executed in {1}ms", result.Tables.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            for (int i = 0; i < result.Tables.Count; i++)
            {
                Console.WriteLine("Table {0} contains {1} rows", i, result.Tables[i].Rows.Count);
            }
        }


        [TestMethod]
        public void SelectWithAliasInWhereClause()
        {
            SqlBuilder builder = SqlBuilder.Select()
                .From("account", null)
                .Into("tempPerson")
                .Column("AccountID","ID")
                .Columns("Name","Address1","PostalCode","City")
                .OrderBy("ID", OrderByDirections.Desc)
                .Where<int>("account","ID", SqlOperators.LessThan,1000)
                .AndGroup()
                    .And<string>("account","Name", SqlOperators.StartsWith,"A")
                    .Or<string>("account","City", SqlOperators.StartsWith,"A")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResultTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }
        [TestMethod]
        public void Select2000AccountsIntoTempTableWithOutput()
        {
            SqlBuilder builder = SqlBuilder.Select(2000)
                .From("Account")
                .Into("tempPerson")
                .Columns("AccountID","Name","Address1")
                .ConcatColumns("Address","\r\n","Address1","PostalCode","City")
                .OrderBy("AccountID", OrderByDirections.Desc)
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResultTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }
        [TestMethod]
        public void SelectAllColumnsFromOneTableOrderedByTwoFields()
        {
            
            SqlBuilder builder = SqlBuilder.Select(2000)
                .From("Account")
                .AllColumns()
                .OrderBy("City", OrderByDirections.Desc).OrderBy("Name", OrderByDirections.Asc)
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            foreach (dynamic row in result.Take(10))
            {
                Console.WriteLine("{0} {1} - Ordered by City DESC, Name ASC", row.Name, row.City);
            }
        }


        [TestMethod]
        public void SelectValueColumnFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select().From("account", null).AllColumns()
                .Column<int>(22,"Age")
                .Column<string>(Guid.NewGuid().ToString(),"UniqueID")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Account> list = builder.List<Account>(null, 30, true, true);
            Console.WriteLine("List<Account> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void SelectFunctionColumnsFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select(20).From("account", null).AllColumns()
                .Fn()
                    .GetDate("Today")
                    .Concat("My Name",
                        ConstantField<string>.Constant("Michael"),
                        ConstantField<string>.Constant(" "),
                        ConstantField<string>.Constant("Randrup")
                        )
                .ToTable()
                .Column<string>(Guid.NewGuid().ToString(), "UniqueID")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Account> list = builder.List<Account>(null, 30, true, true);
            Console.WriteLine("List<Account> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }


        [TestMethod]
        public void SelectAllAccountsIntoCustomDictionary()
        {
            SqlBuilder builder = SqlBuilder.Select().From("account").AllColumns()
                    .InnerJoin("State", null).On("StateID", SqlOperators.Equal, "StateID")
                    .ToTable()
                    .Column("Description","State")
                .From("account")
                    .InnerJoin("Checkkode", null).On("DatasourceID", SqlOperators.Equal, "CheckID")
                    .And<decimal>("CheckGroup", SqlOperators.Equal,7).ToTable()
                    .Column("BeskrivelseDK","Datasource")
            .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            MyDictionary result = builder.Dictionary<decimal, Account, MyDictionary>("AccountID");
            Console.WriteLine("MyDictionary<int, Account> with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            result = null;
        }

        [TestMethod]
        public void SelectAccountsWithCustomAliasesIntoResultTable()
        {

            SqlBuilder builder = SqlBuilder.Select().From("account", "a").AllColumns()
                    .InnerJoin("State", "b").On("StateID", SqlOperators.Equal, "StateID")
                    .ToTable()
                    .Column("Description", "State")
                .From("a")
                    .InnerJoin("Checkkode", "c").On("DatasourceID", SqlOperators.Equal, "CheckID")
                    .And<decimal>("CheckGroup", SqlOperators.Equal, 7).ToTable()
                    .Column("BeskrivelseDK", "Datasource")
            .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute();
            Console.WriteLine("ResultTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            result = null;
        }



        public class MyDictionary : Dictionary<decimal, Account>
        {

        }




    }
}
