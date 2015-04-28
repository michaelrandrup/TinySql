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
            SqlBuilder builder = SqlBuilder.Select().From("Person", null, "Person").AllColumns().Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Person> list = builder.List<Person>(null, 30, true, true);
            Console.WriteLine("List<Person> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void SelectValueColumnFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select().From("Person", null, "Person").AllColumns()
                .Column<int>(22,"Age")
                .Column<string>(Guid.NewGuid().ToString(),"UniqueID")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Person> list = builder.List<Person>(null, 30, true, true);
            Console.WriteLine("List<Person> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void SelectFunctionColumnsFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select(20).From("Person", null, "Person").AllColumns()
                .Fn()
                    .GetDate("Today")
                    .Concat("My Name",
                        ConstantFunction<string>.Constant("Michael"),
                        ConstantFunction<string>.Constant(" "),
                        ConstantFunction<string>.Constant("Randrup")
                        )
                .ToTable()
                .Column<string>(Guid.NewGuid().ToString(), "UniqueID")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Person> list = builder.List<Person>(null, 30, true, true);
            Console.WriteLine("List<Person> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }


        [TestMethod]
        public void SelectAllPersonsIntoCustomDictionary()
        {
            SqlBuilder builder = SqlBuilder.Select().From("Person", null, "Person").AllColumns()
                    .InnerJoin("EmailAddress", null, "Person").On("BusinessEntityID", SqlOperators.Equal, "BusinessEntityID")
                    .ToTable()
                    .Column("EmailAddress")
                .From("Person")
                    .InnerJoin("BusinessEntityAddress", null, "Person").On("BusinessEntityID", SqlOperators.Equal, "BusinessEntityID")
                    .And<int>("AddressTypeID", SqlOperators.Equal,2).ToTable()
                    .InnerJoin("Address", null, "Person").On("AddressID", SqlOperators.Equal, "AddressID")
                    .ToTable()
                    .Columns("AddressLine1", "AddressLine2", "City", "PostalCode")
            .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            MyDictionary result = builder.Dictionary<int, Person, MyDictionary>("BusinessEntityID");
            Console.WriteLine("MyDictionary<int, Person> with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            result = null;
        }

        [TestMethod]
        public void SelectPersonsWithCustomAliasesIntoResultTable()
        {
            
            SqlBuilder builder = SqlBuilder.Select().From("Person", "a", "Person").AllColumns()
                    .InnerJoin("EmailAddress", "b", "Person").On("BusinessEntityID", SqlOperators.Equal, "BusinessEntityID")
                    .ToTable()
                    .Column("EmailAddress")
                .From("a")
                    .InnerJoin("BusinessEntityAddress", "c", "Person").On("BusinessEntityID", SqlOperators.Equal, "BusinessEntityID")
                    .And<int>("AddressTypeID", SqlOperators.Equal, 2).ToTable()
                    .InnerJoin("Address", "d", "Person").On("AddressID", SqlOperators.Equal, "AddressID")
                    .ToTable()
                    .Columns("AddressLine1", "AddressLine2", "City", "PostalCode")
            .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute();
            Console.WriteLine("ResultTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            result = null;
        }



        public class MyDictionary : Dictionary<int, Person>
        {

        }




    }
}
