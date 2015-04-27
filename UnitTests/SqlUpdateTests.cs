using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class SqlUpdateTests : BaseTest
    {
        [TestMethod]
        public void UpdatePersonWithOutputResults()
        {
            string NewTitle = Guid.NewGuid().ToString().Substring(0, 8);
            SqlBuilder builder = SqlBuilder.Update()
                .Table("Person", "Person")
                    .Output()
                    .Column("BusinessEntityID", System.Data.SqlDbType.Int)
                    .Column("Title", System.Data.SqlDbType.VarChar, 8)
                .UpdateTable()
                .Set<string>("Title", System.Data.SqlDbType.VarChar, NewTitle)
                .Where<int>("Person", "BusinessEntityID", SqlOperators.Equal, 1)

                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            List<Person> Persons = builder.List<Person>();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One person updated and retrieved as List<T> in {0}ms"));
            Assert.IsTrue(Persons.Count == 1);
            Assert.AreEqual<string>(NewTitle, Persons.First().Title);
        }

        [TestMethod]
        public void UpdatePersonWithJoinAndOutputResults()
        {
            string NewTitle = Guid.NewGuid().ToString().Substring(0, 8);
            SqlBuilder builder = SqlBuilder.Update()
                .Table("Person", "Person")
                    .Output()
                    .Column("BusinessEntityID", System.Data.SqlDbType.Int)
                    .Column("Title", System.Data.SqlDbType.VarChar, 8)
                .UpdateTable()
                .Set<string>("Title", System.Data.SqlDbType.VarChar, NewTitle)
                .InnerJoin("BusinessEntity",null,"Person").On("BusinessEntityID", SqlOperators.Equal, "BusinessEntityID")
                .ToTable()
                .Where<int>("BusinessEntity", "BusinessEntityID", SqlOperators.Equal, 1)

                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            List<Person> Persons = builder.List<Person>();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One person updated and retrieved as List<T> in {0}ms"));
            Assert.IsTrue(Persons.Count == 1);
            Assert.AreEqual<string>(NewTitle, Persons.First().Title);
        }

    }
}
