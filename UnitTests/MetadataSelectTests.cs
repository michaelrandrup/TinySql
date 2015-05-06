using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using TinySql.Serialization;
using System.Linq;

namespace UnitTests
{
    [TestClass]
    public class MetadataSelectTests : BaseTest
    {
        [TestMethod]
        public void CrossJoinAccountsAndContactsWithMetadata()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select(100000)
                .From("Account")
                .AllColumns()
                .WithMetadata().CrossJoin("Contact", null)
                .AllColumns()
                .Builder();
            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute(30, false);
            Console.WriteLine("{0} rows selected in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Console.WriteLine(SerializationExtensions.ToJson<dynamic>(result.First(), true));
        }

        [TestMethod]
        public void JoinAccountsAndContactsWithMetadata()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .AllColumns()
                .WithMetadata().InnerJoin("Contact", null)
                .AllColumns()
                .Builder();
            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute(30, false);
            Console.WriteLine("{0} rows selected in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Console.WriteLine(SerializationExtensions.ToJson<dynamic>(result.First(), true));
        }

        [TestMethod]
        public void JoinContactsAndAccountsWithMetadata()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select()
                .From("Contact")
                .AllColumns()
                .WithMetadata().InnerJoin("AccountID")
                .AllColumns()
                .Builder();

            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute(30, false);
            Console.WriteLine("{0} rows selected in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Console.WriteLine(SerializationExtensions.ToJson<dynamic>(result.First(), true));
        }

        [TestMethod]
        public void SimpleSelectTop400Accounts()
        {
            Guid g = StopWatch.Start();
            Guid g2 = StopWatch.Start();
            TableHelper<Account> t = new TableHelper<Account>();
            SqlBuilder builder = SqlBuilder.Select(400).WithMetadata(true, SetupData.MetadataFileName)
                .From<Account>()
                .Column(c => c.AccountID)
                .Column(c => c.Address1)
                .Column(c => c.Address2)
                .Column(c => c.Address3)
                .Column(c => c.PostalCode)
                .Model.Builder();
            Console.WriteLine(builder.ToSql());
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Model built in {0}ms"));
            ResultTable result = builder.Execute();
            Console.WriteLine("{0} rows executed in {1}ms", result.Count, StopWatch.Stop(g2, StopWatch.WatchTypes.Milliseconds));
            Assert.IsTrue(result.Count == 400);
        }

        //[TestMethod]
        //public void SimpleSelectWithJoin()
        //{
        //    Guid g = StopWatch.Start();
        //    Guid g2 = StopWatch.Start();
        //    TableHelper<Account> t = new TableHelper<Account>();
        //    SqlBuilder builder = SqlBuilder.Select(15000).WithMetadata(true, SetupData.MetadataFileName)
        //        .From<Account>(null)
        //        .Column(c => c.AccountID)
        //        .Column(c => c.Name)
        //        .Column(c => c.Address1)
        //        .Column(c => c.ModifiedOn)
        //        .InnerJoin(c => c.StateID).ToTable
        //        .Column(c => c.State)
        //        .InnerJoin("BusinessEntityAddress","account").ToTable
        //        .InnerJoin("Address","account").FromTable
        //        .Column(c => c.PostalCode)

        //        .Model.Builder;
        //    Console.WriteLine(builder.ToSql());
        //    Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Model built in {0}ms"));
        //    ResultTable result = builder.Execute();
        //    Console.WriteLine("{0} rows executed in {1}ms", result.Count, StopWatch.Stop(g2, StopWatch.WatchTypes.Milliseconds));
        //    Assert.IsTrue(result.Count == 15000);



        //}
    }
}
