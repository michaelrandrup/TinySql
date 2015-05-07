using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using TinySql.Serialization;
using System.Linq;
using TinySql.Classes;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class AttributeMetadataSelectTests : BaseTest
    {
        [TestMethod]
        public void SelectContactAndAccount()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select().WithMetadata<Contact>()
                .Column(x => x.ContactID)
                .Column(x => x.Name)
                .InnerJoin(x => x.AccountID)
                    .Column(x => x.AccountName, "Name")
                    .Column(x => x.Address1)
                    .Column(x => x.Address2)
                    .Column(x => x.Address3)
                    .Column(x => x.PostalCode)
                    .Column(x => x.City)
                .From<Contact>()
                .Column(x => x.CreatedOn)
                .Builder();
            

            List<Contact> result = builder.List<Contact>();
            Console.WriteLine("{0} contacts selected as List<T> in {1}ms\r\n\r\n", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            foreach (Contact c in result.Take(5))
            {
                Console.WriteLine("{0},{1} works with {1} @ {2}, {3} {4}", c.ContactID, c.Name, c.AccountName, c.Address1, c.PostalCode, c.City);
            }
            Console.WriteLine("");
            Console.WriteLine(builder.ToSql());
            

        }

        [TestMethod]
        public void SelectContactAndAccountAllColumns()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select().WithMetadata<Contact>()
                .AllColumns()
                .InnerJoin(x => x.AccountID)
                    .AllColumns()
                .Builder();

            List<Contact> result = builder.List<Contact>();
            Console.WriteLine("{0} contacts selected as List<T> in {1}ms\r\n\r\n", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            foreach (Contact c in result.Take(5))
            {
                Console.WriteLine("{0},{1} works with {1} @ {2}, {3} {4}", c.ContactID, c.Name, c.AccountName, c.Address1, c.PostalCode, c.City);
            }
            Console.WriteLine("");
            Console.WriteLine(builder.ToSql());


        }
    }
    

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

        //[TestMethod]
        //public void SimpleSelectTop400Accounts()
        //{
        //    Guid g = StopWatch.Start();
        //    Guid g2 = StopWatch.Start();
        //    TableHelper<Account> t = new TableHelper<Account>();
        //    SqlBuilder builder = SqlBuilder.Select(400).WithMetadata(true, SetupData.MetadataFileName)
        //        .From<Account>()
        //        .Column(c => c.AccountID)
        //        .Column(c => c.Address1)
        //        .Column(c => c.Address2)
        //        .Column(c => c.Address3)
        //        .Column(c => c.PostalCode)
        //        .Model.Builder();
        //    Console.WriteLine(builder.ToSql());
        //    Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Model built in {0}ms"));
        //    ResultTable result = builder.Execute();
        //    Console.WriteLine("{0} rows executed in {1}ms", result.Count, StopWatch.Stop(g2, StopWatch.WatchTypes.Milliseconds));
        //    Assert.IsTrue(result.Count == 400);
        //}

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
