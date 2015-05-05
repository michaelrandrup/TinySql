using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Collections.Generic;
using Microsoft.CSharp;

namespace UnitTests
{
    [TestClass]
    public class SqlInsertTests : BaseTest
    {
        static decimal FirstInsertedId = 99999999;
        [TestMethod]
        public void InsertOneAccount()
        {
            Assert.IsTrue(InsertOneAccountInternal() == 1);
        }
        
        public int InsertOneAccountInternal(bool WriteSql = true)
        {
            Guid g = StopWatch.Start();

            string id = DateTime.Now.Ticks.ToString();
            SqlBuilder builder = SqlBuilder.Insert()
                .Into("account")
                .Value<string>("Name", System.Data.SqlDbType.VarChar, "Test Account " + id)
                .Value<string>("Address1", System.Data.SqlDbType.VarChar, "Address1 " + id)
                .Value<string>("Address2", System.Data.SqlDbType.VarChar, "Address2 " + id)
                .Value<string>("Address3", System.Data.SqlDbType.VarChar, "Address3 " + id)
                .Value<string>("PostalCode", System.Data.SqlDbType.VarChar, "1165")
                .Value<string>("City", System.Data.SqlDbType.VarChar, "City " + id)
                .Value<string>("Telephone", System.Data.SqlDbType.VarChar, "500-500-2015")
                .Value<string>("Telefax", System.Data.SqlDbType.VarChar, "500-500-2015")
                .Value<string>("Web", System.Data.SqlDbType.VarChar, "http://www.company.com")
                .Value<decimal>("AccountTypeID", System.Data.SqlDbType.Decimal, 1)
                .Value<decimal>("DataSourceID", System.Data.SqlDbType.Decimal, 1)
                .Value<decimal>("StateID", System.Data.SqlDbType.Decimal, 1)
                .Value<decimal>("CreatedBy", System.Data.SqlDbType.Decimal, 1)
                .Value<decimal>("ModifiedBy", System.Data.SqlDbType.Decimal, 1)
                .Value<DateTime>("CreatedOn", System.Data.SqlDbType.DateTime, DateTime.Now)
                .Value<DateTime>("ModifiedOn", System.Data.SqlDbType.DateTime, DateTime.Now)
                .Value<decimal>("OwningUserID", System.Data.SqlDbType.Decimal, 1)
                .Value<decimal>("OwningBusinessUnitID", System.Data.SqlDbType.Decimal, 1)
                .Output()
                    .Column("AccountID", System.Data.SqlDbType.Decimal)
                .Builder;
            if (WriteSql)
            {
                Console.WriteLine(builder.ToSql());
            }
            g = StopWatch.Start();
            ResultTable result = builder.Execute();
            if (FirstInsertedId == 99999999)
            {
                FirstInsertedId = result.First().Column<decimal>("AccountID");
            }
            if (WriteSql)
            {
                Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One account inserted in {0}ms"));
            }
            Assert.IsTrue(result.Count == 1);
            return result.Count;
        }

        [TestMethod]
        public void Insert1000Accounts()
        {
            int num = 0;
            Guid g = StopWatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                num += InsertOneAccountInternal(i < 1);
            }
            Console.WriteLine("{0} accounts inserted in {1:0.00}s", num, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            Assert.IsTrue(num == 1000);
            DeleteInsertedAccounts();
        }

        [TestMethod]
        public void Insert1000AccountsInOneBatch()
        {
            Guid g = StopWatch.Start();
            List<SqlBuilder> Builders = new List<SqlBuilder>();

            for (int i = 0; i < 1000; i++)
            {
                string id = DateTime.Now.Ticks.ToString();
                SqlBuilder builder = SqlBuilder.Insert()
                    .Into("account")
                    .Value<string>("Name", System.Data.SqlDbType.VarChar, "Test Account " + id)
                    .Value<string>("Address1", System.Data.SqlDbType.VarChar, "Address1 " + id)
                    .Value<string>("Address2", System.Data.SqlDbType.VarChar, "Address2 " + id)
                    .Value<string>("Address3", System.Data.SqlDbType.VarChar, "Address3 " + id)
                    .Value<string>("PostalCode", System.Data.SqlDbType.VarChar, "1165")
                    .Value<string>("City", System.Data.SqlDbType.VarChar, "City " + id)
                    .Value<string>("Telephone", System.Data.SqlDbType.VarChar, "500-500-2015")
                    .Value<string>("Telefax", System.Data.SqlDbType.VarChar, "500-500-2015")
                    .Value<string>("Web", System.Data.SqlDbType.VarChar, "http://www.company.com")
                    .Value<decimal>("AccountTypeID", System.Data.SqlDbType.Decimal, 1)
                    .Value<decimal>("DataSourceID", System.Data.SqlDbType.Decimal, 1)
                    .Value<decimal>("StateID", System.Data.SqlDbType.Decimal, 1)
                    .Value<decimal>("CreatedBy", System.Data.SqlDbType.Decimal, 1)
                    .Value<decimal>("ModifiedBy", System.Data.SqlDbType.Decimal, 1)
                    .Value<DateTime>("CreatedOn", System.Data.SqlDbType.DateTime, DateTime.Now)
                    .Value<DateTime>("ModifiedOn", System.Data.SqlDbType.DateTime, DateTime.Now)
                    .Value<decimal>("OwningUserID", System.Data.SqlDbType.Decimal, 1)
                    .Value<decimal>("OwningBusinessUnitID", System.Data.SqlDbType.Decimal, 1)
                    .Output()
                        .Column("AccountID", System.Data.SqlDbType.Decimal)
                    .Builder;
                Builders.Add(builder);
            }
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "1000 Builders created in {0}ms"));
            g = StopWatch.Start();
            int result = Builders.ToArray().ExecuteNonQuery();
            Console.WriteLine("{0} Rows affected in {1}ms", result, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Assert.IsTrue(result == 1000);
        }




        public void DeleteInsertedAccounts()
        {
            SqlBuilder b1 = SqlBuilder.Delete()
                .From("account", null)
                .Where<decimal>("account", "AccountID", SqlOperators.GreaterThanEqual, FirstInsertedId)
                .Builder;
            Console.WriteLine(b1.ToSql());
            Guid g = StopWatch.Start();

            int i = new SqlBuilder[] { b1 }.ExecuteNonQuery();
            Console.WriteLine("{0} Accounts deleted", i);
            Assert.IsTrue(i >= 1000);
            FirstInsertedId = 99999999;
        }

    }
}
