using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using Microsoft.CSharp;
using TinySql;
using TinySql.Metadata;

namespace UnitTests
{
    [TestClass]
    public class IfElseTests : BaseTest
    {
        [TestMethod]
        public void TestSimpleIf()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.If()
                .Conditions.AndExists("Account")
                .And<decimal>("Account", "AccountID", SqlOperators.Equal, 543)
                .Begin(SqlBuilder.StatementTypes.Select)
                .From("Account")
                .AllColumns()
                .Where<decimal>("Account", "AccountID", SqlOperators.Equal, 543)
                .Builder.End();

            Console.WriteLine(builder.ToSql());

            ResultTable result = builder.Execute();

            Console.WriteLine(string.Format("{0} rows retrieved in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds)));
            Assert.IsTrue(result.Count == 1);

        }

        private ResultTable InsertOrUpdate(decimal Id)
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.If()
                .Conditions.AndExists("Account")
                .And<decimal>("Account", "AccountID", SqlOperators.Equal, Id)
                .Begin(SqlBuilder.StatementTypes.Update).Table("Account")
                    .Output()
                        .Column("AccountID", System.Data.SqlDbType.Decimal)
                    .UpdateTable()
                    .Set<string>("Name", System.Data.SqlDbType.VarChar,"Name Updated " + DateTime.Now.ToString(),200)
                    .Where<decimal>("Account","AccountID", SqlOperators.Equal, Id)
                .Builder.End()
                .Else().Begin(SqlBuilder.StatementTypes.Insert).Into("Account")
                    .Value<string>("Name", System.Data.SqlDbType.VarChar, "Test Account")
                    .Value<string>("Address1", System.Data.SqlDbType.VarChar, "Address1")
                    .Value<string>("Address2", System.Data.SqlDbType.VarChar, "Address2")
                    .Value<string>("Address3", System.Data.SqlDbType.VarChar, "Address3")
                    .Value<string>("PostalCode", System.Data.SqlDbType.VarChar, "1165")
                    .Value<string>("City", System.Data.SqlDbType.VarChar, "City")
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
                .Builder.End();

            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Console.WriteLine(string.Format("{0} rows inserted or updated in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds)));
            Assert.IsTrue(result.Count == 1);
            return result;
        }

        [TestMethod]
        public void TestIfElseInsertUpdate()
        {
            decimal Id = 0;
            Console.WriteLine("Pass 1/2: Inserting Account ID {0}", Id);
            ResultTable result = InsertOrUpdate(Id);
            Console.WriteLine("{0} rows returned", result.Count);
            Assert.IsTrue(result.Count == 1);
            Id = result.First().Column<decimal>("AccountID");
            Console.WriteLine("Updating returned Account ID {0}", Id);
            result = InsertOrUpdate(Id);
            Assert.IsTrue(result.Count == 1);
            decimal Id2 = result.First().Column<decimal>("AccountID");
            Console.WriteLine("Pass 2/2: Account ID {0} returned", Id);
            Assert.IsTrue(Id.Equals(Id2));
        }
    }
}
