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
        static int FirstInsertedId = 99999999;
        [TestMethod]
        public void InsertOnePerson()
        {
            Assert.IsTrue(InsertOnePersonInternal() == 1);
        }
        
        public int InsertOnePersonInternal(bool WriteSql = true)
        {
            Guid g = StopWatch.Start();

            SqlBuilder b1 = SqlBuilder.Insert().Into("BusinessEntity", "Person")
                .Value<DateTime>("ModifiedDate", System.Data.SqlDbType.DateTime, DateTime.Now)
                .Output()
                    .Column("BusinessEntityID", System.Data.SqlDbType.Int)
                .Builder;
            if (WriteSql)
            {
                Console.WriteLine(b1.ToSql());
            }
            ResultTable result = b1.Execute();
            dynamic row = result.First();
            int businessid = row.BusinessEntityID;
            if (FirstInsertedId == 99999999)
            {
                FirstInsertedId = businessid;
            }



            string id = DateTime.Now.Ticks.ToString();
            SqlBuilder builder = SqlBuilder.Insert()
                .Into("Person", "Person")
                .Value<int>("BusinessEntityID", System.Data.SqlDbType.Int,businessid)
                .Value<string>("PersonType", System.Data.SqlDbType.VarChar, "EM")
                .Value<bool>("NameStyle", System.Data.SqlDbType.Bit, false)
                .Value<string>("Title", System.Data.SqlDbType.VarChar, "Mr.", 8)
                .Value<string>("FirstName", System.Data.SqlDbType.VarChar, "First name " + id)
                .Value<string>("LastName", System.Data.SqlDbType.VarChar, "Last name " + id)
                .Value<int>("EmailPromotion", System.Data.SqlDbType.Int, 0)
                .Value<DateTime>("ModifiedDate", System.Data.SqlDbType.DateTime, DateTime.Now)
                .Output()
                    .Column("BusinessEntityID", System.Data.SqlDbType.Int)
                    .Column("FirstName", System.Data.SqlDbType.VarChar,50)
                    .Column("LastName", System.Data.SqlDbType.VarChar, 50)
                    .Column("Title", System.Data.SqlDbType.VarChar,8)
                    .Column("Demographics", System.Data.SqlDbType.Xml)
                    .Column("ModifiedDate", System.Data.SqlDbType.DateTime)
                .Builder;
            Console.WriteLine(builder.ToSql());
            g = StopWatch.Start();
            int i = new SqlBuilder[] { builder }.ExecuteNonQuery();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One person inserted in {0}ms"));
            Assert.IsTrue(i == 1);
            return i;
        }

        [TestMethod]
        public void Insert1000Persons()
        {
            int num = 0;
            Guid g = StopWatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                num += InsertOnePersonInternal(i < 10);
            }
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Seconds, "One person inserted in {0}s"));
            Assert.IsTrue(num == 1000);
        }

        [TestMethod]
        public void DeleteInsertedPersons()
        {
            SqlBuilder b1 = SqlBuilder.Delete()
                .From("Person", null, "Person")
                .Where<int>("Person", "BusinessEntityID", SqlOperators.GreaterThanEqual, FirstInsertedId)
                .Builder;
            Console.WriteLine(b1.ToSql());
            Guid g = StopWatch.Start();

            int i = new SqlBuilder[] { b1 }.ExecuteNonQuery();
            Assert.IsTrue(i == 1001);
        }

    }
}
