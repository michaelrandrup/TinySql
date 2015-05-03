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
    public class MetadataTests : BaseTest
    {
        //[TestMethod]
        //public void TestExpression()
        //{
        //    TableHelper<Account> helper = new TableHelper<Account>()
        //    {
        //        Model = new Person() { City = "Hvidovre" },
        //        table = new Table()
        //    };

        //    TableHelper<Account> t;
        //    Table tt = t.Property(x => x.City);

        //    helper.Property(x => x.City);


        //    SqlBuilder b = SqlBuilder.Select()
        //        .From("account", null, null)
        //        //.Property<Account>(x => x.City == "")
        //        .Builder;

        //}

        [TestMethod]
        public void ExecuteAndSerialize()
        {
            Guid g = StopWatch.Start();
            SqlBuilder sb = SqlBuilder.Select(5)
                .From("Account")
                .Columns("AccountID","Name","Address1","PostalCode","City")
                .Builder;
            Console.WriteLine(sb.ToSql());
            ResultTable result = sb.Execute();
            string s = Serialization.ToJson<ResultTable>(result,true);
            Console.WriteLine("{0} rows executed and serialized  in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            result = Serialization.FromJson<ResultTable>(s);
            Console.WriteLine("{0} rows de-serialized  in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Console.WriteLine(s);
            
                
        }

        [TestMethod]
        public void GenerateMetadataTwoTimesUsingCaching()
        {
            Guid g = StopWatch.Start();
            SqlBuilder b1 = SqlBuilder.Select().WithMetadata();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Seconds, "Metadata Pass 1/2: Metadata serialized to cache and SqlBuilder ready in {0}s"));
            g = StopWatch.Start();
            SqlBuilder b2 = SqlBuilder.Select().WithMetadata();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Metadata Pass 2/2: Metadata serialized from cache and SqlBuilder ready in {0}ms"));
            Console.WriteLine("Removing Metadata from cache");
            bool b = SqlMetadataDatabase.FromBuilder(b2).ClearMetadata();
            Assert.IsTrue(b1.Metadata != null);
            Assert.IsTrue(b2.Metadata != null);
            Assert.IsTrue(b);
        }

        [TestMethod]
        public void GenerateMetadataForDatabase()
        {
            Guid g = StopWatch.Start();
            SqlMetadataDatabase meta = SqlMetadataDatabase.FromConnection(SqlBuilder.DefaultConnection);
            MetadataDatabase mdb = meta.BuildMetadata();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Seconds, "Metadata generated in {0}s"));
            Console.WriteLine("Database contains {0} tables and a total of {1} columns", mdb.Tables.Count, mdb.Tables.Values.SelectMany(x => x.Columns).Count());
            string FileName = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.GetRandomFileName() + ".json");
            g = StopWatch.Start();
            Serialization.ToFile(FileName, mdb);
            Console.WriteLine("Metadata persisted as {0} in {1}ms",FileName, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            mdb = Serialization.FromFile(FileName);
            Console.WriteLine("Metadata read from file '{0}' in {1}ms", FileName, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }


        //[TestMethod]
        //public void GenerateMetadataForDatabase_PWPLATINUM()
        //{
        //    Guid g = StopWatch.Start();
        //    SqlMetadataDatabase meta = SqlMetadataDatabase.FromConnection("Server=APPL-DB;Database=PWPLATINUM_NEWLAYOUT;Integrated Security=SSPI;");
        //    MetadataDatabase mdb = meta.BuildMetadata();
        //    Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Seconds, "Metadata generated in {0}s"));
        //    Console.WriteLine("Database contains {0} tables and a total of {1} columns", mdb.Tables.Count, mdb.Tables.SelectMany(x => x.Columns).Count());
        //    string FileName = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.GetRandomFileName() + ".xml");
        //    g = StopWatch.Start();
        //    mdb.ToFile(FileName);
        //    Console.WriteLine("Metadata persisted as {0} in {1}ms", FileName, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        //    g = StopWatch.Start();
        //    mdb = MetadataDatabase.FromFile(FileName);
        //    Console.WriteLine("Metadata read from file '{0}' in {1}ms", FileName, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        //}

        //[TestMethod]
        //public void SelectAllShoppersAndCustomers_PWPLATINUM()
        //{
        //    Guid g;
        //    SqlBuilder b1 = SqlBuilder.Select()
        //        .From("Shopper")
        //        .AllColumns()
        //        .InnerJoin("Customer").On("CustomerID", SqlOperators.Equal, "CustomerID")
        //        .ToTable().AllColumns()
        //        .Builder;

        //    //g = StopWatch.Start();
        //    //ResultTable result = b1.Execute("Server=APPL-DB;Database=PWPLATINUM_NEWLAYOUT;Integrated Security=SSPI;",240);
        //    //Console.WriteLine("{0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));

        //    g = StopWatch.Start();
        //    Dictionary<string, Shopper> shopperDictionary = b1.Dictionary<string, Shopper>("ShopperReference", "Server=APPL-DB;Database=PWPLATINUM_NEWLAYOUT;Integrated Security=SSPI;", 240);
        //    Console.WriteLine("{0} rows executed as Dictionary<string,Shopper> in {1}s", shopperDictionary.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));

        //}

    }
}
