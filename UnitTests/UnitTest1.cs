using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Xml;
using System.Linq;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class SqlSelectTests
    {
        public SqlSelectTests()
        {
            Assert.AreEqual<bool>(SetupData.Setup(), true);
        }
        
        [TestMethod]
        public void SelectResultsFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select().From("Person",null, "Person").AllColumns().Builder;
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("ResulTable with {0} rows executed in {0}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Person> list = builder.List<Person>(null, 30, true, true);
            Console.WriteLine("List<Person> with {0} rows executed in {0}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void UpdatePersonWithOutputResults()
        {
            string NewTitle = Guid.NewGuid().ToString();
            SqlBuilder builder = SqlBuilder.Update()
                .Table("Person", "Person")
                    .OutputColumn("BusinessEntityID")
                    .OutputColumn("Title")
                .Set<string>("Title", System.Data.SqlDbType.VarChar, NewTitle)
                .Where<int>("Person", "BusinessEntityID", SqlOperators.Equal, 1)
                .Builder;

            Guid g = StopWatch.Start();
            List<Person> Persons = builder.List<Person>();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One person updated and retrieved as List<T> in {0}ms"));
            Assert.IsTrue(Persons.Count == 1);
            Assert.AreEqual<string>(NewTitle, Persons.First().Title);
            

        }

        private class Person
        {
            public string FirstName;
            private string MiddleName { get; set; }
            public string Title { get; set; }
            public string LastName;
            public XmlDocument Demographics;
            public DateTime ModifiedDate { get; set; }
        }
    }
}
