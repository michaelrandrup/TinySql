using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Xml;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            SetupData.Setup();
            Assert.AreEqual<bool>(SetupData.Setup(),true);
        }

        [TestMethod]
        public void SimpleSql1()
        {
            SetupData.Setup();
            DateTime dtStart = DateTime.Now;
            SqlBuilder builder = SqlBuilder.Select().From("Person",null, "Person").AllColumns().Builder;

            ResultTable result = builder.Execute(null, 120);
            Console.WriteLine("Execution took: {0}s", (DateTime.Now - dtStart).TotalSeconds);
            dtStart = DateTime.Now;
            List<Person> list = builder.List<Person>(null, 30, true, true);
            Console.WriteLine("Execution took: {0}s", (DateTime.Now - dtStart).TotalSeconds);
            Console.WriteLine("{0} rows retrieved", result.Count);
        }

        private class Person
        {
            public string FirstName;
            private string MiddleName { get; set; }
            public string LastName;
            public XmlDocument Demographics;
            public DateTime ModifiedDate { get; set; }
        }
    }
}
