using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace UnitTests
{
    public class BaseTest
    {
        public BaseTest()
        {
            Assert.AreEqual<bool>(SetupData.Setup(), true);
        }
    }

    public class Person
    {
        public int BusinessEntityID = 0;
        public string FirstName;
        private string MiddleName { get; set; }
        public string Title { get; set; }
        public string LastName;
        public XmlDocument Demographics;
        public DateTime ModifiedDate { get; set; }
        public string Email { get; set; }
        public string AddressLine1;
        public string AddressLine2;
        public string PostalCode;
        public string City;

    }
}
