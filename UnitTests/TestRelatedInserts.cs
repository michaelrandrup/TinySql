using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using TinySql.Metadata;
using System.Data;
using System.Linq;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class TestRelatedInserts : BaseTest
    {
        [TestMethod]
        public void InsertAccountAndRelatedContact()
        {
            SqlBuilder max = SqlBuilder.Select(1)
                .From("Account")
                .Column("AccountID")
                .OrderBy("AccountID", OrderByDirections.Desc)
                .Builder;
            decimal Accountid = max.Execute().First().Column<decimal>("AccountID");

            List<SqlBuilder> builders = new List<SqlBuilder>() {
            SqlBuilder.Insert()
                .Into("Account")
                .Value("Name", "Account " + DateTime.Now.ToString(), System.Data.SqlDbType.VarChar)
                .Value("AccountTypeID", 1, System.Data.SqlDbType.VarChar)
                .Value("DataSourceID", 1, System.Data.SqlDbType.VarChar)
                .Value("StateID", 1, System.Data.SqlDbType.VarChar)
                .Value("CreatedBy", 2, System.Data.SqlDbType.VarChar)
                .Value("CreatedOn", DateTime.Now, System.Data.SqlDbType.VarChar)
                .Value("ModifiedBy", 2, System.Data.SqlDbType.VarChar)
                .Value("ModifiedOn", DateTime.Now, System.Data.SqlDbType.VarChar)
                .Value("OwningUserID", 2, System.Data.SqlDbType.VarChar)
                .Value("OwningBusinessUnitID", 2, System.Data.SqlDbType.VarChar)
                    .Output().PrimaryKey()
                .Builder()
            };

            for (int i = 0; i < 10; i++)
			{
			    builders.Add(
                     SqlBuilder.Insert()
                    .Into("Contact")
                    .Value("Name","Contact " + DateTime.Now.ToString(), SqlDbType.VarChar)
                    .Value("AccountID",0, SqlDbType.VarChar)
                    .Value("JobfunctionID",1, SqlDbType.VarChar)
                    .Value("JobpositionID", 1, SqlDbType.VarChar)
                    .Value("DataSourceID", 1, System.Data.SqlDbType.VarChar)
                    .Value("StateID", 1, System.Data.SqlDbType.VarChar)
                    .Value("CreatedBy", 2, System.Data.SqlDbType.VarChar)
                    .Value("CreatedOn", DateTime.Now, System.Data.SqlDbType.VarChar)
                    .Value("ModifiedBy", 2, System.Data.SqlDbType.VarChar)
                    .Value("ModifiedOn", DateTime.Now, System.Data.SqlDbType.VarChar)
                    .Value("OwningUserID", 2, System.Data.SqlDbType.VarChar)
                    .Value("OwningBusinessUnitID", 2, System.Data.SqlDbType.VarChar)
                        .Output().PrimaryKey()
                    .Builder()
                    );
			}

            ResultTable results = builders.Execute();
            Assert.IsTrue(results.Count == 2);

            SqlBuilder builder = SqlBuilder.Select()
                .From("Account").AllColumns()
                .Where("Account", "AccountID", SqlOperators.Equal, results.First().Column("AccountID"))
                .Builder.BaseTable()
                .SubSelect("Contact")
                .AllColumns()
                .Builder();

            ResultTable account = builder.Execute();
            dynamic row = account.First();
            ResultTable contacts = (row.ContactList as ResultTable);
            Console.WriteLine("Account ID {0} with name: {1} and a total of {2} contacts", row.AccountID, row.Name, contacts.Count);
            foreach (dynamic contact in contacts)
            {
                Console.WriteLine("Related contact ID {0} with name: {1}", contact.ContactID, contact.Name);
            }
            Console.WriteLine("\r\nDeleting accounts > {0}", Accountid);

            builder = SqlBuilder.Delete()
                .From("Account")
                .Where<decimal>("Account", "AccountID", SqlOperators.GreaterThan, Accountid)
                .Builder();

            Assert.IsTrue(builder.ExecuteNonQuery() == 1);

            Console.WriteLine("Account ID {0} deleted", row.AccountID);
        }
    }
}
