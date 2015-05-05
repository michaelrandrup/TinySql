using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using Microsoft.CSharp;
using TinySql;
using TinySql.Metadata;
using System.IO;
using TinySql.Serialization;

namespace UnitTests
{
    [TestClass]
    public class RowDataTests : BaseTest
    {
        [TestMethod]
        public void SelectAndSerializeDataRows()
        {
            SqlBuilder builder = SqlBuilder.Select(100)
                .From("Account")
                .AllColumns(false)
                //.WhereNotExists("Contact").And("AccountID", SqlOperators.Equal, "AccountID")
                //.EndExists().Builder.BaseTable()
                .SubSelect("Contact","AccountID","AccountID","c")
                .AllColumns(false)
                .Builder();

            Console.WriteLine(builder.ToSql());

            ResultTable result = builder.Execute();
            string prefix = DateTime.Now.Ticks.ToString();
            string path = Path.GetTempPath();
            for (int i = 0; i < result.Count; i++)
            {
                SerializationExtensions.ToFile<RowData>(result[i], Path.Combine(path, string.Format("Row{0}@{1}.json", i + 1,prefix)));
            }

            ResultTable result2 = new ResultTable();
            for (int i = 0; i < result.Count; i++)
            {
                result2.Add(SerializationExtensions.FromFile<RowData>(Path.Combine(path, string.Format("Row{0}@{1}.json", i + 1, prefix))));
            }

            Assert.IsTrue(result.Count == result2.Count, "Number of rows does not match");

            result = new ResultTable(builder, 60, false);
            string prefix2 = DateTime.Now.Ticks.ToString();
            for (int i = 0; i < result.Count; i++)
            {
                SerializationExtensions.ToFile<RowData>(result[i], Path.Combine(path, string.Format("Row{0}@{1}.json", i + 1, prefix2)));
            }

            string[] files = Directory.EnumerateFiles(path, "Row*.json").ToArray();
            foreach (string file in files)
            {
                File.Delete(file);
            }

        }


    }
}
