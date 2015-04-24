using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using TinySql;

namespace UnitTests
{
    public static class SetupData
    {
        public static bool Setup()
        {
            if (SqlBuilder.DefaultConnection != null)
            {
                return true;
            }
            // General options for SqlBuilder


            string dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            SqlBuilder.DefaultConnection = string.Format("Server=(localdb)\\ProjectsV12;Database=AdventureWorks2014;Trusted_Connection=Yes", dir);
            return true;
        }
    }
}
