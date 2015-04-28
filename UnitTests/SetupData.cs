using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using TinySql;

namespace UnitTests
{
    public static class StopWatch
    {
        public enum WatchTypes
        {
            Milliseconds,
            Seconds,
            Minues
        }
        private static Dictionary<Guid, DateTime> Watches = new Dictionary<Guid, DateTime>();
        public static Guid Start()
        {
            DateTime dt = DateTime.Now;
            Guid g = Guid.NewGuid();
            Watches.Add(g, dt);
            return g;
        }
        public static double Stop(Guid g,WatchTypes WatchType = WatchTypes.Milliseconds)
        {
            DateTime dt = DateTime.Now;
            DateTime st = Watches[g];
            Watches.Remove(g);
            switch (WatchType)
            {
                case WatchTypes.Seconds:
                    return (dt - st).Seconds;
                case WatchTypes.Minues:
                    return (dt - st).Minutes;
            }
            return (dt - st).TotalMilliseconds;
        }
        public static string Stop(Guid g, WatchTypes WatchType = WatchTypes.Milliseconds, string Format = "{0}")
        {
            DateTime dt = DateTime.Now;
            DateTime st = Watches[g];
            Watches.Remove(g);
            double d = (dt - st).TotalMilliseconds;
            switch (WatchType)
            {
                case WatchTypes.Seconds:
                    d = (dt - st).Seconds;
                    break;
                case WatchTypes.Minues:
                    d =  (dt - st).Minutes;
                    break;
            }
            return string.Format(Format, d);
        }
    }
    public static class SetupData
    {
        /// <summary>
        /// Requires the Adventure Works Sample Database for SQL 2014: https://msftdbprodsamples.codeplex.com/releases/view/125550
        /// </summary>
        /// <returns></returns>
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
