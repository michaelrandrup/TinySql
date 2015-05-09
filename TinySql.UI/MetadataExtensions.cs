using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TinySql.Metadata;

namespace TinySql.UI
{
    public static class StringMapExtensions
    {
        

    }

    public static class MetadataExtensions
    {
        public static string[] TitleColumns = new string[] { "name", "title", "description", "fullname" };
        public static string GuessTitleColumn(this MetadataTable Table)
        {
            foreach (string s in TitleColumns)
            {
                MetadataColumn mc = Table.Columns.Values.First(x => x.Name.Equals("name", StringComparison.OrdinalIgnoreCase));
                if (mc != null) { return mc.Name; }
            }
            return Table.PrimaryKey.Columns.First().Name;
        }

        public static string GetDisplayName(this MetadataColumn mc, string TableName,int? LCID = null)
        {
            int lcid = LCID.HasValue ? LCID.Value : SqlBuilder.DefaultCulture.LCID;
            return StringMap.Default.GetText(lcid, TableName + mc.Name, mc.Name);
        }
    }
}
