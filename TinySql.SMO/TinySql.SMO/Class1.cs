using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TinySql.Metadata
{
    public static class MetadataExtensions
    {

        public static SqlBuilder ToBuilder(this SqlMetadataDatabase db)
        {
            return db.builder;
        }

        //public static Table Columns(this SqlMetadataDatabase db)
        //{
        //    // Table t = db.builder.From()
        //}
        

        public static void MetaData(this SqlBuilder Builder)
        {
            
            
            
            

        }
    }

    

}
