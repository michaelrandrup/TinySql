using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TinySql.Attributes
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class PK : Attribute
    {
        public PK()
        {

        }
    }

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class FK : Attribute
    {
        readonly string toTable;
        readonly string toSchema;
        readonly string foreignKeyName;
        readonly string toField;

        // This is a positional argument
        public FK(string toTable, string toField = null, string toSchema = null, string foreignKeyName = null)
        {
            this.toTable = toTable;
            this.toSchema = toSchema;
            this.foreignKeyName = foreignKeyName;
            this.toField = null;
        }

        public string ToField
        {
            get { return toField; }
        }
        public string ToTable
        {
            get
            {
                return toTable;
            }
        }
        public string ToSchema
        {
            get { return toSchema; }
        }

        public string ForeignKeyName
        {
            get
            {
                return foreignKeyName;
            }
        }
    }

   
}
