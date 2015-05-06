using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TinySql.Attributes
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class ForeignKey : Attribute
    {
        readonly string toTable;

        // This is a positional argument
        public ForeignKey(string toTable)
        {
            this.toTable = toTable;
        }

        public string ToTable
        {
            get
            {
                return toTable;
            }
        }
    }

   
}
