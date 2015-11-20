using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Xml.Serialization;

namespace TinySql.Metadata
{
    [Serializable]
    public class MetadataDatabase
    {
        public Guid ID = Guid.NewGuid();
        public string Name { get; set; }
        public string Server { get; set; }
        public SqlBuilder Builder { get; set; }

        public long Version { get; set; }

        public ConcurrentDictionary<string, MetadataTable> Tables = new ConcurrentDictionary<string, MetadataTable>();
        // public ConcurrentDictionary<int, MetadataForeignKey> ForeignKeys = new ConcurrentDictionary<int, MetadataForeignKey>();
        // public ConcurrentDictionary<string, List<int>> InversionKeys = new ConcurrentDictionary<string, List<int>>();


        public MetadataTable this[string TableName]
        {
            get
            {
                MetadataTable table;
                if (Tables.TryGetValue(TableName, out table))
                {
                    return table;
                }
                else
                {
                    return null;
                }
            }
        }

        public MetadataTable FindTable(string Name, StringComparison CompareOption = StringComparison.OrdinalIgnoreCase)
        {
            MetadataTable mt = null;
            string Schema = null;
            if (!Name.Contains('.'))
            {
                Schema = "dbo.";
            }
            if (Tables.TryGetValue(Schema + Name, out mt))
            {
                return mt;
            }
            string[] keys = Tables.Keys.Where(x => x.EndsWith(Name, CompareOption)).ToArray();
            if (keys.Length != 1)
            {
                return null;
            }
            return this[keys[0]];

        }


    }
    [Serializable]
    public class MetadataTable
    {
        public MetadataTable()
        {

        }
        public int ID { get; set; }
        //public MetadataDatabase Parent { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }

        public string Fullname
        {
            get
            {
                return (!string.IsNullOrEmpty(Schema) ? Schema + "." + Name : Name);
            }
        }

        

        public MetadataColumn this[string ColumnName]
        {
            get
            {
                MetadataColumn column;
                if (Columns.TryGetValue(ColumnName, out column))
                {
                    return column;
                }
                else
                {
                    return null;
                }
            }
        }

        private ConcurrentDictionary<string, MetadataColumn> _Columns = new ConcurrentDictionary<string, MetadataColumn>();

        public ConcurrentDictionary<string, MetadataColumn> Columns
        {
            get { return _Columns; }
            set { _Columns = value; }
        }


        #region Extended Properties

        private ConcurrentDictionary<int, string> _DisplayNames = new ConcurrentDictionary<int, string>();

        public ConcurrentDictionary<int, string> DisplayNames
        {
            get { return _DisplayNames; }
            set { _DisplayNames = value; }
        }

        public string DisplayName
        {
            get
            {
                return this.GetDisplayName(SqlBuilder.DefaultCulture.LCID);
            }
        }
        public string GetDisplayName(int LCID)
        {
            string value;
            if (_DisplayNames.TryGetValue(SqlBuilder.DefaultCulture.LCID, out value))
            {
                return value;
            }
            return this.Name;
        }

        private string _TitleColumn = null;
        public string TitleColumn
        {
            get { return _TitleColumn; }
            set { _TitleColumn = value; }
        }

        private ConcurrentDictionary<string, List<string>> _ListDefinitions = new ConcurrentDictionary<string,List<string>>();

        public ConcurrentDictionary<string, List<string>> ListDefinitions
        {
            get { return _ListDefinitions; }
            set { _ListDefinitions = value; }
        }
        



        #endregion

        public Key PrimaryKey
        {
            get
            {
                return Indexes.Values.FirstOrDefault(x => x.IsPrimaryKey == true);
            }
        }

        public IEnumerable<MetadataForeignKey> FindForeignKeys(MetadataColumn Column, string ReferencedTable = null)
        {
            foreach (MetadataForeignKey FK in this.ForeignKeys.Values)
            {
                if (FK.ColumnReferences.Select(x => x.Column).Any(x => x.Equals(Column)))
                {
                    if (ReferencedTable == null || FK.ReferencedTable.Equals(ReferencedTable, StringComparison.OrdinalIgnoreCase))
                    {
                        yield return FK;
                    }
                }
            }
        }

        public ConcurrentDictionary<string, MetadataForeignKey> ForeignKeys = new ConcurrentDictionary<string, MetadataForeignKey>();
        public ConcurrentDictionary<string, Key> Indexes = new ConcurrentDictionary<string, Key>();
    }
    [Serializable]
    public class MetadataForeignKey
    {
        public int ID { get; set; }

        public MetadataTable Parent { get; set; }
        public MetadataDatabase Database { get; set; }

        public string Name { get; set; }
        public List<MetadataColumnReference> ColumnReferences = new List<MetadataColumnReference>();
        public string ReferencedSchema { get; set; }
        public string ReferencedTable { get; set; }
        public string ReferencedKey { get; set; }

        private bool _IsVirtual = false;

        public bool IsVirtual
        {
            get { return _IsVirtual; }
            set { _IsVirtual = value; }
        }

    }

    [Serializable]
    public class MetadataColumnReference
    {
        //public MetadataForeignKey Parent { get; set; }
        //public MetadataDatabase Database { get; set; }

        public string Name { get; set; }

        public MetadataColumn Column { get; set; }

        public MetadataColumn ReferencedColumn { get; set; }
    }

    [Serializable]
    public class Key
    {
        public int ID { get; set; }

        public MetadataTable Parent { get; set; }
        public MetadataDatabase Database { get; set; }

        public string Name { get; set; }
        public List<MetadataColumn> Columns = new List<MetadataColumn>();
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }


    }

    public class ForeignKeyCollection : List<MetadataForeignKey> //  IEnumerable<MetadataForeignKey>
    {
        public void AddKey(MetadataForeignKey Value, string InversionKey)
        {
            this.Add(Value);
            List<int> keys = new List<int>(new int[] { Value.ID });
            //this.Database.InversionKeys.AddOrUpdate(InversionKey, keys, (key, existing) =>
            //{
            //    existing.Add(Value.ID);
            //    return existing;
            //});

        }

        public MetadataTable Parent { get; set; }
        public MetadataDatabase Database { get; set; }

        //public IEnumerator<MetadataForeignKey> GetEnumerator()
        //{
        //    foreach (int i in list)
        //    {
        //        MetadataForeignKey FK;
        //        if (this.Database.ForeignKeys.TryGetValue(i,out FK))
        //        {
        //            yield return FK;
        //        }
        //    }
        //}

        //IEnumerator IEnumerable.GetEnumerator()
        //{
        //    return GetEnumerator();
        //}
    }

    [Serializable]
    public class MetadataColumn
    {
        public int ID { get; set; }
        public MetadataTable Parent { get; set; }
        //public MetadataDatabase Database {get; set;}

        public string Name { get; set; }
        public string Collation { get; set; }
        public SqlDbType SqlDataType { get; set; }
        public int Length { get; set; }

        private int _Scale = -1;

        public int Scale
        {
            get { return _Scale; }
            set { _Scale = value; }
        }

        private int _Precision = -1;

        public int Precision
        {
            get { return _Precision; }
            set { _Precision = value; }
        }
        private string _DataTypeName;
        public string DataTypeName
        {
            get { return _DataTypeName; }
            set
            {
                _DataTypeName = value;
                if (_DataType == null)
                {
                    if (!string.IsNullOrEmpty(_DataTypeName))
                    {
                        _DataType = Type.GetType(_DataTypeName);
                    }
                    
                }
            }
        }

        private Type _DataType = null;
        [XmlIgnore]
        public Type DataType
        {
            get { return _DataType; }
            set
            {
                _DataType = value;
                DataTypeName = _DataType != null ? _DataType.FullName : "Virtual";
            }
        }

        public string Default { get; set; }
        public bool IsComputed { get; set; }
        public string ComputedText { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsRowGuid { get; set; }
        public long IdentitySeed { get; set; }
        public long IdentityIncrement { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsForeignKey { get; set; }
        public bool Nullable { get; set; }

        [XmlIgnore]
        [JsonIgnore]
        public bool IsReadOnly
        {
            get
            {
                return IsComputed || IsIdentity || IsRowGuid;
            }
        }


        #region Extended Properties
        
        private ConcurrentDictionary<int, string> _DisplayNames = new ConcurrentDictionary<int, string>();

        public ConcurrentDictionary<int, string> DisplayNames
        {
            get { return _DisplayNames; }
            set { _DisplayNames = value; }
        }

        public string DisplayName
        {
            get
            {
                return this.GetDisplayName(SqlBuilder.DefaultCulture.LCID);
            }
        }
        public string GetDisplayName(int LCID)
        {
            string value;
            if (_DisplayNames.TryGetValue(SqlBuilder.DefaultCulture.LCID, out value))
            {
                return value;
            }
            return this.Name;
        }

        private string[] _IncludeColumns = null;

        public string[] IncludeColumns
        {
            get { return _IncludeColumns; }
            set { _IncludeColumns = value; }
        }

        

        #endregion

        public void PopulateField<T>(T field) where T : class
        {
            Field f = field as Field;
            f.Name = this.Name;
            f.MaxLength = this.Length;
            f.Scale = this.Scale;
            f.Precision = this.Precision;
            f.SqlDataType = this.SqlDataType;
        }

    }




}
