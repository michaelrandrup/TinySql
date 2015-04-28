using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace TinySql.Metadata
{
    [Serializable]
    public class MetadataDatabase
    {
        public Guid ID = Guid.NewGuid();
        public string Name { get; set; }
        public string Server { get; set; }
        public List<MetadataTable> Tables = new List<MetadataTable>();
        public static MetadataDatabase FromFile(string FileName)
        {
            System.Xml.Serialization.XmlSerializer s = new System.Xml.Serialization.XmlSerializer(typeof(MetadataDatabase));
            object o;
            using (System.IO.FileStream fs = System.IO.File.OpenRead(FileName))
            {
                o = s.Deserialize(fs);
                fs.Close();
            }
            if (o != null)
            {
                return (MetadataDatabase)o;
            }
            return null;
        }

        public void ToFile(string FileName)
        {
            System.Xml.Serialization.XmlSerializer s = new System.Xml.Serialization.XmlSerializer(typeof(MetadataDatabase));
            using (System.IO.FileStream fs = System.IO.File.Create(FileName))
            {
                s.Serialize(fs, this);
                fs.Close();
            }
        }

    }
    [Serializable]
    public class MetadataTable
    {
        public int ID { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }
        public List<MetadataColumn> Columns = new List<MetadataColumn>();
        public List<MetadataForeignKey> ForeignKeys = new List<MetadataForeignKey>();
        public List<Key> Indexes = new List<Key>();
    }
    [Serializable]
    public class MetadataForeignKey
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public List<MetadataColumnReference> ColumnReferences = new List<MetadataColumnReference>();
        public string ReferencedSchema { get; set; }
        public string ReferencedTable { get; set; }
        public string ReferencedKey { get; set; }
    }

    [Serializable]
    public class MetadataColumnReference
    {
        public MetadataColumn Column { get; set; }
        public MetadataColumn ReferencedColumn { get; set; }
    }

    [Serializable]
    public class Key
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public List<MetadataColumn> Columns = new List<MetadataColumn>();
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
    }

    [Serializable]
    public class MetadataColumn
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public string Collation { get; set; }
        public SqlDbType SqlDataType { get; set; }
        public int Length { get; set; }
        public int Scale { get; set; }
        public int Precision { get; set; }
        private string _DataTypeName;
        public string DataTypeName
        {
            get { return _DataTypeName; }
            set
            {
                _DataTypeName = value;
                if (_DataType == null)
                {
                    _DataType = Type.GetType(_DataTypeName);
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
                DataTypeName = _DataType.FullName;
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
        public bool IsReadOnly
        {
            get
            {
                return IsComputed || IsIdentity || IsRowGuid;
            }
        }
    }




}
