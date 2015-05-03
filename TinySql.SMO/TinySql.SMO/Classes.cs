using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Data;
using System.Runtime.Caching;

namespace TinySql.Metadata
{
    public class SqlMetadataDatabase
    {
        #region ctor
        internal SqlMetadataDatabase(string ConnectionString, bool UseCache = true, string FileName = null)
        {
            Initialize(ConnectionString, UseCache, FileName);
            this.builder = new SqlBuilder(ConnectionString);
        }
        internal SqlMetadataDatabase(SqlBuilder Builder, bool UseCache = true, string FileName = null)
        {
            Initialize(Builder.ConnectionString ?? SqlBuilder.DefaultConnection, UseCache, FileName);
            this.builder = Builder;
        }

        private void Initialize(string connectionString, bool useCache, string FileName)
        {
            ConnectionBuilder = new System.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
            this.UseCache = useCache;
            this.MetadataKey = ConnectionBuilder.ConnectionString.GetHashCode().ToString();
            this.FileName = FileName;

        }

        #endregion


        public static SqlMetadataDatabase FromBuilder(SqlBuilder Builder, bool UseCache = true, string FileName = null)
        {
            return new SqlMetadataDatabase(Builder, UseCache, FileName);
        }
        public static SqlMetadataDatabase FromConnection(string ConnectionString, bool UseCache = true, string FileName = null)
        {
            return new SqlMetadataDatabase(ConnectionString, UseCache, FileName);
        }

        public MetadataDatabase BuildMetadata(bool PrimaryKeyIndexOnly = true)
        {
            MetadataDatabase mdb = FromCache();
            if (mdb != null)
            {
                // mdb.Builder = builder;
                return mdb;
            }
            Guid g = Guid.NewGuid();
            try
            {
                g = SqlDatabase.DatabaseGuid;
            }
            catch (Exception)
            {
            }
            mdb = new MetadataDatabase()
            {
                ID = g,
                Name = SqlDatabase.Name,
                Server = SqlServer.Name + (!string.IsNullOrEmpty(SqlServer.InstanceName) && SqlServer.Name.IndexOf('\\') == -1 ? "" : ""),
                Builder = builder
            };
            foreach (Microsoft.SqlServer.Management.Smo.Table table in SqlDatabase.Tables)
            {
                table.Refresh();
                BuildMetadata(mdb, table);
            }
            //Parallel.ForEach(db.Tables.OfType<Microsoft.SqlServer.Management.Smo.Table>(),
            //    new ParallelOptions { MaxDegreeOfParallelism = 2 },
            //    table => BuildMetadata(mdb, table));
            // builder.Metadata = mdb;
            ToCache(mdb);
            return mdb;

        }

        private MetadataTable BuildMetadata(MetadataDatabase mdb, Microsoft.SqlServer.Management.Smo.Table table, bool PrimaryKeyIndexOnly = true, bool SelfJoin = false)
        {
            MetadataTable mt = null;
            table.Refresh();
            if (mdb.Tables.TryGetValue(table.Name, out mt))
            {
                return mt;
            }

            mt = new MetadataTable()
            {
                ID = table.ID,
                Schema = table.Schema,
                Name = table.Name,
                Parent = mdb
            };


            foreach (Microsoft.SqlServer.Management.Smo.Column column in table.Columns)
            {
                MetadataColumn col = new MetadataColumn()
                {
                    ID = column.ID,
                    Parent = mt,
                    Database = mdb,
                    Name = column.Name,
                    Collation = column.Collation,
                    Default = column.Default,
                    IsComputed = column.Computed,
                    ComputedText = column.ComputedText,
                    IsPrimaryKey = column.InPrimaryKey,
                    IsIdentity = column.Identity,
                    IsForeignKey = column.IsForeignKey,
                    IdentityIncrement = column.IdentityIncrement,
                    IdentitySeed = column.IdentitySeed,
                    Nullable = column.Nullable,
                    IsRowGuid = column.RowGuidCol
                };
                BuildColumnDataType(col, column);
                col = mt.Columns.AddOrUpdate(col.Name, col, (key, existing) =>
                {
                    return col;
                });

            }

            foreach (Index idx in table.Indexes)
            {
                if (!PrimaryKeyIndexOnly || idx.IndexKeyType == IndexKeyType.DriPrimaryKey)
                {
                    Key key = new Key()
                    {
                        ID = idx.ID,
                        Parent = mt,
                        Database = mdb,
                        Name = idx.Name,
                        IsUnique = idx.IsUnique,
                        IsPrimaryKey = idx.IndexKeyType == IndexKeyType.DriPrimaryKey
                    };
                    foreach (IndexedColumn c in idx.IndexedColumns)
                    {
                        key.Columns.Add(mt[c.Name]);
                    }
                    mt.Indexes.AddOrUpdate(key.Name, key, (k, v) => { return key; });
                }
            }
            if (!SelfJoin)
            {
                foreach (ForeignKey FK in table.ForeignKeys)
                {
                    MetadataForeignKey mfk = new MetadataForeignKey()
                    {
                        ID = FK.ID,
                        Parent = mt,
                        Database = mdb,
                        Name = FK.Name,
                        ReferencedKey = FK.ReferencedKey,
                        ReferencedSchema = FK.ReferencedTableSchema,
                        ReferencedTable = FK.ReferencedTable
                    };
                    MetadataTable mtref = null;
                    if (!mdb.Tables.TryGetValue(mfk.ReferencedSchema + "." + mfk.ReferencedTable, out mtref))
                    {
                        bool self = false;
                        if (mfk.ReferencedSchema == mt.Schema && mfk.ReferencedTable == mt.Name)
                        {
                            self = true;
                        }
                        mtref = BuildMetadata(mdb, mfk.ReferencedTable, mfk.ReferencedSchema, PrimaryKeyIndexOnly, self);
                    }
                    foreach (ForeignKeyColumn cc in FK.Columns)
                    {
                        mfk.ColumnReferences.Add(new MetadataColumnReference()
                        {
                            Parent = mfk,
                            Database = mdb,
                            Name = cc.Name,
                            Column = mt[cc.Name],
                            ReferencedColumn = mtref[cc.ReferencedColumn]
                        });
                    }
                    mt.ForeignKeys.AddOrUpdate(mfk.Name, mfk, (key, existing) =>
                    {
                        return mfk;
                    });
                }
            }

            mdb.Tables.AddOrUpdate(mt.Schema + "." + mt.Name, mt, (key, existing) =>
            {
                return mt;
            });
            return mt;
        }

        public MetadataTable BuildMetadata(MetadataDatabase mdb, string TableName, string Schema = "dbo", bool PrimaryKeyIndexOnly = true, bool SelfJoin = false)
        {
            MetadataTable mt = null;
            if (mdb.Tables.TryGetValue(TableName, out mt))
            {
                return mt;
            }
            Microsoft.SqlServer.Management.Smo.Table t = new Microsoft.SqlServer.Management.Smo.Table(SqlDatabase, TableName, Schema);
            t.Refresh();
            return BuildMetadata(mdb, t, PrimaryKeyIndexOnly, SelfJoin);
        }

        private long GetVersion()
        {
            try
            {
                DataSet ds = SqlDatabase.ExecuteWithResults(VERSION_SQL);
                if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1 && !ds.Tables[0].Rows[0].IsNull(0))
                {
                    DateTime date = ds.Tables[0].Rows[0].Field<DateTime>(0);
                    return date.Ticks;
                }
                return 0;
            }
            catch (Exception)
            {
                return -1;
            }
        }

        private const string VERSION_SQL = "select MAX(modify_date) from sys.objects where is_ms_shipped = 0";

        private bool ToCache(MetadataDatabase mdb)
        {
            if (!UseCache)
            {
                return true;
            }
            return CacheMetadata(MetadataKey, mdb);
        }

        private MetadataDatabase FromCache()
        {
            if (UseCache == false)
            {
                return null;
            }
            MetadataDatabase mdb = CacheMetadata(MetadataKey);
            if (mdb != null)
            {
                return mdb;
            }
            if (!string.IsNullOrEmpty(FileName))
            {
                mdb = Serialization.FromFile(FileName);
                CacheMetadata(MetadataKey, mdb);
            }
            return mdb;
        }

        private static CacheItemPolicy _CachePolicy = new CacheItemPolicy() { AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration, SlidingExpiration = MemoryCache.NoSlidingExpiration };
        public static CacheItemPolicy CachePolicy
        {
            get { return _CachePolicy; }
            set { _CachePolicy = value; }
        }


        public static bool CacheMetadata(string MetadataKey, MetadataDatabase mdb, CacheItemPolicy Policy = null)
        {
            if (MemoryCache.Default.Contains(MetadataKey))
            {
                return true;
            }
            CacheItem item = MemoryCache.Default.AddOrGetExisting(new CacheItem(MetadataKey, Serialization.ToJson<MetadataDatabase>(mdb)), (Policy ?? CachePolicy));
            return item.Value == null;
        }

        public bool ClearMetadata()
        {
            return MemoryCache.Default.Remove(MetadataKey) != null;
        }


        public static MetadataDatabase CacheMetadata(string MetadataKey)
        {
            CacheItem item = MemoryCache.Default.GetCacheItem(MetadataKey);
            if (item != null)
            {
                return Serialization.FromJson<MetadataDatabase>(item.Value.ToString());
            }
            return null;
        }






        private void BuildColumnDataType(MetadataColumn column, Column col)
        {
            DataType ColumnType = col.DataType;

            switch (ColumnType.SqlDataType)
            {
                case SqlDataType.UserDefinedDataType:
                    SqlDatabase.UserDefinedDataTypes.Refresh(true);
                    UserDefinedDataType udt = SqlDatabase.UserDefinedDataTypes[ColumnType.Name];
                    column.SqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), udt.SystemType, true);
                    column.Length = udt.Length;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                case SqlDataType.SysName:
                    column.SqlDataType = SqlDbType.NVarChar;
                    column.Length = 128;
                    break;

                case SqlDataType.UserDefinedType:
                    column.SqlDataType = System.Data.SqlDbType.Udt;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                case SqlDataType.NVarCharMax:
                    column.SqlDataType = System.Data.SqlDbType.NVarChar;
                    break;

                case SqlDataType.VarCharMax:
                    column.SqlDataType = System.Data.SqlDbType.VarChar;
                    break;

                case SqlDataType.VarBinaryMax:
                    column.SqlDataType = SqlDbType.VarBinary;
                    break;

                case SqlDataType.Numeric:
                    column.SqlDataType = SqlDbType.Decimal;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                case SqlDataType.UserDefinedTableType:
                case SqlDataType.HierarchyId:
                case SqlDataType.Geometry:
                case SqlDataType.Geography:
                    column.SqlDataType = SqlDbType.Variant;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                default:
                    column.SqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), ColumnType.SqlDataType.ToString());
                    switch (column.SqlDataType)
                    {
                        case SqlDbType.Decimal:
                        case SqlDbType.Float:
                        case SqlDbType.Money:
                        case SqlDbType.Real:
                        case SqlDbType.SmallMoney:
                            column.Scale = ColumnType.NumericScale;
                            column.Precision = ColumnType.NumericPrecision;
                            break;

                        default:
                            break;
                    }
                    break;
            }

            column.Length = ColumnType.MaximumLength;
            column.DataType = ConvertSqlTypeToType(column.SqlDataType);
        }

        private static Type ConvertSqlTypeToType(SqlDbType sqlDataType)
        {
            switch (sqlDataType)
            {
                case SqlDbType.BigInt:
                    return typeof(Int64);

                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.Timestamp:
                case SqlDbType.VarBinary:
                    return typeof(Byte[]);

                case SqlDbType.Bit:
                    return typeof(Boolean);

                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                    return typeof(String);

                case SqlDbType.Date:
                case SqlDbType.DateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.SmallDateTime:
                case SqlDbType.Time:
                    return typeof(DateTime);

                case SqlDbType.DateTimeOffset:
                    return typeof(DateTimeOffset);

                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return typeof(Decimal);

                case SqlDbType.Float:
                    return typeof(Double);

                case SqlDbType.Int:
                    return typeof(Int32);

                case SqlDbType.Real:
                    return typeof(Single);

                case SqlDbType.SmallInt:
                    return typeof(Int16);

                case SqlDbType.TinyInt:
                    return typeof(Byte);

                case SqlDbType.UniqueIdentifier:
                    return typeof(Guid);

                case SqlDbType.Xml:
                    return typeof(System.Xml.XmlDocument);

                case SqlDbType.Structured:
                case SqlDbType.Udt:
                case SqlDbType.Variant:
                default:
                    return typeof(Object);
            }
        }


        private static ServerConnection GetConnection(SqlBuilder Builder)
        {
            return GetConnection(Builder.ConnectionString ?? SqlBuilder.DefaultConnection);
        }
        private static ServerConnection GetConnection(string ConnectionString)
        {
            System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(ConnectionString);
            return new ServerConnection(con);
        }

        private System.Data.SqlClient.SqlConnectionStringBuilder _ConnectionBuilder = null;
        public System.Data.SqlClient.SqlConnectionStringBuilder ConnectionBuilder
        {
            get { return _ConnectionBuilder; }
            private set { _ConnectionBuilder = value; }
        }
        private Server _SqlServer = null;
        public Server SqlServer
        {
            get
            {
                if (_SqlServer == null)
                {
                    _SqlServer = new Server(GetConnection(ConnectionBuilder.ConnectionString));
                }
                return _SqlServer;
            }
            set { _SqlServer = value; }
        }
        private Database _SqlDatabase;

        public Database SqlDatabase
        {
            get
            {
                if (_SqlDatabase == null)
                {
                    SqlDatabase = new Database(SqlServer, ConnectionBuilder.InitialCatalog);
                    SqlDatabase.Refresh();
                }
                return _SqlDatabase;
            }
            set { _SqlDatabase = value; }
        }
        internal SqlBuilder builder;
        public string MetadataKey { get; private set; }
        private bool _UseCache = true;

        public bool UseCache
        {
            get { return _UseCache; }
            private set { _UseCache = value; }
        }

        public string FileName { get; private set; }





    }
}
