using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Data;

namespace TinySql.Metadata
{
    public class SqlMetadataDatabase
    {
        #region ctor
        internal SqlMetadataDatabase(string ConnectionString)
        {
            Initialize(ConnectionString);
            this.builder = new SqlBuilder(ConnectionString);
        }
        internal SqlMetadataDatabase(SqlBuilder Builder)
        {
            Initialize(Builder.ConnectionString ?? SqlBuilder.DefaultConnection);
            this.builder = Builder;
        }

        private void Initialize(string ConnectionString)
        {
            con = new System.Data.SqlClient.SqlConnectionStringBuilder(ConnectionString);
            server = new Server(GetConnection(ConnectionString));
            db = new Database(server, con.InitialCatalog);
            db.Refresh();
        }

        #endregion


        public static SqlMetadataDatabase FromBuilder(SqlBuilder Builder)
        {
            return new SqlMetadataDatabase(Builder);
        }
        public static SqlMetadataDatabase FromConnection(string ConnectionString)
        {
            return new SqlMetadataDatabase(ConnectionString);
        }

        public MetadataDatabase BuildMetadata(bool PrimaryKeyIndexOnly = true)
        {
            Guid g = Guid.NewGuid();
            try
            {
                g = db.DatabaseGuid;
            }
            catch (Exception)
            {
            }
            MetadataDatabase mdb = new MetadataDatabase()
            {
                ID = g,
                Name = db.Name,
                Server = server.Name + (!string.IsNullOrEmpty(server.InstanceName) && server.Name.IndexOf('\\') == -1 ? "" : ""),
                Builder = builder
            };
            foreach (Microsoft.SqlServer.Management.Smo.Table table in db.Tables)
            {
                table.Refresh();
                BuildMetadata(mdb, table);
            }
            //Parallel.ForEach(db.Tables.OfType<Microsoft.SqlServer.Management.Smo.Table>(),
            //    new ParallelOptions { MaxDegreeOfParallelism = 2 },
            //    table => BuildMetadata(mdb, table));
            builder.Metadata = mdb;
            return mdb;

        }

        private MetadataTable BuildMetadata(MetadataDatabase mdb, Microsoft.SqlServer.Management.Smo.Table table, bool PrimaryKeyIndexOnly = true)
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
                    mtref = BuildMetadata(mdb, mfk.ReferencedTable, mfk.ReferencedSchema);
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

            mdb.Tables.AddOrUpdate(mt.Schema + "." + mt.Name, mt, (key, existing) =>
            {
                return mt;
            });
            return mt;
            Console.WriteLine("Built table {0}", mt.Name);
        }

        public MetadataTable BuildMetadata(MetadataDatabase mdb, string TableName, string Schema = "dbo", bool PrimaryKeyIndexOnly = true)
        {
            MetadataTable mt = null;
            if (mdb.Tables.TryGetValue(TableName, out mt))
            {
                return mt;
            }
            Microsoft.SqlServer.Management.Smo.Table t = new Microsoft.SqlServer.Management.Smo.Table(db, TableName, Schema);
            t.Refresh();
            return BuildMetadata(mdb, t);
        }





        private void BuildColumnDataType(MetadataColumn column, Column col)
        {
            DataType ColumnType = col.DataType;

            switch (ColumnType.SqlDataType)
            {
                case SqlDataType.UserDefinedDataType:
                    db.UserDefinedDataTypes.Refresh(true);
                    UserDefinedDataType udt = db.UserDefinedDataTypes[ColumnType.Name];
                    column.SqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), udt.SystemType, true);
                    column.Length = udt.Length;
                    column.Scale = udt.NumericScale;
                    column.Precision = udt.NumericPrecision;
                    break;

                case SqlDataType.SysName:
                    column.SqlDataType = SqlDbType.NVarChar;
                    column.Length = 128;
                    break;

                case SqlDataType.UserDefinedType:
                    column.SqlDataType = System.Data.SqlDbType.Udt;
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
                    break;

                case SqlDataType.UserDefinedTableType:
                case SqlDataType.HierarchyId:
                case SqlDataType.Geometry:
                case SqlDataType.Geography:
                    column.SqlDataType = SqlDbType.Variant;
                    break;

                default:
                    column.SqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), ColumnType.SqlDataType.ToString());
                    break;
            }

            column.Length = ColumnType.MaximumLength;
            column.Scale = ColumnType.NumericScale;
            column.Precision = ColumnType.NumericPrecision;
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

        private System.Data.SqlClient.SqlConnectionStringBuilder con;
        private Server server;
        private Database db;
        internal SqlBuilder builder;





    }
}
