using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Transactions;
using System.Xml;

namespace TinySql
{
    public static class Data
    {

        public static DataTable All<T>(string TableName = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(TableName, new string[] { "*" }, null, Top, Distinct);
            return builder.DataTable(ConnectionString,TimeoutSeconds,null);
        }

        public static List<T> All<T>(string TableName = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true)
        {
            return List<T>(null, All<T>(TableName, Top, Distinct, ConnectionString,TimeoutSeconds), AllowPrivateProperties, EnforceTypesafety);
        }

        public static Dictionary<TKey, T> All<TKey, T>(string TKeyPropertyName, string TableName = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true)
        {
            return Dictionary<TKey, T>(null, TKeyPropertyName, All<T>(TableName, Top, Distinct, ConnectionString,TimeoutSeconds), AllowPrivateProperties, EnforceTypesafety);
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(string TKeyPropertyName, string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(TableName, Properties, ExcludeProperties, Top, Distinct);
            return builder.Dictionary<TKey, T>(TKeyPropertyName, ConnectionString,TimeoutSeconds ,AllowPrivateProperties, EnforceTypesafety, Format);
        }

        public static List<T> List<T>(string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false, string ConnectionString = null,int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(TableName, Properties, ExcludeProperties, Top, Distinct);
            return builder.List<T>(ConnectionString,TimeoutSeconds, AllowPrivateProperties, EnforceTypesafety, Format);
        }

        public static DataTable DataTable(this SqlBuilder Builder, string ConnectionString = null,int TimeoutSeconds = 30, params object[] Format)
        {
            ConnectionString = ConnectionString ?? Builder.ConnectionString ?? SqlBuilder.DefaultConnection;
            if (ConnectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            DataTable dt = new DataTable();
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromMinutes(1)
            }))
            {
                try
                {
                    using (SqlConnection context = new SqlConnection(ConnectionString))
                    {
                        context.Open();
                        SqlCommand cmd = new SqlCommand(Builder.ToSql(Format), context);
                        cmd.CommandTimeout = TimeoutSeconds;
                        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                        adapter.Fill(dt);
                        context.Close();
                    }
                }
                catch (TransactionException exTrans)
                {
                    trans.Dispose();
                    throw exTrans;
                }
                trans.Complete();
            }
            return dt;
        }

        public static DataSet DataSet(this SqlBuilder Builder, string ConnectionString = null, params object[] Format)
        {
            ConnectionString = ConnectionString ?? Builder.ConnectionString ?? SqlBuilder.DefaultConnection;
            if (ConnectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            DataSet ds = new DataSet();
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromMinutes(1)
            }))
            {
                try
                {
                    using (SqlConnection context = new SqlConnection(ConnectionString))
                    {
                        context.Open();
                        SqlCommand cmd = new SqlCommand(Builder.ToSql(Format), context);
                        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                        adapter.Fill(ds);
                        context.Close();
                    }
                }
                catch (TransactionException exTrans)
                {
                    trans.Dispose();
                    throw exTrans;
                }
                trans.Complete();
            }
            return ds;
        }

        public static ResultTable Execute(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30, params object[] Format)
        {
            return new ResultTable(DataTable(Builder, ConnectionString,TimeoutSeconds, Format));
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(this SqlBuilder Builder, string TKeyPropertyName, DataTable dataTable, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
            Dictionary<TKey, T> dict = new Dictionary<TKey, T>();
            foreach (DataRow row in dataTable.Rows)
            {
                T instance = PopulateObject<T>(dataTable, row, AllowPrivateProperties, EnforceTypesafety);
                PropertyInfo prop = instance.GetType().GetProperty(TKeyPropertyName);
                if (prop != null)
                {
                    dict.Add((TKey)prop.GetValue(instance, null), instance);
                }
                else
                {
                    FieldInfo field = instance.GetType().GetField(TKeyPropertyName);
                    dict.Add((TKey)field.GetValue(instance), instance);
                }
            }
            return dict;
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(this SqlBuilder Builder, string TKeyPropertyName, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            Dictionary<TKey, T> dict = new Dictionary<TKey, T>();
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            return Dictionary<TKey, T>(Builder, TKeyPropertyName, dt, AllowPrivateProperties, EnforceTypesafety);
        }

        public static List<T> List<T>(this SqlBuilder Builder, DataTable dataTable, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
            List<T> list = new List<T>();
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add(PopulateObject<T>(dataTable, row, AllowPrivateProperties, EnforceTypesafety));
            }
            return list;
        }

        public static List<T> List<T>(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            return List<T>(Builder, dt, AllowPrivateProperties, EnforceTypesafety);
        }

        private static T PopulateObject<T>(DataTable dt, DataRow row, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
            T instance = Activator.CreateInstance<T>();
            foreach (DataColumn col in dt.Columns)
            {
                BindingFlags flag = BindingFlags.Public;
                if (AllowPrivateProperties)
                {
                    flag = flag | BindingFlags.NonPublic;
                }
                PropertyInfo prop = instance.GetType().GetProperty(col.ColumnName, BindingFlags.Instance | flag);
                FieldInfo field = null;
                if (prop == null)
                {
                    field = instance.GetType().GetField(col.ColumnName, BindingFlags.Instance | flag);
                    if (field != null)
                    {
                        if (field.FieldType == typeof(XmlDocument) && !row.IsNull(col))
                        {
                            if (col.DataType == typeof(string))
                            {
                                XmlDocument xml = new XmlDocument();
                                xml.LoadXml((string)row[col]);
                                field.SetValue(instance, xml);
                            }
                            else if (col.DataType == typeof(XmlDocument))
                            {
                                field.SetValue(instance, (XmlDocument)row[col]);
                            }
                        }
                        else if (!EnforceTypesafety || field.FieldType == col.DataType)
                        {
                            if (row.IsNull(col))
                            {
                                if (!field.FieldType.IsValueType || Nullable.GetUnderlyingType(field.FieldType) != null)
                                {
                                    field.SetValue(instance, null);
                                }
                            }
                            else
                            {
                                field.SetValue(instance, row[col.ColumnName]);
                            }
                        }
                    }
                    else
                    {
                        continue;
                    }
                   
                }
                else
                {
                    if (prop.CanWrite)
                    {
                        if (prop.PropertyType== typeof(XmlDocument) && !row.IsNull(col))
                        {
                            if (col.DataType == typeof(string))
                            {
                                XmlDocument xml = new XmlDocument();
                                xml.LoadXml((string)row[col]);
                                prop.SetValue(instance, xml,null);
                            }
                            else if (col.DataType == typeof(XmlDocument))
                            {
                                prop.SetValue(instance, (XmlDocument)row[col],null);
                            }
                        }
                        else if (!EnforceTypesafety || prop.PropertyType == col.DataType)
                        {
                            if (row.IsNull(col))
                            {
                                if (!prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null)
                                {
                                    prop.SetValue(instance, null, null);
                                }
                            }
                            else
                            {
                                prop.SetValue(instance, row[col.ColumnName], null);
                            }
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
            }
            return instance;
        }

        private static int ExecuteNonQueryInternal(SqlBuilder Builder, string ConnectionString, int Timeout = 30)
        {
            using (SqlConnection context = new SqlConnection(ConnectionString))
            {
                context.Open();
                SqlCommand cmd = new SqlCommand(Builder.ToSql(Builder.Format), context);
                cmd.CommandTimeout = Timeout;
                int i = cmd.ExecuteNonQuery();
                context.Close();
                return i;
            }
        }
        public static int ExecuteNonQuery(this SqlBuilder[] Builders, string ConnectionString = null, int TimeoutSeconds = 30)
        {
            ConnectionString = ConnectionString ?? SqlBuilder.DefaultConnection;
            if (ConnectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            int RowsAffected = 0;
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(TimeoutSeconds)
            }))
            {
                try
                {
                    // using (SqlConnection context = new SqlConnection(ConnectionString))
                    //{
                    //  context.Open();
                    foreach (SqlBuilder Builder in Builders)
                    {
                        // SqlCommand cmd = new SqlCommand(Builder.ToSql(Builder.Format), context);
                        // RowsAffected += cmd.ExecuteNonQuery();
                        RowsAffected += ExecuteNonQueryInternal(Builder, Builder.ConnectionString ?? ConnectionString, TimeoutSeconds);
                    }
                    //context.Close();
                    //}
                }
                catch (TransactionException exTrans)
                {
                    trans.Dispose();
                    throw exTrans;
                }
                catch (SqlException exSql)
                {
                    trans.Dispose();
                    throw exSql;
                }
                catch (ApplicationException exApplication)
                {
                    trans.Dispose();
                    throw exApplication;
                }
                trans.Complete();
            }
            return RowsAffected;
        }
    }
}
