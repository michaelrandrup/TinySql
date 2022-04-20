using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Transactions;
using TinySql.Cache;
using TinySql.Metadata;

namespace TinySql
{
    public static class Data
    {
        #region Execute methods

        public static ResultTable Execute(this SqlBuilder Builder, int TimeoutSeconds = 30, bool WithMetadata = true, ResultTable.DateHandlingEnum? DateHandling = null, bool UseCache = true, string UseHierachyField = null, params object[] Format)
        {
            if (UseCache && CacheProvider.UseResultCache)
            {
                if (CacheProvider.ResultCache.IsCached(Builder))
                {
                    return CacheProvider.ResultCache.Get(Builder);
                }
            }
            ResultTable result = new ResultTable(Builder, TimeoutSeconds, WithMetadata, DateHandling, UseHierachyField, Format);
            if (CacheProvider.UseResultCache)
            {
                CacheProvider.ResultCache.Add(Builder, result);
            }
            return result;

        }



        public static ResultTable Execute(this List<SqlBuilder> Builders, int TimeoutSeconds = 30)
        {
            return Execute(Builders.ToArray(), TimeoutSeconds);
        }

        private static ResultTable ExecuteRelatedInternal(SqlBuilder builder, Dictionary<string, RowData> results)
        {
            if (results.Count > 0)
            {
                MetadataTable mt = builder.BaseTable().WithMetadata().Model;
                foreach (string key in results.Keys)
                {
                    foreach (MetadataForeignKey fk in mt.ForeignKeys.Values.Where(x => (x.ReferencedSchema + "." + x.ReferencedTable).Equals(key, StringComparison.OrdinalIgnoreCase)))
                    {
                        RowData row = results[key];
                        foreach (MetadataColumnReference mcr in fk.ColumnReferences)
                        {
                            if (row.Columns.Contains(mcr.Column.Name))
                            {
                                Field f = builder.BaseTable().FindField(mcr.Column.Name);
                                if (f != null)
                                {
                                    f.Value = row.Column(mcr.Column.Name);
                                }
                                else
                                {
                                    (builder.BaseTable() as InsertIntoTable).Value(mcr.Column.Name, row.Column(mcr.Column.Name), SqlDbType.VarChar);
                                }
                            }
                        }
                    }
                }
            }
            DataTable dt = new DataTable();
            ResultTable table = new ResultTable();
            using (SqlConnection context = new SqlConnection(builder.ConnectionString))
            {
                context.Open();
                SqlCommand cmd = new SqlCommand(builder.ToSql(), context);
                SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                adapter.AcceptChangesDuringFill = false;
                adapter.Fill(dt);
                context.Close();
            }

            if (builder.SubQueries.Count > 0)
            {
                Dictionary<string, RowData> subresults = new System.Collections.Generic.Dictionary<string, RowData>(results);
                if (dt.Rows.Count > 0)
                {
                    MetadataTable mt = builder.BaseTable().WithMetadata().Model;
                    if (!subresults.ContainsKey(mt.Fullname))
                    {
                        ResultTable rt = new ResultTable(dt, ResultTable.DateHandlingEnum.None);
                        RowData row = rt.First();
                        table.Add(row);
                        subresults.Add(mt.Fullname, row);
                    }
                }
                foreach (SqlBuilder Builder in builder.SubQueries.Values)
                {
                    ResultTable sub = ExecuteRelatedInternal(Builder, subresults);
                    foreach (RowData row in sub)
                    {
                        table.Add(row);
                    }
                }
            }
            return table;


        }

        public static ResultTable Execute(this SqlBuilder[] Builders, int TimeoutSeconds = 30)
        {
            ResultTable table = new ResultTable();
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(TimeoutSeconds)
            }))
            {
                try
                {
                    foreach (SqlBuilder builder in Builders)
                    {
                        DataTable dt = new DataTable();
                        using (SqlConnection context = new SqlConnection(builder.ConnectionString))
                        {
                            context.Open();
                            SqlCommand cmd = new SqlCommand(builder.ToSql(), context);
                            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                            adapter.AcceptChangesDuringFill = false;
                            adapter.Fill(dt);
                            context.Close();
                        }

                        if (builder.SubQueries.Count > 0)
                        {
                            Dictionary<string, RowData> results = new Dictionary<string, RowData>();
                            if (dt.Rows.Count > 0)
                            {
                                MetadataTable mt = builder.BaseTable().WithMetadata().Model;
                                if (!results.ContainsKey(mt.Fullname))
                                {
                                    ResultTable rt = new ResultTable(dt, ResultTable.DateHandlingEnum.None);
                                    RowData row = rt.First();
                                    results.Add(mt.Fullname, row);
                                    table.Add(row);
                                }
                            }
                            foreach (SqlBuilder Builder in builder.SubQueries.Values)
                            {
                                ResultTable sub = ExecuteRelatedInternal(Builder, results);
                                foreach (RowData row in sub)
                                {
                                    table.Add(row);
                                }
                            }
                        }
                    }
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

            return table;


        }



        public static DataTable DataTable(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30, params object[] Format)
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
            if (Builder.StatementType == SqlBuilder.StatementTypes.Procedure)
            {
                FillBuilderFromProcedureOutput(Builder, dt);
            }
            return dt;
        }

        private static void FillBuilderFromProcedureOutput(SqlBuilder builder, DataTable dt)
        {
            int count = builder.Procedure.Parameters.Count(x => x.IsOutput);
            if (count > 0 && dt.Rows.Count == 1 && dt.Columns.Count == count)
            {
                int idx = 0;
                foreach (ParameterField field in builder.Procedure.Parameters.Where(x => x.IsOutput == true))
                {
                    field.Value = dt.Rows[0][idx];
                    idx++;
                }
            }
        }

        public static DataSet DataSet(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 60, params object[] Format)
        {
            ConnectionString = ConnectionString ?? Builder.ConnectionString ?? SqlBuilder.DefaultConnection;
            if (ConnectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            DataSet ds = new DataSet();
            //using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            //{
            //    IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
            //    Timeout = TimeSpan.FromSeconds(TimeoutSeconds)
            //}))
            //{
                try
                {
                    using (SqlConnection context = new SqlConnection(ConnectionString))
                    {
                        context.Open();
                        SqlCommand cmd = new SqlCommand(Builder.ToSql(Format), context);
                        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                        adapter.AcceptChangesDuringFill = false;
                        adapter.Fill(ds);
                        context.Close();
                    }
                }
                catch (TransactionException exTrans)
                {
                    //trans.Dispose();
                    throw exTrans;
                }
            //    trans.Complete();
            //}
            if (Builder.StatementType == SqlBuilder.StatementTypes.Procedure)
            {
                FillBuilderFromProcedureOutput(Builder, ds.Tables[0]);
            }
            return ds;
        }

        private static int ExecuteNonQueryInternal(SqlBuilder Builder, string ConnectionString, int Timeout = 30)
        {
            using (SqlConnection context = new SqlConnection(ConnectionString))
            {
                context.Open();
                SqlCommand cmd = null;
                if (Builder.StatementType != SqlBuilder.StatementTypes.Procedure)
                {
                    cmd = new SqlCommand(Builder.ToSql(Builder.Format), context);
                }
                else
                {
                    cmd = new SqlCommand(Builder.Procedure.Name, context);
                    cmd.CommandType = CommandType.StoredProcedure;
                    foreach (ParameterField par in Builder.Procedure.Parameters)
                    {
                        cmd.Parameters.Add(ToSqlParameter(par));
                    }
                }
                cmd.CommandTimeout = Timeout;
                int i = cmd.ExecuteNonQuery();
                context.Close();
                if (Builder.StatementType == SqlBuilder.StatementTypes.Procedure && Builder.Procedure.Parameters.Count(x => x.IsOutput) > 0)
                {
                    foreach (ParameterField par in Builder.Procedure.Parameters.Where(x => x.IsOutput))
                    {
                        par.Value = cmd.Parameters[par.ParameterName].Value;
                    }
                }
                return i;
            }
        }

        private static SqlParameter ToSqlParameter(ParameterField field)
        {
            SqlParameter p = new SqlParameter()
            {
                ParameterName = field.ParameterName,
                SqlDbType = field.SqlDataType,
                Precision = field.Precision >= 0 ? (byte)field.Precision : (byte)0,
                Scale = field.Scale >= 0 ? (byte)field.Scale : (byte)0,
                Size = field.MaxLength >= 0 ? field.MaxLength : 0,
                Direction = field.IsOutput ? ParameterDirection.Output : ParameterDirection.Input
            };
            if (!field.IsOutput)
            {
                // object o = ParameterField.GetFieldValue(field.DataType, field.Value, field.Builder.Culture);
                // p.Value = o == null ? DBNull.Value : o;
                p.Value = field.Value == null ? DBNull.Value : field.Value;
            }
            return p;
        }


        public static int ExecuteNonQuery(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30)
        {
            return new SqlBuilder[] { Builder }.ExecuteNonQuery(ConnectionString, TimeoutSeconds);
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

        #endregion

        #region FirstOrDefault<T> Methods
        public static T FirstOrDefault<T>(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            if (dt.Rows.Count == 0)
            {
                return default(T);
            }
            return TypeBuilder.PopulateObject<T>(dt, dt.Rows[0], AllowPrivateProperties, EnforceTypesafety);
        }

        #endregion

            #region List<T> Methods

        public static List<T> All<T>(string TableName = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true)
        {
            return List<T>(null, All<T>(TableName, Top, Distinct, ConnectionString, TimeoutSeconds), AllowPrivateProperties, EnforceTypesafety);
        }

        public static List<T> List<T>(string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(TableName, Properties, ExcludeProperties, Top, Distinct);
            return builder.List<T>(ConnectionString, TimeoutSeconds, AllowPrivateProperties, EnforceTypesafety, Format);
        }

        public static List<T> List<T>(this SqlBuilder Builder, DataTable dataTable, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
            List<T> list = new List<T>();
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add(TypeBuilder.PopulateObject<T>(dataTable, row, AllowPrivateProperties, EnforceTypesafety));
            }
            return list;
        }

        public static S List<T, S>(this SqlBuilder Builder, DataTable dataTable, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
            ICollection<T> list = Activator.CreateInstance<S>() as ICollection<T>;
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add(TypeBuilder.PopulateObject<T>(dataTable, row, AllowPrivateProperties, EnforceTypesafety));
            }
            return (S)list;
        }

        public static S List<T, S>(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            DataSet ds = DataSet(Builder, ConnectionString, TimeoutSeconds, Format);
            return List<T, S>(Builder, dt, AllowPrivateProperties, EnforceTypesafety);


        }


        public static List<T> List<T>(this SqlBuilder Builder, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            return List<T>(Builder, dt, AllowPrivateProperties, EnforceTypesafety);
        }

        #endregion

        #region Dictionary<TKey, TValue> Methods

        public static Dictionary<TKey, T> All<TKey, T>(string TKeyPropertyName, string TableName = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true)
        {
            return Dictionary<TKey, T>(null, TKeyPropertyName, All<T>(TableName, Top, Distinct, ConnectionString, TimeoutSeconds), AllowPrivateProperties, EnforceTypesafety);
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(string TKeyPropertyName, string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(TableName, Properties, ExcludeProperties, Top, Distinct);
            return builder.Dictionary<TKey, T>(TKeyPropertyName, ConnectionString, TimeoutSeconds, AllowPrivateProperties, EnforceTypesafety, Format);
        }

        public static S Dictionary<TKey, T, S>(this SqlBuilder Builder, string TKeyPropertyName, DataTable dataTable, bool AllowPrivateProperties, bool EnforceTypesafety, Func<S, TKey, T, bool> InsertUpdateDelegate = null) where S : IDictionary<TKey, T>
        {
            IDictionary<TKey, T> dict = Activator.CreateInstance<S>() as IDictionary<TKey, T>;
            foreach (DataRow row in dataTable.Rows)
            {
                T instance = TypeBuilder.PopulateObject<T>(dataTable, row, AllowPrivateProperties, EnforceTypesafety);
                PropertyInfo prop = instance.GetType().GetProperty(TKeyPropertyName);
                if (prop != null)
                {
                    if (InsertUpdateDelegate != null)
                    {
                        if (!InsertUpdateDelegate((S)dict, (TKey)prop.GetValue(instance, null), instance))
                        {
                            throw new InvalidOperationException("The InsertUpdate delegate failed to insert or update the dictionary " + typeof(S).Name);
                        }
                    }
                    else
                    {
                        dict.Add((TKey)prop.GetValue(instance, null), instance);
                    }
                }
                else
                {
                    FieldInfo field = instance.GetType().GetField(TKeyPropertyName);
                    if (InsertUpdateDelegate != null)
                    {
                        if (!InsertUpdateDelegate((S)dict, (TKey)field.GetValue(instance), instance))
                        {
                            throw new InvalidOperationException("The InsertUpdate delegate failed to insert or update the dictionary " + typeof(S).Name);
                        }
                    }
                    else
                    {
                        dict.Add((TKey)field.GetValue(instance), instance);
                    }

                }
            }
            return (S)dict;
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(this SqlBuilder Builder, string TKeyPropertyName, DataTable dataTable, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
            Dictionary<TKey, T> dict = new Dictionary<TKey, T>();
            foreach (DataRow row in dataTable.Rows)
            {
                T instance = TypeBuilder.PopulateObject<T>(dataTable, row, AllowPrivateProperties, EnforceTypesafety);
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
            // Dictionary<TKey, T> dict = new Dictionary<TKey, T>();
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            return (Dictionary<TKey, T>)Dictionary<TKey, T, Dictionary<TKey, T>>(Builder, TKeyPropertyName, dt, AllowPrivateProperties, EnforceTypesafety);
        }

        public static S Dictionary<TKey, T, S>(this SqlBuilder Builder, string TKeyPropertyName, string ConnectionString = null, int TimeoutSeconds = 30, bool AllowPrivateProperties = false, bool EnforceTypesafety = true, params object[] Format) where S : IDictionary<TKey, T>
        {
            IDictionary<TKey, T> dict = Activator.CreateInstance<S>() as IDictionary<TKey, T>;
            DataTable dt = DataTable(Builder, ConnectionString, TimeoutSeconds, Format);
            return (S)Dictionary<TKey, T, S>(Builder, TKeyPropertyName, dt, AllowPrivateProperties, EnforceTypesafety);
        }

        #endregion


        public static DataTable All<T>(string TableName = null, int? Top = null, bool Distinct = false, string ConnectionString = null, int TimeoutSeconds = 30)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(TableName, new string[] { "*" }, null, Top, Distinct);
            return builder.DataTable(ConnectionString, TimeoutSeconds, null);
        }



    }
}
