using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml;

namespace TinySql
{
    public static class TypeBuilder
    {
        public static SqlBuilder Select(Type ObjectType, string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false)
        {
            SqlBuilder builder = SqlBuilder.Select(Top, Distinct);
            Table BaseTable = null;
            if (string.IsNullOrEmpty(TableName))
            {
                BaseTable = builder.From(ObjectType.Name);
            }
            else
            {
                BaseTable = builder.From(TableName);
            }
            if (Properties == null)
            {
                Properties = ObjectType.GetProperties().Select(x => x.Name).Union(ObjectType.GetFields().Select(x => x.Name)).ToArray();
            }
            if (ExcludeProperties == null)
            {
                ExcludeProperties = new string[0];
            }
            foreach (string Name in Properties.Except(ExcludeProperties))
            {
                BaseTable.Column(Name);
            }
            return builder;
        }

        public static SqlBuilder Update<TModel, TProperty>(this TableHelper<TModel> helper, TModel Instance, Expression<Func<TModel, TProperty>> prop)
        {
            return helper.table.Builder;
        }

        public static void UpdateEx<TModel, TProperty>(this ModelHelper<TModel> helper, Expression<Func<TModel, TProperty>> prop)
        {
            TProperty t = prop.Compile().Invoke(helper.Model);
            
            
        }

        public class ModelHelper<TModel>
        {
            public TModel Model { get; set; }
            public ModelHelper(TModel model)
            {
                this.Model = model;
            }
        }





        public static SqlBuilder Select<T>(string TableName = null, string[] Properties = null, string[] ExcludeProperties = null, int? Top = null, bool Distinct = false)
        {

            return Select(typeof(T), TableName, Properties, ExcludeProperties, Top, Distinct);
        }

        public static T PopulateObject<T>(T instance, DataTable dt, DataRow row, bool AllowPrivateProperties, bool EnforceTypesafety)
        {
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
                        if (prop.PropertyType == typeof(XmlDocument) && !row.IsNull(col))
                        {
                            if (col.DataType == typeof(string))
                            {
                                XmlDocument xml = new XmlDocument();
                                xml.LoadXml((string)row[col]);
                                prop.SetValue(instance, xml, null);
                            }
                            else if (col.DataType == typeof(XmlDocument))
                            {
                                prop.SetValue(instance, (XmlDocument)row[col], null);
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

        public static T PopulateObject<T>(DataTable dt, DataRow row, bool AllowPrivateProperties, bool EnforceTypesafety, bool UseDefaultConstructor = true)
        {
            T instance = default(T);
            if (UseDefaultConstructor)
            {
                instance = Activator.CreateInstance<T>();
                return PopulateObject<T>(instance, dt, row, AllowPrivateProperties, EnforceTypesafety);
            }
            else
            {
                object o = Activator.CreateInstance(typeof(T), new object[] { row });
                if (o == null)
                {
                    o = Activator.CreateInstance(typeof(T), new object[] { dt, row });
                }
                if (o == null)
                {
                    o = Activator.CreateInstance(typeof(T), new object[] { row, dt });
                }
                if (o == null)
                {
                    o = Activator.CreateInstance(typeof(T), new object[] { dt });
                }
                if (o != null)
                {
                    return (T)o;
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The type {0} does not provide a valid constructor for a DataRow and/or DataTable object", typeof(T).FullName));
                }

            }



        }
    }
}
