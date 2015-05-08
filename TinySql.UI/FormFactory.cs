using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TinySql.Metadata;

namespace TinySql.UI
{
    public class FormFactory
    {
        private ConcurrentDictionary<string, Form> _PrimaryForms = new ConcurrentDictionary<string, Form>();

        public ConcurrentDictionary<string, Form> PrimaryForms
        {
            get { return _PrimaryForms; }
        }

        private static FormFactory instance = null;
        public static FormFactory Default
        {
            get
            {
                if (instance == null)
                {
                    instance = new FormFactory();
                }
                return instance;
            }
        }

        public Form GetForm(MetadataTable Table, FormTypes FormType)
        {
            if (FormType == FormTypes.Mobile)
            {
                throw new NotSupportedException("Mobile forms are not supported");
            }
            Form f;
            if (PrimaryForms.TryGetValue(Table.Fullname,out f))
            {
                return f;
            }
            else
            {
                f = BuildDefaultForm(Table);
                if (PrimaryForms.TryAdd(Table.Fullname,f))
                {
                    return f;
                }
                else
                {
                    throw new InvalidOperationException("The default form for " + Table.Fullname + " could not be cached");
                }
            }
        }

        public static Form BuildDefaultForm(MetadataTable Table)
        {
            Form form = new Form();
            form.TitleColumn = Table.GuessTitleColumn();
            if (form.TitleColumn == null)
            {
                form.TitleColumn = Table.PrimaryKey.Columns.First().Name;
            }
            FormSection section = new FormSection();
            foreach (MetadataColumn col in Table.Columns.Values)
            {
                if (col.IsRowGuid)
                {
                    continue;
                }
                FormField field = new FormField()
                {
                    DisplayName = col.GetDisplayName(Table.Fullname),
                    ID = col.Name,
                    Name = col.Name,
                    NullText = "Enter " + col.Name,
                    IsReadOnly = col.IsReadOnly || col.IsPrimaryKey
                };
                ResolveFieldType(col,field);
                section.Fields.Add(field);
            }
            form.Sections.Add(section);

            // Cache the form

            return form;
        }

        private static void ResolveFieldType(MetadataColumn col, FormField field)
        {
            switch (col.SqlDataType)
            {
                case System.Data.SqlDbType.Text:
                case System.Data.SqlDbType.NText:
                case System.Data.SqlDbType.VarChar:
                case System.Data.SqlDbType.NVarChar:
                case System.Data.SqlDbType.Char:
                case System.Data.SqlDbType.NChar:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.text;
                    if (col.Length <= 0)
                    {
                        field.FieldType = FieldTypes.TextArea;
                    }
                    field.MaxLength = col.Length;
                    break;

                case System.Data.SqlDbType.Date:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.date;
                    break;

                case System.Data.SqlDbType.SmallDateTime:
                case System.Data.SqlDbType.DateTime:
                case System.Data.SqlDbType.DateTime2:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.datetime_local;
                    break;

                case System.Data.SqlDbType.DateTimeOffset:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.datetime;
                    break;

                case System.Data.SqlDbType.Time:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.time;
                    break;

                case System.Data.SqlDbType.Bit:
                    field.FieldType = FieldTypes.Checkbox;
                    break;

                case System.Data.SqlDbType.Decimal:
                case System.Data.SqlDbType.Float:
                case System.Data.SqlDbType.Int:
                case System.Data.SqlDbType.Money:
                case System.Data.SqlDbType.BigInt:
                case System.Data.SqlDbType.Real:
                case System.Data.SqlDbType.SmallInt:
                case System.Data.SqlDbType.SmallMoney:
                case System.Data.SqlDbType.TinyInt:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.number;
                    break;

                case System.Data.SqlDbType.VarBinary:
                case System.Data.SqlDbType.Timestamp:
                case System.Data.SqlDbType.Structured:
                case System.Data.SqlDbType.Binary:
                case System.Data.SqlDbType.Variant:
                case System.Data.SqlDbType.Xml:
                case System.Data.SqlDbType.Image:
                case System.Data.SqlDbType.Udt:
                case System.Data.SqlDbType.UniqueIdentifier:
                default:
                    field.FieldType = FieldTypes.Input;
                    field.IsHidden = true;
                    return;
            }

            // Special cases
            if (col.IsForeignKey)
            {
                field.FieldType = FieldTypes.LookupInput;
            }

        }



    }
}
