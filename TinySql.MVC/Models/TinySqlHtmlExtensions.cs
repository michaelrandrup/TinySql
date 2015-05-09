using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TinySql.UI;

namespace TinySql.UI
{
    public static class TinySqlExtensions
    {
        public static MvcHtmlString TinySqlLabel(this HtmlHelper helper, FieldModel model)
        {

            string label = string.Format("<label id=\"label{1}\" for=\"{1}\" class=\"{2}\">{0}</label>",
                model.Field.DisplayName,                      // 0
                model.Field.ID,                               // 1
                model.Field.GetCssLabelLayout(model.SectionLayout)  // 2
            );
            return MvcHtmlString.Create(label);
        }

        public static MvcHtmlString TinySqlInput(this HtmlHelper helper, FieldModel model)
        {
            string ctrl = "";
            string ReadOnly = model.Field.IsReadOnly ? "readonly" : "";
            if (model.Field.FieldType == FieldTypes.Input)
            {
                ctrl = string.Format("<input type=\"{4}\" class=\"{2}\" name=\"{1}\" id=\"input{0}\" placeholder=\"Email\" value=\"{5}\" {6} >",
                    model.Field.ID,                                         // 0
                    model.Field.Name,                                       // 1
                    model.Field.CssInputControlLayout,                      // 2
                    model.Field.NullText,                                   // 3
                    model.Field.InputType.ToString().Replace("_", "-"),      // 4
                    model.Data == null ? "" : Convert.ToString(model.Data),  // 5
                    ReadOnly                                                // 6
                    );
            }
            else if (model.Field.FieldType == FieldTypes.LookupInput || model.Field.FieldType == FieldTypes.SelectList)
            {
                LookupFormField lookup = (model.Field as LookupFormField);
                ResultTable results = null;
                if (lookup.FieldType == FieldTypes.SelectList)
                {
                    string ctrlItems = "";
                    string v = model.Data != null ? Convert.ToString(model.Data) : "";
                    if (lookup.LookupSource == LookupSources.NameValueCollection)
                    {
                        foreach (string name in lookup.Collection.AllKeys)
                        {
                            string value = lookup.Collection[name];
                            ctrlItems += string.Format("<option value=\"{0}\" {1}>{2}</option>",
                                value,
                                v.Equals(value) ? "selected" : "",
                                name);
                        }
                    }
                    else if (lookup.LookupSource == LookupSources.SqlBuilder)
                    {
                        results = lookup.Builder.Execute();
                        foreach (RowData row in results)
                        {
                            string name = Convert.ToString(row.Column(row.Columns[0]));
                            string value = Convert.ToString(row.Column(row.Columns[1]));
                            ctrlItems += string.Format("<option value=\"{0}\" {1}>{2}</option>",
                                value,
                                v.Equals(value) ? "selected" : "",
                                name);
                        }
                    }
                    ctrl = string.Format("<select id=\"{0}\" name=\"{1}\" class=\"{2}\" {3} >{4}</select>",
                    lookup.ID,                      // 0
                    lookup.Name,                    // 1
                    lookup.CssInputControlLayout,   // 2
                    ReadOnly,                       // 3
                    ctrlItems                       // 4  
                    );
                }
                else if (lookup.FieldType == FieldTypes.LookupInput)
                {
                    throw new NotImplementedException();
                }
            }


            return MvcHtmlString.Create(ctrl);
        }
    }
}