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
            if (model.Field.FieldType == FieldTypes.Input)
            {
                ctrl = string.Format("<input type=\"{4}\" class=\"{2}\" name=\"{1}\" id=\"input{0}\" placeholder=\"Email\" value=\"{5}\" >",
                    model.Field.ID,                                         // 0
                    model.Field.Name,                                       // 1
                    model.Field.CssInputControlLayout,                      // 2
                    model.Field.NullText,                                   // 3
                    model.Field.InputType.ToString().Replace("_","-"),      // 4
                    model.Data == null ? "" : Convert.ToString(model.Data)  // 5
                    );
            }



            return MvcHtmlString.Create(ctrl);
        }
    }
}