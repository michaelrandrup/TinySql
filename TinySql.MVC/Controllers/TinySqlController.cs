using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TinySql.MVC.Models;
using TinySql.Serialization;
using TinySql;
using TinySql.Metadata;
using TinySql.UI;

namespace TinySql.MVC.Controllers
{
    public class TinySqlController : Controller
    {
        // GET: TinySql
        public ActionResult Index()
        {
            return View();
        }

        public PartialViewResult Edit(string Id, string Table, ListTypes ListType, string ListName)
        {

            decimal pk = Convert.ToDecimal(Id);
            MetadataTable table = SqlBuilder.DefaultMetadata.FindTable(Table);
            SqlBuilder builder = table.ToSqlBuilder(ListType != ListTypes.Custom ? ListType.ToString() : ListName);
            builder.BaseTable().WithMetadata().WherePrimaryKey(new object[] { (object)Id });
            
            
            //SqlBuilder builder = SqlBuilder.Select()
            //    .From("Contact").Columns("ContactID", "Name", "Title", "WorkEmail", "JobfunctionID", "JobpositionID", "StateID")
            //    .WithMetadata()
            //    .AutoJoin("AccountID")
            //    .From("Contact").WithMetadata().AutoJoin("JobfunctionID")
            //    .From("Contact").WithMetadata().AutoJoin("JobpositionID")
            //    .Where<decimal>("Contact", "ContactID", SqlOperators.Equal, pk)
            //    .Builder();

            ResultTable result = builder.Execute();
            
            Form model = FormFactory.Default.BuildForm(builder);
            model.Initialize(result.First());
            return PartialView("~/Views/TinySql/Details/dialog.cshtml",model);
        }



        [HttpPost]
        public ContentResult Save(FormCollection form)
        {
            // Retrieve by Primary key
            var s = form[0];

            string TableName = form["__TABLE"];
            MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(TableName);
            List<object> PKs = new List<object>();
            foreach (MetadataColumn mc in mt.PrimaryKey.Columns)
            {
                PKs.Add(form[TableName + "_" + mc.Name]);
            }
            RowData row = RowData.Create(mt, true,PKs.ToArray());
            foreach (string key in form.Keys)
            {
                if (!key.StartsWith("__"))
                {
                    string ColumnName = key.Replace(TableName, "").Replace("_", "");
                    MetadataColumn mc;
                    if (mt.Columns.TryGetValue(ColumnName, out mc))
                    {
                        if (!mc.IsReadOnly)
                        {
                            row.Column(mc.Name, (object)form[key]);
                        }
                    }
                }
            }

            SqlBuilder builder = row.Update(true, true);
            ResultTable result = builder.Execute(30, false);
            if (result.Count == 1)
            {
                object PK = result.First().Column(mt.PrimaryKey.Columns.First().Name);
                SqlBuilder Builder = mt.ToSqlBuilder(ListTypes.Primary.ToString());
                Builder.BaseTable().WithMetadata().WherePrimaryKey(new object[] { (object)PK });
                ResultTable updated = Builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
                if (updated.Count == 1)
                {
                    return Content(SerializationExtensions.ToJson<dynamic>(updated.First()), "application/json");
                }
                
            }

            return Content("");


        }


        [HttpPost]
        public ContentResult Update(string rowData)
        {
            RowData row = SerializationExtensions.FromJson<RowData>(rowData);
            row.LoadMetadata();
            row.Column("Name", "Random " + Guid.NewGuid().ToString());
            SqlBuilder builder = row.Update(false,true);

            ResultTable result = builder.Execute(30, false);
            if (result.Count == 1)
            {
                 builder = SqlBuilder.Select()
                .From("Contact")
                .Column("ContactID")
                .Column("Name")
                .Column("Telephone")
                .Column("WorkEmail")
                .Column("ModifiedOn")
                .WithMetadata().InnerJoin("AccountID")
                .Column("Name", "AccountName")
                .From("Contact")
                .Where<decimal>("Contact", "ContactID", SqlOperators.Equal,result.First().Column<decimal>("ContactID"))
                .Builder();
                
                result = builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
                row = result.First();
                return Content(SerializationExtensions.ToJson<dynamic>(row), "application/json");
            }
            return Content("Hmmmm...?","application/text");
        }

        [AcceptVerbs( HttpVerbs.Get)]
        public ContentResult Rows(string TableName, ListTypes ListType, string ListName)
        {
            MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(TableName);
            SqlBuilder Builder = mt.ToSqlBuilder(ListType != ListTypes.Custom ? ListType.ToString() : ListName);

            ResultTable result = Builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
            return Content(SerializationExtensions.ToJson<dynamic>(result), "application/json");

        }

        [AcceptVerbs(HttpVerbs.Get)]
        public PartialViewResult List(string TableName, ListTypes ListType, string ListName)
        {
            ListBuilder model = ListFactory.Default.BuildList(TableName, ListType, null, ListName);
            return PartialView(ListFactory.Defaults.ListViewUrl, model);
        }

        [AcceptVerbs( HttpVerbs.Get)]
        public ContentResult Contacts()
        {
            // Metadata
            //
            SqlBuilder builder = SqlBuilder.Select(500)
                .From("Contact")
                .Column("ContactID")
                .Column("Name")
                .Column("Telephone")
                .Column("WorkEmail")
                .Column("ModifiedOn")
                .WithMetadata().InnerJoin("AccountID")
                .Column("Name", "AccountName")
                .From("Contact")
                //.Where<string>("Contact","WorkEmail", SqlOperators.NotNull,null)
                //.And<string>("Account","AccountID", SqlOperators.NotNull,null)
                .Builder();

            ResultTable result = builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
            return Content(SerializationExtensions.ToJson<dynamic>(result), "application/json");
            
            // Object
            //SqlBuilder builder = SqlBuilder.Select().WithMetadata<Contact>()
            //    .Column(c => c.ContactID)
            //    .Column(c => c.Name)
            //    .Column(c => c.Telephone)
            //    .Column(c => c.WorkEmail)
            //    .Column(c => c.ModifiedOn)
            //    .InnerJoin(c => c.AccountID)
            //    .Column(c => c.AccountName, "Name")
            //    .Builder();

            //List<Contact> contacts = builder.List<Contact>();
            //return Content(SerializationExtensions.ToJson<dynamic>(contacts), "application/json");

            
            
            
            

        }
    }
}