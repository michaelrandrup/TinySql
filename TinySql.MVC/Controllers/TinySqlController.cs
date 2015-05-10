using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TinySql.MVC.Models;
using TinySql.Serialization;
using TinySql;
using TinySql.Metadata;

namespace TinySql.MVC.Controllers
{
    public class TinySqlController : Controller
    {
        // GET: TinySql
        public ActionResult Index()
        {
            return View();
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
                .Where<string>("Contact","WorkEmail", SqlOperators.NotNull,null)
                .And<string>("Account","AccountID", SqlOperators.NotNull,null)
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