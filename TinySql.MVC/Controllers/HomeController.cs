using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Web;
using System.Web.Mvc;
using TinySql;
using TinySql.Metadata;
using TinySql.MVC.Models;

namespace TinySql.MVC.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Contacts()
        {
            //SqlBuilder builder = SqlBuilder.Select()
            //    .From("Contact")
            //    .AllColumns()
            //    .Builder();
            
            // List<Expression<Func<TClass, TProperty>>> list = new List<Expression<Func<TClass, TProperty>>>();



            
            return View();
        }


        public ActionResult Index()
        {
            return View();
        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }
    }
}