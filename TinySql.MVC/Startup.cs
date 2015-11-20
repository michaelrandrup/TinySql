using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(TinySql.MVC.Startup))]
namespace TinySql.MVC
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
