using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Activity
{
		[PK]
		public Decimal  ActivityID { get; set; }

		[FK("Checkkode","CheckID","dbo","FK_ActivityStatusID_Checkkode_CheckID")]
		public Decimal  ActivityStatusID { get; set; }

		[FK("Checkkode","CheckID","dbo","FK_ActivityTypeID_Checkkode_CheckID")]
		public Decimal  ActivityTypeID { get; set; }

		[FK("Contact","ContactID","dbo","Contact_Activity_ContactID")]
		public Decimal  ContactID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public DateTime  Date { get; set; }

		public String  Description { get; set; }

		public Nullable<Int32>  DurationMinutes { get; set; }

		public Boolean  HasAttachment { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[FK("Organisation","OrganisationsID","dbo","FK_Activity_Organisation")]
		public Decimal  OwningBusinessUnitID { get; set; }

		[FK("SystemUser","SystemUserID","dbo","FK_Activity_SystemUser")]
		public Decimal  OwningUserID { get; set; }

		public String  Title { get; set; }

	}
}
