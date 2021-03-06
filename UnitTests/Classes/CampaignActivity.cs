using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class CampaignActivity
{
		[PK]
		public Decimal  CampaignActivityID { get; set; }

		public Decimal  CampaignActivityTypeID { get; set; }

		[FK("Campaign","CampaignID","dbo","Campaign_CampaignActivity_CampaignID")]
		public Decimal  CampaignID { get; set; }

		[FK("Contact","ContactID","dbo","Contact_CampaignActivity_ContactID")]
		public Decimal  ContactID { get; set; }

		public Nullable<Int32>  Count { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Description { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

		public DateTime  RegisteredOn { get; set; }

		public String  Url { get; set; }

	}
}
