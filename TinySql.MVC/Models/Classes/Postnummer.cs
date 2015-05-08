using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Postnummer
{
		[PK]
		public Decimal  Postnr { get; set; }

		public String  Bynavn { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		[FK("Kommune","KommuneID","dbo","Kommune_Postnummer_KommuneID")]
		public Decimal  KommuneID { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

	}
}
