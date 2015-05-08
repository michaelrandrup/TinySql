using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class State
{
		[PK]
		public Decimal  StateID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Description { get; set; }

		public Boolean  IsActive { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

	}
}
