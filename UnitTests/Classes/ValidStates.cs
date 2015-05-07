using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class ValidStates
{
		[PK]
		public Decimal  ValidStatesID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  EntityID { get; set; }

		[FK("State","StateID","dbo","State_ValidStates_FromStateID")]
		public Decimal  FromStateID { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[FK("State","StateID","dbo","RefState126")]
		public Decimal  ToStateID { get; set; }

	}
}
