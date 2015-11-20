using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class SystemUserRole
{
		[PK]
		public Decimal  SystemUserRoleID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[FK("SystemRole","SystemRoleID","dbo","RefSystemRole8")]
		public Decimal  SystemRoleID { get; set; }

		[FK("SystemUser","SystemUserID","dbo","SystemUser_SystemUserRole_SystemUserID")]
		public Decimal  SystemUserID { get; set; }

	}
}
