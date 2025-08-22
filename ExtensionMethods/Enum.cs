using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Unleasharp.DB.PostgreSQL.ExtensionMethods;
public static class EnumExtensions {
	public static string GetPgName(this Enum enumValue) {
		FieldInfo              field = enumValue.GetType().GetField(enumValue.ToString());
		PgNameAttribute[] attributes = field.GetCustomAttributes(typeof(PgNameAttribute), false) as PgNameAttribute[];

		if (attributes != null && attributes.Any()) {
			return attributes.First().PgName;
		}

		return enumValue.ToString();
	}
}
