using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Utils
{
    public class QueryUtils
    {

        public static void configParameter(SqlCommand command)
        {
            foreach (SqlParameter Parameter in command.Parameters)
            {
                if (Parameter.Value == null)
                {
                    Parameter.Value = DBNull.Value;
                }
            }
        }


        public static string getValueAsString(IDataRecord record, string columnName)
        {
            if (record.GetValue(record.GetOrdinal(columnName)) == null)
            {
                return "";
            }
            return record.GetValue(record.GetOrdinal(columnName)).ToString();
        }
        public static Decimal? getValueAsDecimal(IDataRecord record, string columnName)
        {
            var val = record.GetValue(record.GetOrdinal(columnName));
            return val == null || String.IsNullOrEmpty(val.ToString()) ? null : Convert.ToDecimal(val.ToString());

        }

        public static Decimal getValueAsDecimalRequired(IDataRecord record, string columnName)
        {
            var val = record.GetValue(record.GetOrdinal(columnName));
            return Convert.ToDecimal(val.ToString());

        }

        public static DateTime? getValueAsDateTime(IDataRecord record, string columnName)
        {
            var val = record.GetValue(record.GetOrdinal(columnName));
            return val == null || String.IsNullOrEmpty(val.ToString()) ? null : Convert.ToDateTime(val.ToString());

        }

        public static DateTime getValueAsDateTimeRequired(IDataRecord record, string columnName)
        {
            var val = record.GetValue(record.GetOrdinal(columnName));
            return Convert.ToDateTime(val.ToString());

        }

    }
}
