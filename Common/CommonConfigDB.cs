using AzureFunctionApp.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Common
{
    public class CommonConfigDB
    {

        private static async Task<string> ReadCommonConfigDB(string connectionString, string ParamKeyword)
        {
            string ParamValue = null;
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.AppendFormat(" select * from MS_CONFIG_PARAM where ACTIVE_FLAG = 'Y' ");
            queryBuilder.AppendFormat(" and PARAM_KEYWORD = '{0}' ", ParamKeyword);
            string queryString = queryBuilder.ToString();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(queryString, connection);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        IDataRecord record = (IDataRecord)reader;
                        ParamValue = QueryUtils.getValueAsString(record, "PARAM_VALUE");
                    }
                    // Call Close when done reading.
                    reader.Close();
                }
            }

            return ParamValue;
        }

        public static async Task<InterfaceSapConfig> getInterfaceSapConfigAsync(string connectionString, ILogger logger)
        {
            InterfaceSapConfig interfaceSapConfig = new InterfaceSapConfig();
            interfaceSapConfig.InterfaceSapUrl = await ReadCommonConfigDB(connectionString, "INTERFACE_SAP_URL");
            interfaceSapConfig.InterfaceSapUser = await ReadCommonConfigDB(connectionString, "INTERFACE_SAP_USER");

            logger.LogDebug("INTERFACE_SAP_BATCH_URL:" + interfaceSapConfig.InterfaceSapUrl);
            Console.WriteLine("INTERFACE_SAP_BATCH_URL:" + interfaceSapConfig.InterfaceSapUrl);
            logger.LogDebug("INTERFACE_SAP_USER:" + interfaceSapConfig.InterfaceSapUser);
            Console.WriteLine("INTERFACE_SAP_USER:" + interfaceSapConfig.InterfaceSapUser);
            return interfaceSapConfig;
        }


    }

    public class InterfaceSapConfig
    {
        public string InterfaceSapUrl { get; set; }
        public string InterfaceSapUser { get; set; }
        public string InterfaceSapPwd { get; set; }
        public string ReqKey { get; set; }
    }
}
