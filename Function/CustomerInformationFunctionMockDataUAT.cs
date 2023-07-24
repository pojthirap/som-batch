using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using AzureFunctionApp.common;
using AzureFunctionApp.EnumVal;
using AzureFunctionApp.Model.Request.OutboundCustomerInformationMockDataUATRequest;
using AzureFunctionApp.Model.Response.OutboundCustomerInformationResponse;
using AzureFunctionApp.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using AzureFunctionApp.Common;

namespace AzureFunctionApp
{
    public  class CustomerInformationFunctionMockDataUAT
    {
        private readonly IConfiguration _configuration;
        private string connectionString_ = null;
        private string interfaceSapPassword_ = null;
        private string interfaceSapReqKey_ = null;
        public CustomerInformationFunctionMockDataUAT(IConfiguration configuration)
        {
            _configuration = configuration;
            string keyValue = _configuration[CommonConstant.KEYVALUE_TXT];
            string connectionString = _configuration[CommonConstant.CONNECTIONSTRING_TEXT];
            connectionString_ = String.IsNullOrEmpty(keyValue) ? connectionString : keyValue;

            string keyValueInterfaceSapPassword = _configuration[CommonConstant.KEYVALUE_SAP_INTERFACE_PASSWORD_TXT];
            string sapInterfacePassword = _configuration[CommonConstant.SAP_INTERFACE_PASSWORD_TEXT];
            interfaceSapPassword_ = String.IsNullOrEmpty(keyValueInterfaceSapPassword) ? sapInterfacePassword : keyValueInterfaceSapPassword;

            string keyValueInterfaceSapReqKey = _configuration[CommonConstant.KEYVALUE_SAP_INTERFACE_REQ_KEY_TXT];
            string sapInterfaceReqKey = _configuration[CommonConstant.SAP_INTERFACE_REQ_KEY_TEXT];
            interfaceSapReqKey_ = String.IsNullOrEmpty(keyValueInterfaceSapReqKey) ? sapInterfaceReqKey : keyValueInterfaceSapReqKey;

            Console.WriteLine("keyValue:" + keyValue);
            Console.WriteLine("connectionString:" + connectionString_);
            Console.WriteLine("keyValueInterfaceSapPassword:" + keyValueInterfaceSapPassword);
            Console.WriteLine("interfaceSapPassword_:" + interfaceSapPassword_);
            Console.WriteLine("keyValueInterfaceSapReqKey:" + keyValueInterfaceSapReqKey);
            Console.WriteLine("interfaceSapReqKey_:" + interfaceSapReqKey_);

        }

        [Function("CustomerInformationFunctionMockDataUAT")]
        public  async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(CommonConstant.GetLoggerString);
            logger.LogInformation("C# HTTP trigger function processed a request. CustomerInformationFunctionMockDataUAT");

            var responses = req.CreateResponse(HttpStatusCode.OK);
            responses.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            var queryDictionary = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(req.Url.Query);
            string Start_Date = queryDictionary.Count == 0 ? "" : queryDictionary["Start_Date"];
            string Start_Time = queryDictionary.Count == 0 ? "" : queryDictionary["Start_Time"];
            string End_Date = queryDictionary.Count == 0 ? "" : queryDictionary["End_Date"];
            string End_Time = queryDictionary.Count == 0 ? "" : queryDictionary["End_Time"];

            responses.WriteString("connectionString : " + connectionString_);

            // Get Config From Database
            InterfaceSapConfig ic = await CommonConfigDB.getInterfaceSapConfigAsync(connectionString_, logger);
            ic.InterfaceSapPwd = interfaceSapPassword_;
            ic.ReqKey = interfaceSapReqKey_;

            // SQL Scope
            string VAL_SYNC_DATA_LOG_SEQ = "";
            using (SqlConnection connection = new SqlConnection(connectionString_))
            {
                connection.Open();
                // Create SQL
                String sql = "select NEXT VALUE FOR SYNC_DATA_LOG_SEQ as SEQ_";
                SqlCommand command = connection.CreateCommand();
                // Execute SQL
                logger.LogDebug("Query:" + sql);
                Console.WriteLine("Query:" + sql);
                command.CommandText = sql;
                
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        VAL_SYNC_DATA_LOG_SEQ = reader.GetValue(reader.GetOrdinal("SEQ_")).ToString();
                    }
                }


                command = connection.CreateCommand();
                SqlTransaction transaction;

                // Start a local transaction.
                transaction = connection.BeginTransaction("T1");

                // Must assign both transaction object and connection
                // to Command object for a pending local transaction
                command.Connection = connection;
                command.Transaction = transaction;

                try
                {
                    // INSERT SYNC_DATA_LOG
                    // Create SQL
                    command.Parameters.Clear();
                    StringBuilder bulder = new StringBuilder();
                    bulder.AppendFormat(" INSERT INTO SYNC_DATA_LOG ([SYNC_ID], [INTERFACE_ID], [CREATE_USER], [CREATE_DTM]) ");
                    bulder.AppendFormat(" VALUES(@VAL_SYNC_DATA_LOG_SEQ, 'ZSOMI011', 'SYSTEM', dbo.GET_SYSDATETIME()); ");
                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;

                    // Execute SQL
                    logger.LogDebug("Query:" + bulder.ToString());
                    Console.WriteLine("Query:" + bulder.ToString());
                    command.CommandText = bulder.ToString();
                    Int32 rowsAffected = command.ExecuteNonQuery();
                    logger.LogDebug("RowsAffected:" + rowsAffected);
                    Console.WriteLine("RowsAffected:" + rowsAffected);

                    // Attempt to commit the transaction.
                    transaction.Commit();
                    logger.LogDebug("Commit to database.");
                    Console.WriteLine("Commit to database.");
                }
                catch (Exception ex)
                {
                    logger.LogDebug("Commit Exception Type: {0}", ex.GetType());
                    logger.LogDebug("  Message: {0} {1}", ex.Message, ex.ToString());
                    Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                    Console.WriteLine("  Message: {0} {1}", ex.Message, ex.ToString());
                    responses = req.CreateResponse(HttpStatusCode.InternalServerError);
                    // Attempt to roll back the transaction.
                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception ex2)
                    {
                        // This catch block will handle any errors that may have occurred
                        // on the server that would cause the rollback to fail, such as
                        // a closed connection.
                        logger.LogDebug("Rollback Exception Type: {0}", ex2.GetType());
                        logger.LogDebug("  Message: {0} {1}", ex2.Message, ex2.ToString());

                        Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                        Console.WriteLine("  Message: {0} {1}", ex2.Message, ex2.ToString());
                        responses = req.CreateResponse(HttpStatusCode.InternalServerError);
                        throw;
                    }
                }
            }
            // SQL Scope


            using (var client = new HttpClient())
            {
                /*OutboundCustomerInformationMockDataUATRequest reqData = new OutboundCustomerInformationMockDataUATRequest();
                RequestInput input = new RequestInput();
                input.Interface_ID = "ZSOMI011";
                input.Table_Object = "XD02";
                input.Start_Date = long.Parse(Start_Date);// 20180124;//--PARAM FORMAT EX 20170124 (YYYYMMDD)
                input.Start_Time = long.Parse(Start_Time);// 000001;//--PARAM FORMAT EX 000001 (HHMISS)
                input.End_Date = long.Parse(End_Date);// 20180424;//--PARAM FORMAT EX 20170124 (YYYYMMDD)
                input.End_Time = long.Parse(End_Time);// 235959;//--PARAM FORMAT EX 235959 (HHMISS)
                List<DivisionInput> divisionInput = new List<DivisionInput>();
                DivisionInput d = new DivisionInput();
                d.Division = "10";//--Fix
                divisionInput.Add(d);
                d = new DivisionInput();
                d.Division = "11";//--Fix
                divisionInput.Add(d);
                d = new DivisionInput();
                d.Division = "12";//--Fix
                divisionInput.Add(d);
                reqData.Division = divisionInput;
                reqData.Input = input;*/

                //
                OutboundCustomerInformationMockDataUATRequest reqData = new OutboundCustomerInformationMockDataUATRequest();
                string fileName = "D:\\WORK\\IWIZ\\WORKSPACES\\PT\\CustomerInformationFunctionMockDataUAT.json";
                using FileStream openStream = File.OpenRead(fileName);
                try
                {
                    reqData = await System.Text.Json.JsonSerializer.DeserializeAsync<OutboundCustomerInformationMockDataUATRequest>(openStream);
                }
                catch (Exception ex)
                {
                    logger.LogDebug("Commit Exception Type: {0}", ex.GetType());
                    logger.LogDebug("  Message: {0} {1}", ex.Message, ex.ToString());
                    Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                    Console.WriteLine("  Message: {0} {1}", ex.Message, ex.ToString());
                    //
                    throw;
                }
                client.BaseAddress = new Uri(ic.InterfaceSapUrl + CommonConstant.API_OutboundCustomerInformation);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{ic.InterfaceSapUser}:{ic.InterfaceSapPwd}")));
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.Add("User-Agent", CommonConstant.USER_AGEN);
                client.DefaultRequestHeaders.Add(CommonConstant.ReqKey, ic.ReqKey);
                client.Timeout = TimeSpan.FromSeconds(Convert.ToDouble(CommonConstant.API_TIMEOUT));
                Console.WriteLine(client.DefaultRequestHeaders.ToString());
                Console.WriteLine(client.DefaultRequestHeaders.GetValues("req-key"));
                logger.LogInformation(client.DefaultRequestHeaders.ToString());
                //logger.LogInformation(client.DefaultRequestHeaders.GetValues("req-key"));
                string fullPath = ic.InterfaceSapUrl + CommonConstant.API_OutboundCustomerInformation;
                var jsonVal = JsonConvert.SerializeObject(reqData);
                var content = new StringContent(jsonVal, Encoding.UTF8, "application/json");
                logger.LogDebug("Call Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundCustomerInformation);
                logger.LogDebug("=========== jsonVal ================");
                logger.LogDebug("REQUEST:" + jsonVal);
                logger.LogDebug("REQUEST:" + JObject.Parse(jsonVal));
                Console.WriteLine("Call Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundCustomerInformation);
                Console.WriteLine("=========== jsonVal ================");
                Console.WriteLine("REQUEST:" + jsonVal);
                Console.WriteLine("REQUEST:" + JObject.Parse(jsonVal));

                // Cal use time
                // Create new stopwatch.
                Stopwatch stopwatch = new Stopwatch();

                // Begin timing.
                stopwatch.Start();
                //
                HttpResponseMessage response = await client.PostAsync(fullPath, content);

                // Stop timing.
                stopwatch.Stop();

                // Write result.
                logger.LogDebug("Time elapsed: {0}", stopwatch.Elapsed);
                logger.LogDebug("Time Use: {0}", stopwatch.Elapsed);
                Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
                Console.WriteLine("Time Use: {0}", stopwatch.Elapsed);

                OutboundCustomerInformationResponse resultOutbound = null;
                // SQL Scope
                using (SqlConnection connection = new SqlConnection(connectionString_))
                {
                    connection.Open();


                    SqlCommand command = connection.CreateCommand();
                    SqlTransaction transaction;

                    // Start a local transaction.
                    transaction = connection.BeginTransaction("T2");

                    // Must assign both transaction object and connection
                    // to Command object for a pending local transaction
                    command.Connection = connection;
                    command.Transaction = transaction;

                    try
                    {
                        resultOutbound = new OutboundCustomerInformationResponse();
                        List<Data> lst = new List<Data>();

                        if (response.IsSuccessStatusCode) // Call Success
                        {
                            string result = response.Content.ReadAsStringAsync().Result;
                            var dynamicData = JObject.Parse(result);
                            Console.WriteLine("Response Data");
                            Console.WriteLine(dynamicData);
                            //logger.LogDebug("Response Data");
                            //logger.LogDebug(dynamicData);
                            resultOutbound = await response.Content.ReadAsAsync<OutboundCustomerInformationResponse>();
                            lst = resultOutbound.GeneralData;
                            if (!"S".Equals(resultOutbound.Header.Status))
                            {
                                // Create SQL
                                command.Parameters.Clear();
                                StringBuilder bulder = new StringBuilder();
                                bulder.AppendFormat(" UPDATE SYNC_DATA_LOG  ");
                                bulder.AppendFormat(" SET [ERROR_DESC]=@Message, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                bulder.AppendFormat(" WHERE [SYNC_ID]= @VAL_SYNC_DATA_LOG_SEQ ");
                                command.Parameters.Add("@Message", SqlDbType.NVarChar);
                                command.Parameters["@Message"].Value = resultOutbound.Header.Message;
                                command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                QueryUtils.configParameter(command);
                                // Execute SQL
                                logger.LogDebug("Query:" + bulder.ToString());
                                Console.WriteLine("Query:" + bulder.ToString());
                                command.CommandText = bulder.ToString();
                                Int32 rowsAffected = command.ExecuteNonQuery();
                                logger.LogDebug("RowsAffected:" + rowsAffected);
                                Console.WriteLine("RowsAffected:" + rowsAffected);
                                //EXIT Program
                            }
                        }
                        else
                        { // Call Error

                            //var ErrMsg = JsonConvert.DeserializeObject<dynamic>(response.Content.ReadAsStringAsync().Result);
                            string ErrMsg = response.Content.ReadAsStringAsync().Result;

                            // Create SQL
                            command.Parameters.Clear();
                            StringBuilder bulder = new StringBuilder();
                            bulder.AppendFormat(" UPDATE SYNC_DATA_LOG  ");
                            bulder.AppendFormat(" SET [ERROR_DESC]=@ErrMsg, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                            bulder.AppendFormat(" WHERE [SYNC_ID]= @VAL_SYNC_DATA_LOG_SEQ ");
                            command.Parameters.Add("@ErrMsg", SqlDbType.NVarChar);
                            command.Parameters["@ErrMsg"].Value = ErrMsg;
                            command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                            command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                            QueryUtils.configParameter(command);
                            // Execute SQL
                            logger.LogDebug("Query:" + bulder.ToString());
                            Console.WriteLine("Query:" + bulder.ToString());
                            command.CommandText = bulder.ToString();
                            Int32 rowsAffected = command.ExecuteNonQuery();
                            logger.LogDebug("RowsAffected:" + rowsAffected);
                            Console.WriteLine("RowsAffected:" + rowsAffected);
                            //EXIT Program
                        }

                        logger.LogDebug("Response Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundCustomerInformation);
                        logger.LogDebug("Response:" + JsonConvert.SerializeObject(resultOutbound));
                        Console.WriteLine("Response Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundCustomerInformation);
                        Console.WriteLine("Response:" + JsonConvert.SerializeObject(resultOutbound));



                        // Attempt to commit the transaction.
                        transaction.Commit();
                        logger.LogDebug("Commit to database.");
                        Console.WriteLine("Commit to database.");
                    }
                    catch (Exception ex)
                    {
                        // Create SQL
                        command.Parameters.Clear();
                        StringBuilder bulder = new StringBuilder();
                        bulder.AppendFormat(" UPDATE SYNC_DATA_LOG  ");
                        bulder.AppendFormat(" SET [ERROR_DESC]=@ErrMsg, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                        bulder.AppendFormat(" WHERE [SYNC_ID]= @VAL_SYNC_DATA_LOG_SEQ ");
                        command.Parameters.Add("@ErrMsg", SqlDbType.NVarChar);
                        command.Parameters["@ErrMsg"].Value = (ex.Message + ":" + ex.ToString());
                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                        // Execute SQL
                        logger.LogDebug("Query:" + bulder.ToString());
                        Console.WriteLine("Query:" + bulder.ToString());
                        command.CommandText = bulder.ToString();
                        Int32 rowsAffected = command.ExecuteNonQuery();
                        logger.LogDebug("RowsAffected:" + rowsAffected);
                        Console.WriteLine("RowsAffected:" + rowsAffected);

                        logger.LogDebug("Commit Exception Type: {0}", ex.GetType());
                        logger.LogDebug("  Message: {0} {1}", ex.Message, ex.ToString());
                        Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                        Console.WriteLine("  Message: {0} {1}", ex.Message, ex.ToString());
                        responses = req.CreateResponse(HttpStatusCode.InternalServerError);

                        // Attempt to commit the transaction.
                        transaction.Commit();
                        logger.LogDebug("Commit to database.");
                        Console.WriteLine("Commit to database.");
                        responses = req.CreateResponse(HttpStatusCode.InternalServerError);
                        throw;
                        // Attempt to roll back the transaction.
                        /*try
                        {
                            transaction.Rollback();
                        }
                        catch (Exception ex2)
                        {
                            // This catch block will handle any errors that may have occurred
                            // on the server that would cause the rollback to fail, such as
                            // a closed connection.
                            logger.LogDebug("Rollback Exception Type: {0}", ex2.GetType());
                            logger.LogDebug("  Message: {0}", ex2.Message);
                            
                        }*/
                    }
                }
                // SQL Scope


                if (resultOutbound != null)// Process Data 
                {
                    SqlTransaction transaction = null;
                    try
                    {
                        // SQL Scope
                        using (SqlConnection connection = new SqlConnection(connectionString_))
                        {
                            connection.Open();

                            SqlCommand command = connection.CreateCommand();

                            // Start a local transaction.
                            transaction = connection.BeginTransaction("T3");

                            // Must assign both transaction object and connection
                            // to Command object for a pending local transaction
                            command.Connection = connection;
                            command.Transaction = transaction;


                            // Begin Process In Database
                            if (resultOutbound.GeneralData != null && resultOutbound.GeneralData.Count != 0)
                            {
                                StringBuilder bulder = null;
                                Int32 rowsAffected = 0;
                                foreach (Data data in resultOutbound.GeneralData)
                                {

                                    if ("D".Equals(data.Status_IND))
                                    {
                                        // UPDATE CUSTOMER
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE CUSTOMER  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);


                                        // UPDATE CUSTOMER_COMPANY
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE CUSTOMER_COMPANY  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);

                                        // UPDATE CUSTOMER_SALE
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE CUSTOMER_SALE  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);

                                        // UPDATE CUSTOMER_PARTNER
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE CUSTOMER_PARTNER  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);


                                    }
                                    else
                                    {
                                        if(!data.Customer.Substring(0,1).Equals("9")) {
                                        /*
                                        // INSERT SYNC_DATA_LOG
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE ACC SET ACC.CUST_CODE=@Customer, ACC.IDENTIFY_ID=@VAT_Registration_No, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" FROM PROSPECT_ACCOUNT ACC ");
                                        bulder.AppendFormat(" INNER JOIN PROSPECT PP on PP.PROSP_ACC_ID = ACC.PROSP_ACC_ID ");
                                        bulder.AppendFormat(" WHERE PP.PROSPECT_ID =@Location_Code ");
                                        bulder.AppendFormat(" and((ACC.CUST_CODE = @Customer) OR(ACC.CUST_CODE IS NULL and not exists(select 1 from PROSPECT_ACCOUNT where CUST_CODE = @Customer))) ");
                                        //bulder.AppendFormat(" and ACC.CUST_CODE IS NULL ");
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        command.Parameters.Add("@VAT_Registration_No", SqlDbType.NVarChar);
                                        command.Parameters["@VAT_Registration_No"].Value = data.VAT_Registration_No;
                                        command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                        command.Parameters["@Location_Code"].Value = data.Location_Code;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);
                                        */

                                        //
                                        List<ProspectAccount> prospectAccountList = new List<ProspectAccount>();
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" select ACC.PROSP_ACC_ID,ACC.CUST_CODE ");
                                        bulder.AppendFormat(" FROM PROSPECT_ACCOUNT ACC ");
                                        bulder.AppendFormat(" INNER JOIN PROSPECT PP on PP.PROSP_ACC_ID = ACC.PROSP_ACC_ID ");
                                        bulder.AppendFormat(" WHERE PP.PROSPECT_ID = @Location_Code ");
                                        command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                        command.Parameters["@Location_Code"].Value = data.Location_Code;
                                        QueryUtils.configParameter(command);
                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();

                                        using (SqlDataReader reader = command.ExecuteReader())
                                        {
                                            ProspectAccount prospectAccountdata;
                                            while (reader.Read())
                                            {
                                                prospectAccountdata = new ProspectAccount();
                                                prospectAccountdata.prospAccId = reader.GetValue(reader.GetOrdinal("PROSP_ACC_ID")).ToString();
                                                prospectAccountdata.custCode = reader.GetValue(reader.GetOrdinal("CUST_CODE")).ToString();
                                                prospectAccountList.Add(prospectAccountdata);
                                            }
                                        }
                                        rowsAffected = 0;
                                        if (prospectAccountList.Count != 0)
                                        {
                                            foreach (ProspectAccount p in prospectAccountList)
                                            {
                                                if (String.IsNullOrEmpty(p.custCode))
                                                {
                                                    string resultPA02_PROSP_ACC_ID = null;
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" select PROSP_ACC_ID from PROSPECT_ACCOUNT where CUST_CODE = @Customer  ");
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = data.Customer;
                                                    QueryUtils.configParameter(command);
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();

                                                    using (SqlDataReader reader = command.ExecuteReader())
                                                    {
                                                        while (reader.Read())
                                                        {
                                                            resultPA02_PROSP_ACC_ID = reader.GetValue(reader.GetOrdinal("PROSP_ACC_ID")).ToString();
                                                        }
                                                    }
                                                    if (String.IsNullOrEmpty(resultPA02_PROSP_ACC_ID))//resultPA02 is not found 
                                                    {

                                                        // Create SQL
                                                        command.Parameters.Clear();
                                                        bulder = new StringBuilder();
                                                        bulder.AppendFormat(" UPDATE ACC SET ACC.CUST_CODE=@Customer, ACC.IDENTIFY_ID=@VAT_Registration_No, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                        bulder.AppendFormat(" FROM PROSPECT_ACCOUNT ACC  ");
                                                        //bulder.AppendFormat(" where ACC.PROSP_ACC_ID = @Location_Code ");
                                                        bulder.AppendFormat(" where ACC.PROSP_ACC_ID = @resultPA01_PROSP_ACC_ID ");
                                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                        command.Parameters["@Customer"].Value = data.Customer;
                                                        command.Parameters.Add("@VAT_Registration_No", SqlDbType.NVarChar);
                                                        command.Parameters["@VAT_Registration_No"].Value = data.VAT_Registration_No;
                                                        //command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                                        //command.Parameters["@Location_Code"].Value = data.Location_Code;
                                                        command.Parameters.Add("@resultPA01_PROSP_ACC_ID", SqlDbType.Decimal);
                                                        command.Parameters["@resultPA01_PROSP_ACC_ID"].Value = p.prospAccId;
                                                        QueryUtils.configParameter(command);
                                                        // Execute SQL
                                                        logger.LogDebug("Query:" + bulder.ToString());
                                                        Console.WriteLine("Query:" + bulder.ToString());
                                                        command.CommandText = bulder.ToString();
                                                        rowsAffected = command.ExecuteNonQuery();
                                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                                        Console.WriteLine("RowsAffected:" + rowsAffected);
                                                    }
                                                }else if (p.custCode != null && p.custCode.Equals(data.Customer))
                                                {

                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();

                                                    bulder.AppendFormat(" UPDATE ACC SET ACC.IDENTIFY_ID=@VAT_Registration_No, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                    bulder.AppendFormat(" FROM PROSPECT_ACCOUNT ACC  ");
                                                    //bulder.AppendFormat(" where ACC.PROSP_ACC_ID = @Location_Code ");
                                                    bulder.AppendFormat(" where ACC.PROSP_ACC_ID = @resultPA01_PROSP_ACC_ID ");
                                                    //command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                                    //command.Parameters["@Location_Code"].Value = data.Location_Code;
                                                    command.Parameters.Add("@resultPA01_PROSP_ACC_ID", SqlDbType.Decimal);
                                                    command.Parameters["@resultPA01_PROSP_ACC_ID"].Value = p.prospAccId;
                                                    command.Parameters.Add("@VAT_Registration_No", SqlDbType.NVarChar);
                                                    command.Parameters["@VAT_Registration_No"].Value = data.VAT_Registration_No;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);

                                                }
                                                
                                            }
                                        }


                                        //

                                        if (rowsAffected > 0)
                                        {
                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" UPDATE PROSPECT SET PROSPECT_TYPE = @PROSPECT_TYPE_CUSTOMER WHERE PROSPECT_ID =@Location_Code ");//--Note PROSPECT_TYPE.CUSTOMER-- ตรงนี้ทำเป็น ENUM Value = '2' Fix
                                            command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                            command.Parameters["@Location_Code"].Value = data.Location_Code;
                                            command.Parameters.Add("@PROSPECT_TYPE_CUSTOMER", SqlDbType.NVarChar);
                                            command.Parameters["@PROSPECT_TYPE_CUSTOMER"].Value = ProspectType.CUSTOMER.ToString("d");
                                            QueryUtils.configParameter(command);
                                            // Execute SQL
                                            logger.LogDebug("Query:" + bulder.ToString());
                                            Console.WriteLine("Query:" + bulder.ToString());
                                            command.CommandText = bulder.ToString();
                                            rowsAffected = command.ExecuteNonQuery();
                                            logger.LogDebug("RowsAffected:" + rowsAffected);
                                            Console.WriteLine("RowsAffected:" + rowsAffected);




                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" UPDATE PROSPECT_ADDRESS  ");
                                            bulder.AppendFormat(" SET [ADDR_NO]=NULL, [MOO]=NULL, [SOI]=NULL, [STREET]=@Street, [TELL_NO]=@Telephone_1, [PROVINCE_CODE]=@Region, [DISTRICT_CODE]=@City, [SUBDISTRICT_CODE]=@District, [POST_CODE]=@Postal_Code, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                            bulder.AppendFormat(" WHERE [PROSPECT_ID] = @Location_Code ");
                                            bulder.AppendFormat(" AND [MAIN_FLAG]= 'Y' ");
                                            command.Parameters.Add("@Street", SqlDbType.NVarChar);
                                            command.Parameters["@Street"].Value = data.Street;
                                            command.Parameters.Add("@Telephone_1", SqlDbType.NVarChar);
                                            command.Parameters["@Telephone_1"].Value = data.Telephone_1;
                                            command.Parameters.Add("@Region", SqlDbType.NVarChar);
                                            command.Parameters["@Region"].Value = data.Region;
                                            command.Parameters.Add("@City", SqlDbType.NVarChar);
                                            command.Parameters["@City"].Value = data.City;
                                            command.Parameters.Add("@District", SqlDbType.NVarChar);
                                            command.Parameters["@District"].Value = data.District;
                                            command.Parameters.Add("@Postal_Code", SqlDbType.NVarChar);
                                            command.Parameters["@Postal_Code"].Value = data.Postal_Code;
                                            command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                            command.Parameters["@Location_Code"].Value = data.Location_Code;
                                            QueryUtils.configParameter(command);
                                            // Execute SQL
                                            logger.LogDebug("Query:" + bulder.ToString());
                                            Console.WriteLine("Query:" + bulder.ToString());
                                            command.CommandText = bulder.ToString();
                                            rowsAffected = command.ExecuteNonQuery();
                                            logger.LogDebug("RowsAffected:" + rowsAffected);
                                            Console.WriteLine("RowsAffected:" + rowsAffected);

                                        }
                                    }

                                        // UPDATE CUSTOMER
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE CUSTOMER ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [PROSPECT_ID]=iif(substring(CUST_CODE,1,1)='9',PROSPECT_ID,@Location_Code) , [ACC_GROUP_CODE]=@Account_Group, [CUST_NAME_TH]=@Name_1, [CUST_NAME_EN]=@Name_2, [SEARCH_TERM]=@Search_Term, [TRANSPORT_ZONE]=@Transportation_Zone, [TAX_NO]=@Tax_Number_3, [VAT_NO]=@VAT_Registration_No, [STREET]=@Street, [TELL_NO]=@Telephone_1, [COUNTRY_CODE]=@Country_Key, [PROVINCE_CODE]=@Region, [DISTRICT_CODE]=@City, [SUBDISTRICT_CODE]=@District, [POST_CODE]=@Postal_Code, [ACTIVE_FLAG]='Y', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                        command.Parameters["@Location_Code"].Value = data.Location_Code;
                                        command.Parameters.Add("@Account_Group", SqlDbType.NVarChar);
                                        command.Parameters["@Account_Group"].Value = data.Account_Group;
                                        command.Parameters.Add("@Name_1", SqlDbType.NVarChar);
                                        command.Parameters["@Name_1"].Value = data.Name_1;
                                        command.Parameters.Add("@Name_2", SqlDbType.NVarChar);
                                        command.Parameters["@Name_2"].Value = data.Name_2;
                                        command.Parameters.Add("@Search_Term", SqlDbType.NVarChar);
                                        command.Parameters["@Search_Term"].Value = data.Search_Term;
                                        command.Parameters.Add("@Transportation_Zone", SqlDbType.NVarChar);
                                        command.Parameters["@Transportation_Zone"].Value = data.Transportation_Zone;
                                        command.Parameters.Add("@Tax_Number_3", SqlDbType.NVarChar);
                                        command.Parameters["@Tax_Number_3"].Value = data.Tax_Number_3;
                                        command.Parameters.Add("@VAT_Registration_No", SqlDbType.NVarChar);
                                        command.Parameters["@VAT_Registration_No"].Value = data.VAT_Registration_No;
                                        command.Parameters.Add("@Street", SqlDbType.NVarChar);
                                        command.Parameters["@Street"].Value = data.Street;
                                        command.Parameters.Add("@Telephone_1", SqlDbType.NVarChar);
                                        command.Parameters["@Telephone_1"].Value = data.Telephone_1;
                                        command.Parameters.Add("@Country_Key", SqlDbType.NVarChar);
                                        command.Parameters["@Country_Key"].Value = data.Country_Key;
                                        command.Parameters.Add("@Region", SqlDbType.NVarChar);
                                        command.Parameters["@Region"].Value = data.Region;
                                        command.Parameters.Add("@City", SqlDbType.NVarChar);
                                        command.Parameters["@City"].Value = data.City;
                                        command.Parameters.Add("@District", SqlDbType.NVarChar);
                                        command.Parameters["@District"].Value = data.District;
                                        command.Parameters.Add("@Postal_Code", SqlDbType.NVarChar);
                                        command.Parameters["@Postal_Code"].Value = data.Postal_Code;
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);


                                        if (rowsAffected == 0)
                                        {
                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" INSERT INTO CUSTOMER ([CUST_CODE], [PROSPECT_ID], [ACC_GROUP_CODE], [CUST_NAME_TH], [CUST_NAME_EN], [SEARCH_TERM], [TRANSPORT_ZONE], [TAX_NO], [VAT_NO], [STREET], [TELL_NO], [COUNTRY_CODE], [PROVINCE_CODE], [DISTRICT_CODE], [SUBDISTRICT_CODE], [POST_CODE], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                            bulder.AppendFormat(" VALUES(@Customer, @Location_Code, @Account_Group, @Name_1, @Name_2, @Search_Term, @Transportation_Zone, @Tax_Number_3, @VAT_Registration_No, @Street, @Telephone_1, @Country_Key, @Region, @City, @District, @Postal_Code, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                            command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                            command.Parameters["@Customer"].Value = data.Customer;
                                            command.Parameters.Add("@Location_Code", SqlDbType.Decimal);
                                            command.Parameters["@Location_Code"].Value = data.Location_Code;
                                            command.Parameters.Add("@Account_Group", SqlDbType.NVarChar);
                                            command.Parameters["@Account_Group"].Value = data.Account_Group;
                                            command.Parameters.Add("@Name_1", SqlDbType.NVarChar);
                                            command.Parameters["@Name_1"].Value = data.Name_1;
                                            command.Parameters.Add("@Name_2", SqlDbType.NVarChar);
                                            command.Parameters["@Name_2"].Value = data.Name_2;
                                            command.Parameters.Add("@Search_Term", SqlDbType.NVarChar);
                                            command.Parameters["@Search_Term"].Value = data.Search_Term;
                                            command.Parameters.Add("@Transportation_Zone", SqlDbType.NVarChar);
                                            command.Parameters["@Transportation_Zone"].Value = data.Transportation_Zone;
                                            command.Parameters.Add("@Tax_Number_3", SqlDbType.NVarChar);
                                            command.Parameters["@Tax_Number_3"].Value = data.Tax_Number_3;
                                            command.Parameters.Add("@VAT_Registration_No", SqlDbType.NVarChar);
                                            command.Parameters["@VAT_Registration_No"].Value = data.VAT_Registration_No;
                                            command.Parameters.Add("@Street", SqlDbType.NVarChar);
                                            command.Parameters["@Street"].Value = data.Street;
                                            command.Parameters.Add("@Telephone_1", SqlDbType.NVarChar);
                                            command.Parameters["@Telephone_1"].Value = data.Telephone_1;
                                            command.Parameters.Add("@Country_Key", SqlDbType.NVarChar);
                                            command.Parameters["@Country_Key"].Value = data.Country_Key;
                                            command.Parameters.Add("@Region", SqlDbType.NVarChar);
                                            command.Parameters["@Region"].Value = data.Region;
                                            command.Parameters.Add("@City", SqlDbType.NVarChar);
                                            command.Parameters["@City"].Value = data.City;
                                            command.Parameters.Add("@District", SqlDbType.NVarChar);
                                            command.Parameters["@District"].Value = data.District;
                                            command.Parameters.Add("@Postal_Code", SqlDbType.NVarChar);
                                            command.Parameters["@Postal_Code"].Value = data.Postal_Code;
                                            command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                            command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                            QueryUtils.configParameter(command);
                                            // Execute SQL
                                            logger.LogDebug("Query:" + bulder.ToString());
                                            Console.WriteLine("Query:" + bulder.ToString());
                                            command.CommandText = bulder.ToString();
                                            rowsAffected = command.ExecuteNonQuery();
                                            logger.LogDebug("RowsAffected:" + rowsAffected);
                                            Console.WriteLine("RowsAffected:" + rowsAffected);


                                            if (data.CompanyData != null && data.CompanyData.Count != 0)
                                            {
                                                foreach (CompanyData item in data.CompanyData)
                                                {
                                                    // Table CUSTOMER_COMPANY
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO CUSTOMER_COMPANY ([CUST_COM_ID], [CUST_CODE], [COMPANY_CODE], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR CUSTOMER_COMPANY_SEQ, @Customer, @Company_Code, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = item.Customer;
                                                    command.Parameters.Add("@Company_Code", SqlDbType.NVarChar);
                                                    command.Parameters["@Company_Code"].Value = item.Company_Code;
                                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);
                                                }
                                            }


                                            if (data.SaleData != null && data.SaleData.Count != 0)
                                            {
                                                foreach (SaleData item in data.SaleData)
                                                {
                                                    // Table CUSTOMER_SALE
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO CUSTOMER_SALE ([CUST_SALE_ID], [CUST_CODE], [CUST_GROUP], [PAYMENT_TERM], [INCOTERM], [ORG_CODE], [CHANNEL_CODE], [DIVISION_CODE], [GROUP_CODE], [OFFICE_CODE], [SHIPPING_COND], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR CUSTOMER_SALE_SEQ, @Customer, @Customer_Group, @Terms_Of_Payment, @Incoterms, @Sales_Organization, @Distribution_Channel, @Division, @Sales_Group, @Sales_Office, @Shipping_Conditions, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME())  ");
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = item.Customer;
                                                    command.Parameters.Add("@Customer_Group", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer_Group"].Value = item.Customer_Group;
                                                    command.Parameters.Add("@Terms_Of_Payment", SqlDbType.NVarChar);
                                                    command.Parameters["@Terms_Of_Payment"].Value = item.Terms_Of_Payment;
                                                    command.Parameters.Add("@Incoterms", SqlDbType.NVarChar);
                                                    command.Parameters["@Incoterms"].Value = item.Incoterms;
                                                    command.Parameters.Add("@Sales_Organization", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Organization"].Value = item.Sales_Organization;
                                                    command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                    command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                    command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                                    command.Parameters["@Division"].Value = item.Division;
                                                    command.Parameters.Add("@Sales_Group", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Group"].Value = item.Sales_Group;
                                                    command.Parameters.Add("@Sales_Office", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Office"].Value = item.Sales_Office;
                                                    command.Parameters.Add("@Shipping_Conditions", SqlDbType.NVarChar);
                                                    command.Parameters["@Shipping_Conditions"].Value = item.Shipping_Conditions;
                                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);
                                                }
                                            }

                                            if (data.PartnerData != null && data.PartnerData.Count != 0)
                                            {
                                                foreach (PartnerData item in data.PartnerData)
                                                {
                                                    // Table CUSTOMER_PARTNER
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO CUSTOMER_PARTNER ([CUST_PARTNER_ID], [CUST_CODE], [ORG_CODE], [CHANNEL_CODE], [DIVISION_CODE], [FUNC_CODE], [CUST_CODE_PARTNER], [PARTNER_COUNTER], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR CUSTOMER_PARTNER_SEQ, @Customer, @Sales_Organization, @Distribution_Channel, @Division, @Partner_Function, @Customer_Partner, @Partner_Counter, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = item.Customer;
                                                    command.Parameters.Add("@Sales_Organization", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Organization"].Value = item.Sales_Organization;
                                                    command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                    command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                    command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                                    command.Parameters["@Division"].Value = item.Division;
                                                    command.Parameters.Add("@Partner_Function", SqlDbType.NVarChar);
                                                    command.Parameters["@Partner_Function"].Value = item.Partner_Function;
                                                    command.Parameters.Add("@Customer_Partner", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer_Partner"].Value = item.Customer_Partner;
                                                    command.Parameters.Add("@Partner_Counter", SqlDbType.NVarChar);
                                                    command.Parameters["@Partner_Counter"].Value = item.Partner_Counter;
                                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);
                                                }
                                            }

                                        }
                                        else
                                        {

                                            if (data.CompanyData != null && data.CompanyData.Count != 0)
                                            {
                                                foreach (CompanyData item in data.CompanyData)
                                                {
                                                    // Table CUSTOMER_COMPANY
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" UPDATE CUSTOMER_COMPANY  ");
                                                    bulder.AppendFormat(" SET [ACTIVE_FLAG]='Y', [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME()  ");
                                                    bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer and [COMPANY_CODE]=@Company_Code  ");
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = item.Customer;
                                                    command.Parameters.Add("@Company_Code", SqlDbType.NVarChar);
                                                    command.Parameters["@Company_Code"].Value = item.Company_Code;
                                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);

                                                    if (rowsAffected == 0)
                                                    {
                                                        // Table CUSTOMER_COMPANY
                                                        // Create SQL
                                                        command.Parameters.Clear();
                                                        bulder = new StringBuilder();
                                                        bulder.AppendFormat(" INSERT INTO CUSTOMER_COMPANY ([CUST_COM_ID], [CUST_CODE], [COMPANY_CODE], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                        bulder.AppendFormat(" VALUES(NEXT VALUE FOR CUSTOMER_COMPANY_SEQ, @Customer, @Company_Code, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                        command.Parameters["@Customer"].Value = item.Customer;
                                                        command.Parameters.Add("@Company_Code", SqlDbType.NVarChar);
                                                        command.Parameters["@Company_Code"].Value = item.Company_Code;
                                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                        QueryUtils.configParameter(command);
                                                        // Execute SQL
                                                        logger.LogDebug("Query:" + bulder.ToString());
                                                        Console.WriteLine("Query:" + bulder.ToString());
                                                        command.CommandText = bulder.ToString();
                                                        rowsAffected = command.ExecuteNonQuery();
                                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                                        Console.WriteLine("RowsAffected:" + rowsAffected);


                                                    }
                                                }
                                            }

                                            // Table CUSTOMER_COMPANY
                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" UPDATE CUSTOMER_COMPANY ");
                                            bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                            bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer and ISNULL(SYNC_ID,-1)  != @VAL_SYNC_DATA_LOG_SEQ ");
                                            command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                            command.Parameters["@Customer"].Value = data.Customer;
                                            command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                            command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                            QueryUtils.configParameter(command);
                                            // Execute SQL
                                            logger.LogDebug("Query:" + bulder.ToString());
                                            Console.WriteLine("Query:" + bulder.ToString());
                                            command.CommandText = bulder.ToString();
                                            rowsAffected = command.ExecuteNonQuery();
                                            logger.LogDebug("RowsAffected:" + rowsAffected);
                                            Console.WriteLine("RowsAffected:" + rowsAffected);




                                            if (data.SaleData != null && data.SaleData.Count != 0)
                                            {
                                                foreach (SaleData item in data.SaleData)
                                                {
                                                    // Table CUSTOMER_SALE
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" UPDATE CUSTOMER_SALE  ");
                                                    bulder.AppendFormat(" SET [CUST_GROUP]=@Customer_Group, [PAYMENT_TERM]=@Terms_Of_Payment, [INCOTERM]=@Incoterms, [GROUP_CODE]=@Sales_Group, [OFFICE_CODE]=@Sales_Office, [SHIPPING_COND]=@Shipping_Conditions, [ACTIVE_FLAG]='Y', [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                    bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer and [ORG_CODE]=@Sales_Organization and [CHANNEL_CODE]=@Distribution_Channel and [DIVISION_CODE]=@Division ");
                                                    command.Parameters.Add("@Customer_Group", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer_Group"].Value = item.Customer_Group;
                                                    command.Parameters.Add("@Terms_Of_Payment", SqlDbType.NVarChar);
                                                    command.Parameters["@Terms_Of_Payment"].Value = item.Terms_Of_Payment;
                                                    command.Parameters.Add("@Incoterms", SqlDbType.NVarChar);
                                                    command.Parameters["@Incoterms"].Value = item.Incoterms;
                                                    command.Parameters.Add("@Sales_Group", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Group"].Value = item.Sales_Group;
                                                    command.Parameters.Add("@Sales_Office", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Office"].Value = item.Sales_Office;
                                                    command.Parameters.Add("@Shipping_Conditions", SqlDbType.NVarChar);
                                                    command.Parameters["@Shipping_Conditions"].Value = item.Shipping_Conditions;
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = item.Customer;
                                                    command.Parameters.Add("@Sales_Organization", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Organization"].Value = item.Sales_Organization;
                                                    command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                    command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                    command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                                    command.Parameters["@Division"].Value = item.Division;
                                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);

                                                    if (rowsAffected == 0)
                                                    {
                                                        // Table CUSTOMER_SALE
                                                        // Create SQL
                                                        command.Parameters.Clear();
                                                        bulder = new StringBuilder();
                                                        bulder.AppendFormat(" INSERT INTO CUSTOMER_SALE ([CUST_SALE_ID], [CUST_CODE], [CUST_GROUP], [PAYMENT_TERM], [INCOTERM], [ORG_CODE], [CHANNEL_CODE], [DIVISION_CODE], [GROUP_CODE], [OFFICE_CODE], [SHIPPING_COND], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                        bulder.AppendFormat(" VALUES(NEXT VALUE FOR CUSTOMER_SALE_SEQ, @Customer, @Customer_Group, @Terms_Of_Payment, @Incoterms, @Sales_Organization, @Distribution_Channel, @Division, @Sales_Group, @Sales_Office, @Shipping_Conditions, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME())   ");
                                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                        command.Parameters["@Customer"].Value = item.Customer;
                                                        command.Parameters.Add("@Customer_Group", SqlDbType.NVarChar);
                                                        command.Parameters["@Customer_Group"].Value = item.Customer_Group;
                                                        command.Parameters.Add("@Terms_Of_Payment", SqlDbType.NVarChar);
                                                        command.Parameters["@Terms_Of_Payment"].Value = item.Terms_Of_Payment;
                                                        command.Parameters.Add("@Incoterms", SqlDbType.NVarChar);
                                                        command.Parameters["@Incoterms"].Value = item.Incoterms;
                                                        command.Parameters.Add("@Sales_Organization", SqlDbType.NVarChar);
                                                        command.Parameters["@Sales_Organization"].Value = item.Sales_Organization;
                                                        command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                        command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                        command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                                        command.Parameters["@Division"].Value = item.Division;
                                                        command.Parameters.Add("@Sales_Group", SqlDbType.NVarChar);
                                                        command.Parameters["@Sales_Group"].Value = item.Sales_Group;
                                                        command.Parameters.Add("@Sales_Office", SqlDbType.NVarChar);
                                                        command.Parameters["@Sales_Office"].Value = item.Sales_Office;
                                                        command.Parameters.Add("@Shipping_Conditions", SqlDbType.NVarChar);
                                                        command.Parameters["@Shipping_Conditions"].Value = item.Shipping_Conditions;
                                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                        QueryUtils.configParameter(command);
                                                        // Execute SQL
                                                        logger.LogDebug("Query:" + bulder.ToString());
                                                        Console.WriteLine("Query:" + bulder.ToString());
                                                        command.CommandText = bulder.ToString();
                                                        rowsAffected = command.ExecuteNonQuery();
                                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                                        Console.WriteLine("RowsAffected:" + rowsAffected);


                                                    }
                                                }
                                            }

                                            // Table CUSTOMER_SALE
                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" UPDATE CUSTOMER_SALE  ");
                                            bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                            bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer and ISNULL(SYNC_ID,-1)  != @VAL_SYNC_DATA_LOG_SEQ ");
                                            command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                            command.Parameters["@Customer"].Value = data.Customer;
                                            command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                            command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                            QueryUtils.configParameter(command);
                                            // Execute SQL
                                            logger.LogDebug("Query:" + bulder.ToString());
                                            Console.WriteLine("Query:" + bulder.ToString());
                                            command.CommandText = bulder.ToString();
                                            rowsAffected = command.ExecuteNonQuery();
                                            logger.LogDebug("RowsAffected:" + rowsAffected);
                                            Console.WriteLine("RowsAffected:" + rowsAffected);



                                        }

                                        if (data.PartnerData != null && data.PartnerData.Count != 0)
                                        {
                                            foreach (PartnerData item in data.PartnerData)
                                            {
                                                // Table CUSTOMER_PARTNER
                                                // Create SQL
                                                command.Parameters.Clear();
                                                bulder = new StringBuilder();

                                                bulder.AppendFormat(" UPDATE CUSTOMER_PARTNER  ");
                                                bulder.AppendFormat(" SET [CUST_CODE_PARTNER]=@Customer_Partner, [ACTIVE_FLAG]='Y', [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer and [ORG_CODE]=@Sales_Organization and [CHANNEL_CODE]=@Distribution_Channel and [DIVISION_CODE]=@Division and [FUNC_CODE]=@Partner_Function and [PARTNER_COUNTER]=@Partner_Counter ");
                                                command.Parameters.Add("@Customer_Partner", SqlDbType.NVarChar);
                                                command.Parameters["@Customer_Partner"].Value = item.Customer_Partner;
                                                command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                command.Parameters["@Customer"].Value = item.Customer;
                                                command.Parameters.Add("@Sales_Organization", SqlDbType.NVarChar);
                                                command.Parameters["@Sales_Organization"].Value = item.Sales_Organization;
                                                command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                                command.Parameters["@Division"].Value = item.Division;
                                                command.Parameters.Add("@Partner_Function", SqlDbType.NVarChar);
                                                command.Parameters["@Partner_Function"].Value = item.Partner_Function;
                                                command.Parameters.Add("@Partner_Counter", SqlDbType.NVarChar);
                                                command.Parameters["@Partner_Counter"].Value = item.Partner_Counter;
                                                command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                QueryUtils.configParameter(command);
                                                // Execute SQL
                                                logger.LogDebug("Query:" + bulder.ToString());
                                                Console.WriteLine("Query:" + bulder.ToString());
                                                command.CommandText = bulder.ToString();
                                                rowsAffected = command.ExecuteNonQuery();
                                                logger.LogDebug("RowsAffected:" + rowsAffected);
                                                Console.WriteLine("RowsAffected:" + rowsAffected);

                                                if (rowsAffected == 0)
                                                {
                                                    // Table CUSTOMER_PARTNER
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO CUSTOMER_PARTNER ([CUST_PARTNER_ID], [CUST_CODE], [ORG_CODE], [CHANNEL_CODE], [DIVISION_CODE], [FUNC_CODE], [CUST_CODE_PARTNER], [PARTNER_COUNTER], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR CUSTOMER_PARTNER_SEQ, @Customer, @Sales_Organization, @Distribution_Channel, @Division, @Partner_Function, @Customer_Partner, @Partner_Counter, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer"].Value = item.Customer;
                                                    command.Parameters.Add("@Sales_Organization", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Organization"].Value = item.Sales_Organization;
                                                    command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                    command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                    command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                                    command.Parameters["@Division"].Value = item.Division;
                                                    command.Parameters.Add("@Partner_Function", SqlDbType.NVarChar);
                                                    command.Parameters["@Partner_Function"].Value = item.Partner_Function;
                                                    command.Parameters.Add("@Customer_Partner", SqlDbType.NVarChar);
                                                    command.Parameters["@Customer_Partner"].Value = item.Customer_Partner;
                                                    command.Parameters.Add("@Partner_Counter", SqlDbType.NVarChar);
                                                    command.Parameters["@Partner_Counter"].Value = item.Partner_Counter;
                                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                                    QueryUtils.configParameter(command);
                                                    // Execute SQL
                                                    logger.LogDebug("Query:" + bulder.ToString());
                                                    Console.WriteLine("Query:" + bulder.ToString());
                                                    command.CommandText = bulder.ToString();
                                                    rowsAffected = command.ExecuteNonQuery();
                                                    logger.LogDebug("RowsAffected:" + rowsAffected);
                                                    Console.WriteLine("RowsAffected:" + rowsAffected);


                                                }
                                            }
                                        }

                                        // Table CUSTOMER_SALE
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE CUSTOMER_PARTNER  ");
                                        bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [CUST_CODE]=@Customer and ISNULL(SYNC_ID,-1)  != @VAL_SYNC_DATA_LOG_SEQ ");
                                        command.Parameters.Add("@Customer", SqlDbType.NVarChar);
                                        command.Parameters["@Customer"].Value = data.Customer;
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        QueryUtils.configParameter(command);
                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);




                                    }




                                }// end for


                                // Create SQL
                                command.Parameters.Clear();
                                bulder = new StringBuilder();
                                bulder.AppendFormat(" update P ");
                                bulder.AppendFormat(" set P.GROUP_CODE = CS.GROUP_CODE , P.UPDATE_USER = 'SYSTEM', P.UPDATE_DTM = dbo.GET_SYSDATETIME() ");
                                bulder.AppendFormat(" from PROSPECT P ");
                                bulder.AppendFormat(" inner join PROSPECT_ACCOUNT PA on P.PROSP_ACC_ID = PA.PROSP_ACC_ID ");
                                bulder.AppendFormat(" inner join CUSTOMER_SALE CS on PA.CUST_CODE = CS.CUST_CODE and CS.ACTIVE_FLAG = 'Y' ");
                                bulder.AppendFormat(" inner join ORG_SALE_AREA SA on CS.ORG_CODE = SA.ORG_CODE and CS.CHANNEL_CODE = SA.CHANNEL_CODE and CS.DIVISION_CODE = SA.DIVISION_CODE ");
                                bulder.AppendFormat(" where SA.BU_ID = P.BU_ID ");
                                bulder.AppendFormat(" and CS.SYNC_ID = @VAL_SYNC_DATA_LOG_SEQ ");
                                command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                // Execute SQL
                                logger.LogDebug("Query:" + bulder.ToString());
                                Console.WriteLine("Query:" + bulder.ToString());
                                command.CommandText = bulder.ToString();
                                rowsAffected = command.ExecuteNonQuery();
                                logger.LogDebug("RowsAffected:" + rowsAffected);
                                Console.WriteLine("RowsAffected:" + rowsAffected);


                                // Create SQL
                                command.Parameters.Clear();
                                bulder = new StringBuilder();
                                bulder.AppendFormat(" UPDATE SYNC_DATA_LOG  ");
                                bulder.AppendFormat(" SET [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                bulder.AppendFormat(" WHERE [SYNC_ID]= @VAL_SYNC_DATA_LOG_SEQ ");
                                command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                // Execute SQL
                                logger.LogDebug("Query:" + bulder.ToString());
                                Console.WriteLine("Query:" + bulder.ToString());
                                command.CommandText = bulder.ToString();
                                rowsAffected = command.ExecuteNonQuery();
                                logger.LogDebug("RowsAffected:" + rowsAffected);
                                Console.WriteLine("RowsAffected:" + rowsAffected);


                                // Attempt to commit the transaction.
                                transaction.Commit();
                                logger.LogDebug("Commit to database.");
                                Console.WriteLine("Commit to database.");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogDebug("Commit Exception Type: {0}", ex.GetType());
                        logger.LogDebug("  Message: {0} {1}", ex.Message, ex.ToString());
                        Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                        Console.WriteLine("  Message: {0} {1}", ex.Message, ex.ToString());

                        // Attempt to roll back the transaction.
                        if (transaction != null)
                        {
                            try
                            {
                                transaction.Rollback();
                            }
                            catch (Exception ex2)
                            {
                                // This catch block will handle any errors that may have occurred
                                // on the server that would cause the rollback to fail, such as
                                // a closed connection.
                                logger.LogDebug("Rollback Exception Type: {0}", ex2.GetType());
                                logger.LogDebug("  Message: {0} {1}", ex2.Message, ex2.ToString());
                                Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                                Console.WriteLine("  Message: {0} {1}", ex2.Message, ex2.ToString());
                            }
                        }

                        using (SqlConnection connection = new SqlConnection(connectionString_))
                        {
                            SqlCommand command = connection.CreateCommand();
                            // Create SQL
                            command.Parameters.Clear();
                            StringBuilder bulder = new StringBuilder();

                            bulder.AppendFormat(" UPDATE SYNC_DATA_LOG ");
                            bulder.AppendFormat(" SET [ERROR_DESC]=@ErrMsg, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                            bulder.AppendFormat(" WHERE [SYNC_ID]= @VAL_SYNC_DATA_LOG_SEQ ");
                            command.Parameters.Add("@ErrMsg", SqlDbType.NVarChar);
                            command.Parameters["@ErrMsg"].Value = (ex.Message + ":" + ex.ToString());
                            command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                            command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                            // Execute SQL
                            SqlTransaction transn = null;
                            try
                            {
                                connection.Open();
                                // Start a local transaction.
                                transn = connection.BeginTransaction("T4");

                                // Must assign both transaction object and connection
                                // to Command object for a pending local transaction
                                command.Connection = connection;
                                command.Transaction = transn;
                                Int32 rowsAffected = command.ExecuteNonQuery();
                                logger.LogDebug("RowsAffected: {0}", rowsAffected);
                                Console.WriteLine("RowsAffected: {0}", rowsAffected);
                                // Attempt to commit the transaction.
                                transn.Commit();
                                logger.LogDebug("Commit to database.");
                                Console.WriteLine("Commit to database.");
                            }
                            catch (Exception ex3)
                            {
                                logger.LogDebug(ex3.Message + ex3.ToString());
                                Console.WriteLine(ex3.Message + ex3.ToString());
                                if (transn != null)
                                {
                                    transn.Rollback();
                                }
                            }
                        }
                        // SQL Scope
                        throw;
                    }
                    // SQL Scope
                }

            }

            responses = req.CreateResponse(HttpStatusCode.OK);
            return responses;
        }


        public class ProspectAccount
        {
            public string prospAccId { get; set; }
            public string custCode { get; set; }

        }

    }
}
