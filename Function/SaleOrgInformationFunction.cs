using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using AzureFunctionApp.common;
using AzureFunctionApp.Common;
using AzureFunctionApp.Model.Request.OutboundSaleOrgInformationRequest;
using AzureFunctionApp.Model.Response.OutboundSaleOrgInformationResponse;
using AzureFunctionApp.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzureFunctionApp
{
    public class SaleOrgInformationFunction
    {
        private readonly IConfiguration _configuration;
        private string connectionString_ = null;
        private string interfaceSapPassword_ = null;
        private string interfaceSapReqKey_ = null;
        public SaleOrgInformationFunction(IConfiguration configuration)
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

        [Function("SaleOrgInformationFunction")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(CommonConstant.GetLoggerString);
            logger.LogInformation("C# HTTP trigger function processed a request. SaleOrgInformationFunction");

            var responses = req.CreateResponse(HttpStatusCode.OK);
            responses.Headers.Add("Content-Type", "text/plain; charset=utf-8");

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
                    bulder.AppendFormat(" VALUES(@VAL_SYNC_DATA_LOG_SEQ, 'ZSOMI002', 'SYSTEM', dbo.GET_SYSDATETIME()); ");
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
                OutboundSaleOrgInformationRequest reqData = new OutboundSaleOrgInformationRequest();
                RequestInput input = new RequestInput();
                input.Interface_ID = "ZSOMI002";
                input.Table_Object = "V_TVKO_ASSIGN";
                input.All_data = "X";
                reqData.Input = input;

                client.BaseAddress = new Uri(ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleOrgInformation);
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
                string fullPath = ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleOrgInformation;
                var jsonVal = JsonConvert.SerializeObject(reqData);
                var content = new StringContent(jsonVal, Encoding.UTF8, "application/json");
                logger.LogDebug("Call Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleOrgInformation);
                logger.LogDebug("=========== jsonVal ================");
                logger.LogDebug("REQUEST:" + jsonVal);
                logger.LogDebug("REQUEST:" + JObject.Parse(jsonVal));
                Console.WriteLine("Call Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleOrgInformation);
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

                OutboundSaleOrgInformationResponse resultOutbound = null;
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
                        resultOutbound = new OutboundSaleOrgInformationResponse();
                        List<Data> lst = new List<Data>();

                        if (response.IsSuccessStatusCode) // Call Success
                        {
                            resultOutbound = await response.Content.ReadAsAsync<OutboundSaleOrgInformationResponse>();
                            lst = resultOutbound.Data;
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
                            logger.LogDebug("  Call Service Exception: {0} ", ErrMsg);
                            Console.WriteLine("  Call Service Exception: {0} ", ErrMsg);

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

                        logger.LogDebug("Response Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleOrgInformation);
                        logger.LogDebug("Response:" + JsonConvert.SerializeObject(resultOutbound));
                        Console.WriteLine("Response Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleOrgInformation);
                        Console.WriteLine("Response:" + JsonConvert.SerializeObject(resultOutbound));



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
                            if (resultOutbound.Data != null && resultOutbound.Data.Count != 0)
                            {
                                StringBuilder bulder = null;
                                Int32 rowsAffected = 0;
                                foreach (Data data in resultOutbound.Data)
                                {
                                    // INSERT SYNC_DATA_LOG
                                    // Create SQL
                                    command.Parameters.Clear();
                                    bulder = new StringBuilder();

                                    bulder.AppendFormat(" UPDATE ORG_SALE_ORGANIZATION  ");
                                    bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [COMPANY_CODE]=@Com_Code, [ORG_NAME_TH]=@Sales_Org_Name, [ORG_NAME_EN]=@Sales_Org_Name, [CURRENCY]=@Stat_Currency, [ACTIVE_FLAG]='Y', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                    bulder.AppendFormat(" WHERE [ORG_CODE]=@Sales_Org ");
                                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                    command.Parameters.Add("@Com_Code", SqlDbType.NVarChar);
                                    command.Parameters["@Com_Code"].Value = data.Com_Code;
                                    command.Parameters.Add("@Sales_Org_Name", SqlDbType.NVarChar);
                                    command.Parameters["@Sales_Org_Name"].Value = data.Sales_Org_Name;
                                    command.Parameters.Add("@Stat_Currency", SqlDbType.NVarChar);
                                    command.Parameters["@Stat_Currency"].Value = data.Stat_Currency;
                                    command.Parameters.Add("@Sales_Org", SqlDbType.NVarChar);
                                    command.Parameters["@Sales_Org"].Value = data.Sales_Org;
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

                                        bulder.AppendFormat(" INSERT INTO ORG_SALE_ORGANIZATION ([ORG_CODE], [COMPANY_CODE], [ORG_NAME_TH], [ORG_NAME_EN], [CURRENCY], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                        bulder.AppendFormat(" VALUES(@Sales_Org, @Com_Code, @Sales_Org_Name, @Sales_Org_Name, @Stat_Currency, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                        command.Parameters.Add("@Sales_Org", SqlDbType.NVarChar);
                                        command.Parameters["@Sales_Org"].Value = data.Sales_Org;
                                        command.Parameters.Add("@Com_Code", SqlDbType.NVarChar);
                                        command.Parameters["@Com_Code"].Value = data.Com_Code;
                                        command.Parameters.Add("@Sales_Org_Name", SqlDbType.NVarChar);
                                        command.Parameters["@Sales_Org_Name"].Value = data.Sales_Org_Name;
                                        command.Parameters.Add("@Stat_Currency", SqlDbType.NVarChar);
                                        command.Parameters["@Stat_Currency"].Value = data.Stat_Currency;
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
                                bulder.AppendFormat(" UPDATE ORG_SALE_ORGANIZATION   ");
                                bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                bulder.AppendFormat(" WHERE ISNULL(SYNC_ID,-1) != @VAL_SYNC_DATA_LOG_SEQ ");
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
                                logger.LogDebug(ex3.Message+ex3.ToString());
                                Console.WriteLine(ex3.Message+ex3.ToString());
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



    }
}
