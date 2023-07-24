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
using AzureFunctionApp.EnumVal;
using AzureFunctionApp.Model.Request.OutboundSaleMaterialMasterRequest;
using AzureFunctionApp.Model.Response.OutboundSaleMaterialMasterResponse;
using AzureFunctionApp.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzureFunctionApp
{
    public  class SaleMaterialMasterFunction
    {
        private readonly IConfiguration _configuration;
        private string connectionString_ = null;
        private string interfaceSapPassword_ = null;
        private string interfaceSapReqKey_ = null;
        public SaleMaterialMasterFunction(IConfiguration configuration)
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

        [Function("SaleMaterialMasterFunction")]
        public  async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(CommonConstant.GetLoggerString);
            logger.LogInformation("C# HTTP trigger function processed a request. SaleMaterialMasterFunction");

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
                    bulder.AppendFormat(" VALUES(@VAL_SYNC_DATA_LOG_SEQ, 'ZSOMI012', 'SYSTEM', dbo.GET_SYSDATETIME()); ");
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
                OutboundSaleMaterialMasterRequest reqData = new OutboundSaleMaterialMasterRequest();
                RequestInput input = new RequestInput();
                input.Interface_ID = "ZSOMI012";
                input.Table_Object = "MARA";
                //input.All_data = "X";
                input.All_data = "";
                input.Start_Date =  Start_Date;// 20180124;//--PARAM FORMAT EX 20170124 (YYYYMMDD)
                input.Start_Time =  Start_Time;// 000001;//--PARAM FORMAT EX 000001 (HHMISS)
                input.End_Date =  End_Date;// 20180424;//--PARAM FORMAT EX 20170124 (YYYYMMDD)
                input.End_Time =  End_Time;// 235959;//--PARAM FORMAT EX 235959 (HHMISS)
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
                List<MaterialInput> materialInput = new List<MaterialInput>();
                MaterialInput m = new MaterialInput();
                m.Material_Code = "";//--Fix
                materialInput.Add(m);
                reqData.Division = divisionInput;
                reqData.Material = materialInput;
                reqData.Input = input;

                client.BaseAddress = new Uri(ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleMaterialMaster);
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
                string fullPath = ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleMaterialMaster;
                var jsonVal = JsonConvert.SerializeObject(reqData);
                var content = new StringContent(jsonVal, Encoding.UTF8, "application/json");
                logger.LogDebug("Call Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleMaterialMaster);
                logger.LogDebug("=========== jsonVal ================");
                logger.LogDebug("REQUEST:" + jsonVal);
                logger.LogDebug("REQUEST:" + JObject.Parse(jsonVal));
                Console.WriteLine("Call Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleMaterialMaster);
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

                OutboundSaleMaterialMasterResponse resultOutbound = null;
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
                        resultOutbound = new OutboundSaleMaterialMasterResponse();
                        List<Data> lst = new List<Data>();

                        if (response.IsSuccessStatusCode) // Call Success
                        {
                            resultOutbound = await response.Content.ReadAsAsync<OutboundSaleMaterialMasterResponse>();
                            lst = resultOutbound.Basic_Data;
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

                        logger.LogDebug("Response Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleMaterialMaster);
                        logger.LogDebug("Response:" + JsonConvert.SerializeObject(resultOutbound));
                        Console.WriteLine("Response Outbound Service:" + ic.InterfaceSapUrl + CommonConstant.API_OutboundSaleMaterialMaster);
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
                            if (resultOutbound.Basic_Data != null && resultOutbound.Basic_Data.Count != 0)
                            {
                                StringBuilder bulder = null;
                                Int32 rowsAffected = 0;
                                foreach (Data data in resultOutbound.Basic_Data)
                                {

                                    if ("D".Equals(data.Status_IND))
                                    {
                                        // UPDATE MS_PRODUCT
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE MS_PRODUCT  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME()  ");
                                        bulder.AppendFormat(" WHERE [PROD_CODE]=@Material  ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                        command.Parameters["@Material"].Value = data.Material;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);


                                        // UPDATE MS_PRODUCT_SALE
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE MS_PRODUCT_SALE  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [PROD_CODE]=@Material ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                        command.Parameters["@Material"].Value = data.Material;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);

                                        // UPDATE MS_PRODUCT_CONVERSION
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE MS_PRODUCT_CONVERSION  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [PROD_CODE]=@Material ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                        command.Parameters["@Material"].Value = data.Material;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);

                                        // UPDATE MS_PRODUCT_PLANT
                                        // Create SQL
                                        /*command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE MS_PRODUCT_PLANT  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [PROD_CODE]=@Material ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                        command.Parameters["@Material"].Value = data.Material;
                                        QueryUtils.configParameter(command);

                                        // Execute SQL
                                        logger.LogDebug("Query:" + bulder.ToString());
                                        Console.WriteLine("Query:" + bulder.ToString());
                                        command.CommandText = bulder.ToString();
                                        rowsAffected = command.ExecuteNonQuery();
                                        logger.LogDebug("RowsAffected:" + rowsAffected);
                                        Console.WriteLine("RowsAffected:" + rowsAffected);
                                        */

                                    }
                                    else
                                    {
                                        // MS_PRODUCT
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE MS_PRODUCT  ");
                                        bulder.AppendFormat(" SET [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [DIVISION_CODE]=@Division, [PROD_NAME_TH]=@Material_Desc_TH, [PROD_NAME_EN]=@Material_Desc_TH, [PROD_TYPE]=@Material_Type, [PROD_GROUP]=@Material_Group, [INDUSTRY_SECTOR]=@Industry_Sector, [OLD_PROD_NO]=@Old_Material_Number, [BASE_UNIT]=@Base_Unit_Of_Measure, [ACTIVE_FLAG]='Y', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [PROD_CODE]=@Material ");
                                        command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                                        command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;
                                        command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                        command.Parameters["@Division"].Value = data.Division;
                                        command.Parameters.Add("@Material_Desc_TH", SqlDbType.NVarChar);
                                        command.Parameters["@Material_Desc_TH"].Value = data.Material_Desc_TH;
                                        command.Parameters.Add("@Material_Type", SqlDbType.NVarChar);
                                        command.Parameters["@Material_Type"].Value = data.Material_Type;
                                        command.Parameters.Add("@Material_Group", SqlDbType.NVarChar);
                                        command.Parameters["@Material_Group"].Value = data.Material_Group;
                                        command.Parameters.Add("@Industry_Sector", SqlDbType.NVarChar);
                                        command.Parameters["@Industry_Sector"].Value = data.Industry_Sector;
                                        command.Parameters.Add("@Old_Material_Number", SqlDbType.NVarChar);
                                        command.Parameters["@Old_Material_Number"].Value = data.Old_Material_Number;
                                        command.Parameters.Add("@Base_Unit_Of_Measure", SqlDbType.NVarChar);
                                        command.Parameters["@Base_Unit_Of_Measure"].Value = data.Base_Unit_Of_Measure;
                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                        command.Parameters["@Material"].Value = data.Material;
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
                                            bulder.AppendFormat(" INSERT INTO MS_PRODUCT ([PROD_CODE], [DIVISION_CODE], [PROD_NAME_TH], [PROD_NAME_EN], [PROD_TYPE], [PROD_GROUP], [INDUSTRY_SECTOR], [OLD_PROD_NO], [BASE_UNIT], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                            bulder.AppendFormat(" VALUES(@Material, @Division, @Material_Desc_TH, @Material_Desc_TH, @Material_Type, @Material_Group, @Industry_Sector, @Old_Material_Number, @Base_Unit_Of_Measure, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME())  ");
                                            command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                            command.Parameters["@Material"].Value = data.Material;
                                            command.Parameters.Add("@Division", SqlDbType.NVarChar);
                                            command.Parameters["@Division"].Value = data.Division;
                                            command.Parameters.Add("@Material_Desc_TH", SqlDbType.NVarChar);
                                            command.Parameters["@Material_Desc_TH"].Value = data.Material_Desc_TH;
                                            command.Parameters.Add("@Material_Type", SqlDbType.NVarChar);
                                            command.Parameters["@Material_Type"].Value = data.Material_Type;
                                            command.Parameters.Add("@Material_Group", SqlDbType.NVarChar);
                                            command.Parameters["@Material_Group"].Value = data.Material_Group;
                                            command.Parameters.Add("@Industry_Sector", SqlDbType.NVarChar);
                                            command.Parameters["@Industry_Sector"].Value = data.Industry_Sector;
                                            command.Parameters.Add("@Old_Material_Number", SqlDbType.NVarChar);
                                            command.Parameters["@Old_Material_Number"].Value = data.Old_Material_Number;
                                            command.Parameters.Add("@Base_Unit_Of_Measure", SqlDbType.NVarChar);
                                            command.Parameters["@Base_Unit_Of_Measure"].Value = data.Base_Unit_Of_Measure;
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


                                            if (data.Sale_Data != null && data.Sale_Data.Count != 0)
                                            {
                                                foreach(Sale_Data item in data.Sale_Data)
                                                {
                                                    // Table MS_PRODUCT_SALE
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO MS_PRODUCT_SALE ([PROD_SELL_ID], [PROD_CODE], [ORG_CODE], [CHANNEL_CODE], [PROD_CATE_CODE], [PROD_CATE_DESC], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR MS_PRODUCT_SALE_SEQ, @Material, @Sales_Org, @Distribution_Channel, @Product_Hirerachy, @Product_Hirerachy_Desc, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                    command.Parameters["@Material"].Value = item.Material;
                                                    command.Parameters.Add("@Sales_Org", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Org"].Value = item.Sales_Org;
                                                    command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                    command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                    command.Parameters.Add("@Product_Hirerachy", SqlDbType.NVarChar);
                                                    command.Parameters["@Product_Hirerachy"].Value = item.Product_Hirerachy;
                                                    command.Parameters.Add("@Product_Hirerachy_Desc", SqlDbType.NVarChar);
                                                    command.Parameters["@Product_Hirerachy_Desc"].Value = item.Product_Hirerachy_Desc;
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


                                            if (data.Mat_Conversion_Data != null && data.Mat_Conversion_Data.Count != 0)
                                            {
                                                foreach (Mat_Conversion_Data item in data.Mat_Conversion_Data)
                                                {
                                                    // Table MS_PRODUCT_CONVERSION
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO MS_PRODUCT_CONVERSION ([PROD_CONV_ID], [PROD_CODE], [ALT_UNIT], [DENOMINATOR], [COUNTER], [GROSS_WEIGHT], [WEIGHT_UNIT], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR MS_PRODUCT_CONVERSION_SEQ, @Material, @Alt_Unit_Of_Measure, @Denominator, @Counter, @Gross_Weight, @Weight_Unit, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                    command.Parameters["@Material"].Value = item.Material;
                                                    command.Parameters.Add("@Alt_Unit_Of_Measure", SqlDbType.NVarChar);
                                                    command.Parameters["@Alt_Unit_Of_Measure"].Value = item.Alt_Unit_Of_Measure;
                                                    command.Parameters.Add("@Denominator", SqlDbType.Decimal);
                                                    command.Parameters["@Denominator"].Value = item.Denominator;
                                                    command.Parameters.Add("@Counter", SqlDbType.Decimal);
                                                    command.Parameters["@Counter"].Value = item.Counter;
                                                    command.Parameters.Add("@Gross_Weight", SqlDbType.Decimal);
                                                    command.Parameters["@Gross_Weight"].Value = item.Gross_Weight;
                                                    command.Parameters.Add("@Weight_Unit", SqlDbType.NVarChar);
                                                    command.Parameters["@Weight_Unit"].Value = item.Weight_Unit;
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

                                            /*if (data.Plant_Data != null && data.Plant_Data.Count != 0)
                                            {
                                                foreach (Plant_Data item in data.Plant_Data)
                                                {
                                                    // Table MS_PRODUCT_PLANT
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO MS_PRODUCT_PLANT ([PROD_PLANT_ID], [PROD_CODE], [PLANT_CODE], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR MS_PRODUCT_PLANT_SEQ, @Material, @Plant, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                    command.Parameters["@Material"].Value = item.Material;
                                                    command.Parameters.Add("@Plant", SqlDbType.NVarChar);
                                                    command.Parameters["@Plant"].Value = item.Plant;
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
                                            }*/

                                        }
                                        else
                                        {

                                            if (data.Sale_Data != null && data.Sale_Data.Count != 0)
                                            {
                                                foreach (Sale_Data item in data.Sale_Data)
                                                {
                                                    // Table MS_PRODUCT_SALE
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" UPDATE MS_PRODUCT_SALE  ");
                                                    bulder.AppendFormat(" SET [ACTIVE_FLAG]='Y', [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                    bulder.AppendFormat(" WHERE [PROD_CODE]=@Material and [ORG_CODE]=@Sales_Org and [CHANNEL_CODE]=@Distribution_Channel ");
                                                    command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                    command.Parameters["@Material"].Value = item.Material;
                                                    command.Parameters.Add("@Sales_Org", SqlDbType.NVarChar);
                                                    command.Parameters["@Sales_Org"].Value = item.Sales_Org;
                                                    command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                    command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
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
                                                        // Table MS_PRODUCT_SALE
                                                        // Create SQL
                                                        command.Parameters.Clear();
                                                        bulder = new StringBuilder();
                                                        bulder.AppendFormat(" INSERT INTO MS_PRODUCT_SALE ([PROD_SELL_ID], [PROD_CODE], [ORG_CODE], [CHANNEL_CODE], [PROD_CATE_CODE], [PROD_CATE_DESC], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                        bulder.AppendFormat(" VALUES(NEXT VALUE FOR MS_PRODUCT_SALE_SEQ, @Material, @Sales_Org, @Distribution_Channel, @Product_Hirerachy, @Product_Hirerachy_Desc, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                        command.Parameters["@Material"].Value = item.Material;
                                                        command.Parameters.Add("@Sales_Org", SqlDbType.NVarChar);
                                                        command.Parameters["@Sales_Org"].Value = item.Sales_Org;
                                                        command.Parameters.Add("@Distribution_Channel", SqlDbType.NVarChar);
                                                        command.Parameters["@Distribution_Channel"].Value = item.Distribution_Channel;
                                                        command.Parameters.Add("@Product_Hirerachy", SqlDbType.NVarChar);
                                                        command.Parameters["@Product_Hirerachy"].Value = item.Product_Hirerachy;
                                                        command.Parameters.Add("@Product_Hirerachy_Desc", SqlDbType.NVarChar);
                                                        command.Parameters["@Product_Hirerachy_Desc"].Value = item.Product_Hirerachy_Desc;
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

                                            // Table MS_PRODUCT_SALE
                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" UPDATE MS_PRODUCT_SALE  ");
                                            bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                            bulder.AppendFormat(" WHERE [PROD_CODE]=@Material and ISNULL(SYNC_ID,-1)  != @VAL_SYNC_DATA_LOG_SEQ ");
                                            command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                            command.Parameters["@Material"].Value = data.Material;
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




                                            if (data.Mat_Conversion_Data != null && data.Mat_Conversion_Data.Count != 0)
                                            {
                                                foreach (Mat_Conversion_Data item in data.Mat_Conversion_Data)
                                                {
                                                    // Table MS_PRODUCT_CONVERSION
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" UPDATE MS_PRODUCT_CONVERSION  ");
                                                    bulder.AppendFormat(" SET [DENOMINATOR]=@Denominator, [COUNTER]=@Counter, [GROSS_WEIGHT]=@Gross_Weight, [WEIGHT_UNIT]=@Weight_Unit, [ACTIVE_FLAG]='Y', [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                    bulder.AppendFormat(" WHERE [PROD_CODE]=@Material and [ALT_UNIT]=@Alt_Unit_Of_Measure ");
                                                    command.Parameters.Add("@Denominator", SqlDbType.Decimal);
                                                    command.Parameters["@Denominator"].Value = item.Denominator;
                                                    command.Parameters.Add("@Counter", SqlDbType.Decimal);
                                                    command.Parameters["@Counter"].Value = item.Counter;
                                                    command.Parameters.Add("@Gross_Weight", SqlDbType.Decimal);
                                                    command.Parameters["@Gross_Weight"].Value = item.Gross_Weight;
                                                    command.Parameters.Add("@Weight_Unit", SqlDbType.NVarChar);
                                                    command.Parameters["@Weight_Unit"].Value = item.Weight_Unit;
                                                    command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                    command.Parameters["@Material"].Value = item.Material;
                                                    command.Parameters.Add("@Alt_Unit_Of_Measure", SqlDbType.NVarChar);
                                                    command.Parameters["@Alt_Unit_Of_Measure"].Value = item.Alt_Unit_Of_Measure;
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
                                                        // Table MS_PRODUCT_CONVERSION
                                                        // Create SQL
                                                        command.Parameters.Clear();
                                                        bulder = new StringBuilder();
                                                        bulder.AppendFormat(" INSERT INTO MS_PRODUCT_CONVERSION ([PROD_CONV_ID], [PROD_CODE], [ALT_UNIT], [DENOMINATOR], [COUNTER], [GROSS_WEIGHT], [WEIGHT_UNIT], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                        bulder.AppendFormat(" VALUES(NEXT VALUE FOR MS_PRODUCT_CONVERSION_SEQ, @Material, @Alt_Unit_Of_Measure, @Denominator, @Counter, @Gross_Weight, @Weight_Unit, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                        command.Parameters["@Material"].Value = item.Material;
                                                        command.Parameters.Add("@Alt_Unit_Of_Measure", SqlDbType.NVarChar);
                                                        command.Parameters["@Alt_Unit_Of_Measure"].Value = item.Alt_Unit_Of_Measure;
                                                        command.Parameters.Add("@Denominator", SqlDbType.Decimal);
                                                        command.Parameters["@Denominator"].Value = item.Denominator;
                                                        command.Parameters.Add("@Counter", SqlDbType.Decimal);
                                                        command.Parameters["@Counter"].Value = item.Counter;
                                                        command.Parameters.Add("@Gross_Weight", SqlDbType.Decimal);
                                                        command.Parameters["@Gross_Weight"].Value = item.Gross_Weight;
                                                        command.Parameters.Add("@Weight_Unit", SqlDbType.NVarChar);
                                                        command.Parameters["@Weight_Unit"].Value = item.Weight_Unit;
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

                                            // Table MS_PRODUCT_CONVERSION
                                            // Create SQL
                                            command.Parameters.Clear();
                                            bulder = new StringBuilder();
                                            bulder.AppendFormat(" UPDATE MS_PRODUCT_CONVERSION  ");
                                            bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                            bulder.AppendFormat(" WHERE [PROD_CODE]=@Material and ISNULL(SYNC_ID,-1)  != @VAL_SYNC_DATA_LOG_SEQ ");
                                            command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                            command.Parameters["@Material"].Value = data.Material;
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

                                        /*if (data.Plant_Data != null && data.Plant_Data.Count != 0)
                                        {
                                            foreach (Plant_Data item in data.Plant_Data)
                                            {
                                                // Table MS_PRODUCT_PLANT
                                                // Create SQL
                                                command.Parameters.Clear();
                                                bulder = new StringBuilder();
                                                bulder.AppendFormat(" UPDATE MS_PRODUCT_PLANT  ");
                                                bulder.AppendFormat(" SET [ACTIVE_FLAG]='Y', [SYNC_ID]=@VAL_SYNC_DATA_LOG_SEQ, [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                                bulder.AppendFormat(" WHERE [PROD_CODE]=@Material and [PLANT_CODE]=@Plant ");
                                                command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                command.Parameters["@Material"].Value = item.Material;
                                                command.Parameters.Add("@Plant", SqlDbType.NVarChar);
                                                command.Parameters["@Plant"].Value = item.Plant;
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
                                                    // Table MS_PRODUCT_PLANT
                                                    // Create SQL
                                                    command.Parameters.Clear();
                                                    bulder = new StringBuilder();
                                                    bulder.AppendFormat(" INSERT INTO MS_PRODUCT_PLANT ([PROD_PLANT_ID], [PROD_CODE], [PLANT_CODE], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                                                    bulder.AppendFormat(" VALUES(NEXT VALUE FOR MS_PRODUCT_PLANT_SEQ, @Material, @Plant, 'Y', @VAL_SYNC_DATA_LOG_SEQ, 'SYSTEM', dbo.GET_SYSDATETIME(), 'SYSTEM', dbo.GET_SYSDATETIME()) ");
                                                    command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                                    command.Parameters["@Material"].Value = item.Material;
                                                    command.Parameters.Add("@Plant", SqlDbType.NVarChar);
                                                    command.Parameters["@Plant"].Value = item.Plant;
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

                                        // Table MS_PRODUCT_PLANT
                                        // Create SQL
                                        command.Parameters.Clear();
                                        bulder = new StringBuilder();
                                        bulder.AppendFormat(" UPDATE MS_PRODUCT_PLANT ");
                                        bulder.AppendFormat(" SET [ACTIVE_FLAG]='N', [UPDATE_USER]='SYSTEM', [UPDATE_DTM]=dbo.GET_SYSDATETIME() ");
                                        bulder.AppendFormat(" WHERE [PROD_CODE]=@Material and ISNULL(SYNC_ID,-1) != @VAL_SYNC_DATA_LOG_SEQ ");
                                        command.Parameters.Add("@Material", SqlDbType.NVarChar);
                                        command.Parameters["@Material"].Value = data.Material;
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

                                        */


                                    }




                                }// end for


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
