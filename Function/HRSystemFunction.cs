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
using AzureFunctionApp.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzureFunctionApp
{
    public  class HRSystemFunction
    {
        private readonly IConfiguration _configuration;
        private string connectionString_ = null;
        private string interfaceSapPassword_ = null;
        private string interfaceSapReqKey_ = null;
        public HRSystemFunction(IConfiguration configuration)
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

        [Function("HRSystemFunction")]
        public  async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(CommonConstant.GetLoggerString);
            logger.LogInformation("C# HTTP trigger function processed a request. HRSystemFunction");

            var responses = req.CreateResponse(HttpStatusCode.OK);
            responses.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            responses.WriteString("connectionString : " + connectionString_);

            // SQL Scope HRMS_UPD

            using (SqlConnection connection = new SqlConnection(connectionString_))
            {
                string VAL_SYNC_DATA_LOG_SEQ = "";
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
                SqlTransaction transaction1;

                // Start a local transaction.
                transaction1 = connection.BeginTransaction("T1");

                // Must assign both transaction object and connection
                // to Command object for a pending local transaction
                command.Connection = connection;
                command.Transaction = transaction1;

                try
                {
                    // INSERT SYNC_DATA_LOG
                    // Create SQL
                    command.Parameters.Clear();
                    StringBuilder bulder = new StringBuilder();
                    bulder.AppendFormat(" INSERT INTO SYNC_DATA_LOG ([SYNC_ID], [INTERFACE_ID], [CREATE_USER], [CREATE_DTM]) ");
                    bulder.AppendFormat(" VALUES(@VAL_SYNC_DATA_LOG_SEQ, 'HRMS_UPD', 'SYSTEM', dbo.GET_SYSDATETIME()); ");
                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;

                    // Execute SQL
                    logger.LogDebug("Query:" + bulder.ToString());
                    Console.WriteLine("Query:" + bulder.ToString());
                    command.CommandText = bulder.ToString();
                    Int32 rowsAffected = command.ExecuteNonQuery();
                    logger.LogDebug("RowsAffected:" + rowsAffected);
                    Console.WriteLine("RowsAffected:" + rowsAffected);


                    command.Parameters.Clear();
                    bulder = new StringBuilder();
                    bulder.AppendFormat(" UPDATE E ");
                    bulder.AppendFormat(" SET  ");
                    bulder.AppendFormat(" E.SYNC_ID = @VAL_SYNC_DATA_LOG_SEQ  ");
                    bulder.AppendFormat(" ,E.COMPANY_CODE=EH.COMPANY_CODE ");
                    bulder.AppendFormat(" ,E.JOB_TITLE=EH.JOB_TITLE ");
                    bulder.AppendFormat(" ,E.TITLE_NAME=EH.TITLE_NAME ");
                    bulder.AppendFormat(" ,E.FIRST_NAME=EH.FIRST_NAME ");
                    bulder.AppendFormat(" ,E.LAST_NAME=EH.LAST_NAME ");
                    bulder.AppendFormat(" ,E.GENDER=EH.GENDER ");
                    bulder.AppendFormat(" ,E.STREET=EH.STREET ");
                    bulder.AppendFormat(" ,E.TELL_NO=EH.TELL_NO ");
                    bulder.AppendFormat(" ,E.COUNTRY_NAME=EH.COUNTRY_NAME ");
                    bulder.AppendFormat(" ,E.PROVINCE_NAME=EH.PROVINCE_NAME ");
                    bulder.AppendFormat(" ,E.DISTRICT_NAME=EH.DISTRICT_NAME ");
                    bulder.AppendFormat(" ,E.SUBDISTRICT_NAME=EH.SUBDISTRICT_NAME ");
                    bulder.AppendFormat(" ,E.POST_CODE=EH.POST_CODE ");
                    bulder.AppendFormat(" ,E.EMAIL=EH.EMAIL ");
                    bulder.AppendFormat(" ,E.STATUS=EH.STATUS ");
                    //--,E.ACTIVE_FLAG=EH.ACTIVE_FLAG
                    bulder.AppendFormat(" ,E.UPDATE_USER= 'HRMS' ");
                    bulder.AppendFormat(" ,E.UPDATE_DTM=EH.UPDATE_DTM ");
                    bulder.AppendFormat(" FROM ADM_EMPLOYEE E ");
                    bulder.AppendFormat(" INNER JOIN(select*, ROW_NUMBER() OVER(partition by emp_id ORDER BY update_dtm DESC) AS ROW_ID from ADM_EMPLOYEE_HRMS) EH on EH.EMP_ID = E.EMP_ID AND EH.ROW_ID = 1 ");
                    bulder.AppendFormat(" where cast(EH.UPDATE_DTM as date) = cast(dbo.GET_SYSDATETIME() as date) ");
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
                    transaction1.Commit();
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

                    bulder.AppendFormat(" UPDATE SYNC_DATA_LOG ");
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
                    int rowsAffected = command.ExecuteNonQuery();
                    logger.LogDebug("RowsAffected:" + rowsAffected);
                    Console.WriteLine("RowsAffected:" + rowsAffected);

                    // Attempt to commit the transaction.
                    transaction1.Commit();
                    logger.LogDebug("Commit to database.");
                    Console.WriteLine("Commit to database.");

                    // Attempt to roll back the transaction.
                    /*try
                    {
                        transaction1.Rollback();
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
                    }*/
                }
            }
            // SQL Scope






            // SQL Scope HRMS_UPD

            using (SqlConnection connection = new SqlConnection(connectionString_))
            {
                string VAL_SYNC_DATA_LOG_SEQ = "";
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
                SqlTransaction transaction1;

                // Start a local transaction.
                transaction1 = connection.BeginTransaction("T1");

                // Must assign both transaction object and connection
                // to Command object for a pending local transaction
                command.Connection = connection;
                command.Transaction = transaction1;

                try
                {
                    // INSERT SYNC_DATA_LOG
                    // Create SQL
                    command.Parameters.Clear();
                    StringBuilder bulder = new StringBuilder();
                    bulder.AppendFormat(" INSERT INTO SYNC_DATA_LOG ([SYNC_ID], [INTERFACE_ID], [CREATE_USER], [CREATE_DTM]) ");
                    bulder.AppendFormat(" VALUES(@VAL_SYNC_DATA_LOG_SEQ, 'HRMS_NEW', 'SYSTEM', dbo.GET_SYSDATETIME()); ");
                    command.Parameters.Add("@VAL_SYNC_DATA_LOG_SEQ", SqlDbType.Decimal);
                    command.Parameters["@VAL_SYNC_DATA_LOG_SEQ"].Value = VAL_SYNC_DATA_LOG_SEQ;

                    // Execute SQL
                    logger.LogDebug("Query:" + bulder.ToString());
                    Console.WriteLine("Query:" + bulder.ToString());
                    command.CommandText = bulder.ToString();
                    Int32 rowsAffected = command.ExecuteNonQuery();
                    logger.LogDebug("RowsAffected:" + rowsAffected);
                    Console.WriteLine("RowsAffected:" + rowsAffected);


                    command.Parameters.Clear();
                    bulder = new StringBuilder();
                    bulder.AppendFormat(" INSERT INTO ADM_EMPLOYEE ([EMP_ID], [COMPANY_CODE], [JOB_TITLE], [TITLE_NAME], [FIRST_NAME], [LAST_NAME], [GENDER], [STREET], [TELL_NO], [COUNTRY_NAME], [PROVINCE_CODE], [PROVINCE_NAME], [GROUP_CODE], [DISTRICT_NAME], [SUBDISTRICT_NAME], [POST_CODE], [EMAIL], [STATUS], [APPROVE_EMP_ID], [ACTIVE_FLAG], [SYNC_ID], [CREATE_USER], [CREATE_DTM], [UPDATE_USER], [UPDATE_DTM])  ");
                    bulder.AppendFormat(" SELECT EH.EMP_ID, EH.COMPANY_CODE, EH.JOB_TITLE, EH.TITLE_NAME, EH.FIRST_NAME, EH.LAST_NAME, EH.GENDER, EH.STREET, EH.TELL_NO, EH.COUNTRY_NAME, EH.PROVINCE_CODE, EH.PROVINCE_NAME, EH.GROUP_CODE, EH.DISTRICT_NAME, EH.SUBDISTRICT_NAME, EH.POST_CODE, EH.EMAIL, EH.STATUS, EH.APPROVE_EMP_ID, EH.ACTIVE_FLAG, @VAL_SYNC_DATA_LOG_SEQ, 'HRMS', EH.UPDATE_DTM, 'HRMS', EH.UPDATE_DTM ");
                    bulder.AppendFormat(" from (select *,ROW_NUMBER() OVER(partition by emp_id ORDER BY update_dtm DESC) AS ROW_ID from ADM_EMPLOYEE_HRMS) EH ");
                    bulder.AppendFormat(" left join ADM_EMPLOYEE E on E.EMP_ID = EH.EMP_ID ");
                    bulder.AppendFormat(" where E.EMP_ID IS NULL and EH.ROW_ID = 1 ");
                    bulder.AppendFormat(" and cast(EH.UPDATE_DTM as date) = cast(dbo.GET_SYSDATETIME() as date) ");
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
                    transaction1.Commit();
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

                    bulder.AppendFormat(" UPDATE SYNC_DATA_LOG ");
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
                    int rowsAffected = command.ExecuteNonQuery();
                    logger.LogDebug("RowsAffected:" + rowsAffected);
                    Console.WriteLine("RowsAffected:" + rowsAffected);

                    // Attempt to commit the transaction.
                    transaction1.Commit();
                    logger.LogDebug("Commit to database.");
                    Console.WriteLine("Commit to database.");

                    // Attempt to roll back the transaction.
                    /*try
                    {
                        transaction1.Rollback();
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
                    }*/
                }
            }
            // SQL Scope



            responses = req.CreateResponse(HttpStatusCode.OK);
            return responses;
        }



    }
}
