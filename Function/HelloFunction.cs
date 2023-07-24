using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration; // used in Configuration
using AzureFunctionApp.common;
using System;

namespace AzureFunctionApp
{
    public class HelloFunction // make normal class
    {
        private readonly IConfiguration _configuration;
        private string connectionString_ = null;
        private string interfaceSapPassword_ = null;
        private string interfaceSapReqKey_ = null;
        public HelloFunction(IConfiguration configuration)
        {
            _configuration = configuration;
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

        [Function("HelloFunction")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(CommonConstant.GetLoggerString);
            logger.LogInformation("C# HTTP trigger function processed a request. HelloFunction");

            var queryDictionary = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(req.Url.Query);
            string name = queryDictionary.Count == 0 ? "" : queryDictionary["name"];

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            response.WriteString("Welcome to Azure Functions! : " + name);
            logger.LogInformation("name="+ name);

            string keyValue = _configuration["dev-uat-database-saleonmob"];
            response.WriteString("Configuration keyName => dev-uat-database-saleonmob : " + keyValue);

            string connectionString = _configuration["connectionString"];
            response.WriteString("Configuration keyName => connectionString : " + connectionString);

            string keyValueInterfaceSapPassword = _configuration["uat-saleonmobile-sappwd"];
            response.WriteString("Configuration keyName => uat-saleonmobile-sappwd : " + keyValueInterfaceSapPassword);

            string sapInterfacePassword = _configuration["SapInterfacePassword"];
            response.WriteString("Configuration keyName => sapInterfacePassword : " + sapInterfacePassword);

            string keyValueInterfaceSapReqKey = _configuration["dev-uat-saleonmobile-sap-reqkey"];
            response.WriteString("Configuration keyName => dev-uat-saleonmobile-sap-reqkey : " + keyValueInterfaceSapReqKey);

            string sapInterfaceReqKey = _configuration["SapInterfacePassReqKey"];
            response.WriteString("Configuration keyName => sapInterfaceReqKey : " + sapInterfaceReqKey);

            return response;
        }
    }
}
