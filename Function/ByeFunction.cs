using System.Net;
using System.Threading.Tasks;
using AzureFunctionApp.common;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace AzureFunctionApp
{
    public static class ByeFunction
    {
        [Function("ByeFunction")]
        public static async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(CommonConstant.GetLoggerString);
            logger.LogInformation("C# HTTP trigger function processed a request. ByeFunction");

            //var queryDictionary = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(req.Url.Query);
            //string name = queryDictionary.Count==0 ? "" : queryDictionary["name"];

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            //response.WriteString("Bye to Azure Functions! : " + name);
            response.WriteString("Bye to Azure Functions! Version : " + CommonConstant.VERSION);

            return response;
        }

    }
}
