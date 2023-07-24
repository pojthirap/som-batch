using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.Base
{
    public class ResponseBase<T>
    {
        public HeaderResponseBase Header { get; set; }
        public List<T> Data { get; set; }
    }

    public class HeaderResponseBase
    {
        public string Status { get; set; }
        public string Message { get; set; }
        public string Interface_ID { get; set; }
        public string Start_Date { get; set; }
        public string Start_Time { get; set; }
        public string End_Date { get; set; }
        public string End_Time { get; set; }
    }
}
