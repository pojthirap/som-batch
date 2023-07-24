using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundBAInformationResponse
{
    public class OutboundBAInformationResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public string Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Business_Area { get; set; }
        public string Business_Area_Desc { get; set; }
    }
}
