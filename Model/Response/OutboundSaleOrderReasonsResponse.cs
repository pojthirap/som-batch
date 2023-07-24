using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundSaleOrderReasonsResponse
{
    public class OutboundSaleOrderReasonsResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public long Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Order_Reason { get; set; }
        public string Order_R_Decription { get; set; }
    }
}
