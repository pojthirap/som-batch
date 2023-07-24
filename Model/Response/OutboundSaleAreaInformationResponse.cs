using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundSaleAreaInformationResponse
{
    public class OutboundSaleAreaInformationResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public long Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Sale_Org { get; set; }
        public string Sale_Org_Name { get; set; }
        public string Distribution_Channel { get; set; }
        public string Distribution_Channel_Name { get; set; }
        public string Division { get; set; }
        public string Division_name { get; set; }
        public string Business_Area { get; set; }
        public string Business_Area_Name { get; set; }
    }
}
