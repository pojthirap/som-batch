using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundSaleOfficeToSaleResponse
{
    public class OutboundSaleOfficeToSaleResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public long Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Sales_Office { get; set; }
        public string Sales_Office_Name { get; set; }
        public string Sales_Org { get; set; }
        public string Sales_Org_Name { get; set; }
        public string Distribution_Channel { get; set; }
        public string Distribution_Channel_Name { get; set; }
        public string Division { get; set; }
        public string Division_Name { get; set; }
    }
}
