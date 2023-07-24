using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundSaleOrderDocumentTypesToSaleAreaResponse
{
    public class OutboundSaleOrderDocumentTypesToSaleAreaResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public long Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Sale_Doc_Type { get; set; }
        public long Sale_Org { get; set; }
        public long Distribution_Channel { get; set; }
        public long Division { get; set; }
        public string Sales_Org { get; set; }
        public string Sales_Office { get; set; }
    }
}
