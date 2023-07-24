using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundSaleOrgInformationResponse
{
    public class OutboundSaleOrgInformationResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public long Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Sales_Org { get; set; }
        public string Sales_Org_Name { get; set; }
        public string Com_Code { get; set; }
        public string Stat_Currency { get; set; }
    }
}
