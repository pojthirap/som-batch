using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundCompanyInformationResponse
{
    public class OutboundCompanyInformationResponse: ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public string Status_IND { get; set; }
        public long Company_Code { get; set; }
        public string Company_Name_EN { get; set; }
        public string Company_Name_TH { get; set; }
        public string VAT_Reg_No { get; set; }
    }
}
