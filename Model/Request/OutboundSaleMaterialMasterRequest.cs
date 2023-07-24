using AzureFunctionApp.Model.Request.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Request.OutboundSaleMaterialMasterRequest
{
    public class OutboundSaleMaterialMasterRequest
    {
        public RequestInput Input { get; set; }
        public List<DivisionInput> Division { get; set; }
        public List<MaterialInput> Material { get; set; }

    }
    public class RequestInput : RequestBase
    {
        public string Start_Date { get; set; }
        public string Start_Time { get; set; }
        public string End_Date { get; set; }
        public string End_Time { get; set; }
        public string All_data { get; set; }
    }

    public class DivisionInput
    {
        public string Division { get; set; }
    }
    public class MaterialInput
    {
        public string Material_Code { get; set; }
    }
    
}
