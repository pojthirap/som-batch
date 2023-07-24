using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundCountryInformationResponse
{
    public class OutboundCountryInformationResponse : ResponseBase<Data>
    {
        public List<Data> Data { get; set; }
    }


    public class Data
    {
        public long Update_Date { get; set; }
        public string Update_Time { get; set; }
        public string Status_IND { get; set; }
        public string Country_Key { get; set; }
        public string Lang_Key { get; set; }
        public string Name { get; set; }
        public string Nationality { get; set; }
        public string Long_Name { get; set; }
        public string Nationality_Long { get; set; }
    }
}
