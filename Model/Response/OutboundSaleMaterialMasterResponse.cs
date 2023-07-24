using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundSaleMaterialMasterResponse
{
    public class OutboundSaleMaterialMasterResponse : ResponseBase<Data>
    {
        public List<Data> Basic_Data { get; set; }
    }


    public class Data
    {
        
       public long Material { get; set; }
        public string Material_Type { get; set; }
        public string Industry_Sector { get; set; }
        public long Material_Group { get; set; }
        public string Old_Material_Number { get; set; }
        public string Base_Unit_Of_Measure { get; set; }
        public string Gross_Weight { get; set; }
        public string Net_Weight { get; set; }
        public string Weight_Unit { get; set; }
        public string Volume { get; set; }
        public long Division { get; set; }
        public long EAN_UPC { get; set; }
        public string Status_IND { get; set; }
        public string Material_Desc_TH { get; set; }
        public List<Sale_Data> Sale_Data { get; set; }
        public List<Mat_Conversion_Data> Mat_Conversion_Data { get; set; }
        public List<Plant_Data> Plant_Data { get; set; }
    }

    public class Sale_Data
    {
        public long Material { get; set; }
        public long Sales_Org { get; set; }
        public long Distribution_Channel { get; set; }
        public long Material_Statis_Group { get; set; }
        public string Sales_Unit { get; set; }
        public string Item_Category_Group { get; set; }
        public string Product_Hirerachy { get; set; }
        public string Acct_assignment_grp { get; set; }
        public string Material_grp_1 { get; set; }
        public string Material_grp_2 { get; set; }
        public string Material_grp_3 { get; set; }
        public string Material_grp_4 { get; set; }
        public string Product_Hirerachy_Desc { get; set; }
    }

    public class Mat_Conversion_Data
    {
        public long Material { get; set; }
        public string Alt_Unit_Of_Measure { get; set; }
        public string Denominator { get; set; }
        public string Counter { get; set; }
        public string Gross_Weight { get; set; }
        public string Weight_Unit { get; set; }

    }
    public class Plant_Data
    {
        public string Material { get; set; }
        public string Plant { get; set; }
        public string Valua_Category { get; set; }
        public string Profit_Center { get; set; }
    }
}
