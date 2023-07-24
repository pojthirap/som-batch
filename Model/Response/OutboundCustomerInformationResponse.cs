using AzureFunctionApp.Model.Response.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Response.OutboundCustomerInformationResponse
{
    public class OutboundCustomerInformationResponse : ResponseBase<Data>
    {
        public List<Data> GeneralData { get; set; }
    }


    public class Data
    {
        public string Account_Group { get; set; }
        public string Customer { get; set; }
        public string Country { get; set; }
        public string Name_1 { get; set; }
        public string Street { get; set; }
        public string Street_5 { get; set; }
        public string District { get; set; }
        public string City { get; set; }
        public string Postal_Code { get; set; }
        public string Region { get; set; }
        public string Country_Key { get; set; }
        public string Transportation_Zone { get; set; }
        public string Tax_Number_3 { get; set; }
        public string VAT_Registration_No { get; set; }
        public string Status_IND { get; set; }
        public string Location_Code { get; set; }
        public string Name_2 { get; set; }
        public string Search_Term { get; set; }
        public string Telephone_1 { get; set; }
        public List<CompanyData> CompanyData { get; set; }
        public List<SaleData> SaleData { get; set; }
        public List<PartnerData> PartnerData { get; set; }
    }

    public class CompanyData
    {
        public string Customer { get; set; }
        public long Company_Code { get; set; }
        public string Sort_key { get; set; }
        public string Reconciliation_Acct { get; set; }
        public string Terms_Of_Payment { get; set; }
        public string Planning_Group { get; set; }
        public string WHTax_Country { get; set; }
    }

    public class SaleData
    {
        public string Customer { get; set; }
        public long Sales_Organization { get; set; }
        public long Distribution_Channel { get; set; }
        public long Division { get; set; }
        public string Sales_Group { get; set; }
        public string Sales_Office { get; set; }
        public string Delivering_Plant { get; set; }
        public string Sales_District { get; set; }
        public string Cus_Pric_Procedure { get; set; }
        public string Customer_Group { get; set; }
        public string Credit_Control_Area { get; set; }
        public string Order_Probability { get; set; }
        public string Terms_Of_Payment { get; set; }
        public string Incoterms { get; set; }
        public string Terms_Of_Payment_TH { get; set; }
        public string Terms_Of_Payment_EN { get; set; }
        public string Max_Part_Deliveries { get; set; }
        public string Part_Dlv_Item { get; set; }
        public string Order_Combination { get; set; }
        public string Delivery_Priority { get; set; }
        public long Shipping_Conditions { get; set; }
        public string Currency { get; set; }
        public string AcctAssgGr { get; set; }
        public string Customer_Group_1 { get; set; }
        public string Customer_Group_5 { get; set; }
        public string Paymt_Guarant_Proc { get; set; }
        public string Prod_Customer_Proced { get; set; }




        /*public string Material { get; set; }
        public string Alt_Unit_Of_Measure { get; set; }
        public string Denominator { get; set; }
        public string Counter { get; set; }
        public string Gross_Weight { get; set; }
        public string Weight_Unit { get; set; }*/
    }
    public class PartnerData
    {
        public string Customer { get; set; }
        public long Sales_Organization { get; set; }
        public long Distribution_Channel { get; set; }
        public long Division { get; set; }
        public string Partner_Function { get; set; }
        public string Partner_Counter { get; set; }
        public string Customer_Partner { get; set; }
        public string Personnel_Number { get; set; }
        public string Contact_Person { get; set; }
    }
}
