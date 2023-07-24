using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureFunctionApp.common
{
    public static class CommonConstant
    {
        public static string CONNECTIONSTRING_TEXT = "connectionString";
        public static string SAP_INTERFACE_PASSWORD_TEXT = "SapInterfacePassword";
        public static string SAP_INTERFACE_REQ_KEY_TEXT = "SapInterfacePassReqKey";
        // PROD
        public static string KEYVALUE_TXT = "prd-saleonmobile-condb";
        public static string KEYVALUE_SAP_INTERFACE_PASSWORD_TXT = "prd-saleonmobile-sappwd";
        public static string KEYVALUE_SAP_INTERFACE_REQ_KEY_TXT = "prd-saleonmobile-sap-reqkey";
        // UAT
        //public static string KEYVALUE_TXT = "uat-dbcon-saleonmob";
        //public static string KEYVALUE_SAP_INTERFACE_PASSWORD_TXT = "uat-saleonmobile-sappwd";
        // DEV 
        //public static string KEYVALUE_TXT = "dev-uat-database-saleonmob";
        //public static string KEYVALUE_SAP_INTERFACE_PASSWORD_TXT = "dev-saleonmobile-sappwd";
        // DEV & UAT
        //public static string KEYVALUE_SAP_INTERFACE_REQ_KEY_TXT = "dev-uat-saleonmobile-sap-reqkey";


        public static string VERSION = "1.0.3";

        public static string API_OutboundCompanyInformation =  "ZSOMI001_OutboundCompanyInformation/";
        public static string API_OutboundSaleOrgInformation =  "ZSOMI002_OutboundSaleOrgInformation/";
        public static string API_OutboundSaleOfficeInformation =  "ZSOMI003_OutboundSaleOfficeInformation/";
        public static string API_OutboundSaleGroupInformation =  "ZSOMI004_OutboundSaleGroupInformation/";
        public static string API_OutboundSaleOfficetoSaleGroupInformation =  "ZSOMI005_OutboundSaleOfficetoSaleGroupInformation/";
        public static string API_OutboundSaleAreaInformation =  "ZSOMI006_OutboundSaleAreaInformation/";
        public static string API_OutboundSaleOfficeToSale =  "ZSOMI007_OutboundSaleOfficeToSale/";
        public static string API_OutboundCustomerInformation =  "ZSOMI011_OutboundCustomerInformation/";
        public static string API_OutboundSaleMaterialMaster =  "ZSOMI012_OutboundSaleMaterialMaster/";
        public static string API_OutboundSaleOrderDocumentTypes =  "ZSOMI016_OutboundSaleOrderDocumentTypes/";
        public static string API_OutboundSaleOrderDocumentTypesToSaleArea =  "ZSOMI017_OutboundSaleOrderDocumentTypesToSaleArea/";
        public static string API_OutboundSaleOrderReasons =  "ZSOMI018_OutboundSaleOrderReasons/";
        public static string API_OutboundSaleOrderItemType =  "ZSOMI019_OutboundSaleOrderItemType/";
        public static string API_OutboundRegionInformation =  "ZSOMI023_OutboundRegionInformation/";
        public static string API_OutboundCountryInformation =  "ZSOMI024_OutboundCountryInformation/";
        public static string API_OutboundBAInformation =  "ZSOMI025_OutboundBAInformation/";
        public static string API_OutboundIncotermInformation =  "ZSOMI028_OutboundIncotermInformation/";
        public static string API_OutboundSubDistrictInformation =  "ZSOMI029_OutboundSubDistrictInformation/";
        public static string API_OutboundDistrictInformation =  "ZSOMI030_OutboundDistrictInformation/";
        public static string USER_AGEN = "PTG-Batch SlaeOnMobile";
        public static string API_TIMEOUT = "600";
        public static string ReqKey = "req-key";
        public static string GetLoggerString = "FunctionAppLogs";

    }
}
