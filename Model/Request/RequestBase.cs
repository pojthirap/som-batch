using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionApp.Model.Request.Base
{
    public class RequestBase
    {      
        public string Interface_ID { get; set; }
        public string Table_Object { get; set; }
    }
}
