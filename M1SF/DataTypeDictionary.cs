using System;
using System.Collections.Generic;
using System.Text;

namespace M1SF
{
    public static class DataTypeDictionary
    {
        public static Dictionary<string, string> GetValuePairs()
        {
            var dictionary = new Dictionary<string, string>();

            dictionary.Add("id",                "nvarchar");
            dictionary.Add("boolean",           "bit");
            dictionary.Add("string",            "nvarchar");
            dictionary.Add("picklist",          "nvarchar");
            dictionary.Add("datetime",          "datetime2");
            dictionary.Add("reference",         "nvarchar");
            dictionary.Add("textarea",          "nvarchar");
            dictionary.Add("double",            "decimal");
            dictionary.Add("address",           "nvarchar");
            dictionary.Add("email",             "nvarchar");
            dictionary.Add("phone",             "nvarchar");
            dictionary.Add("int",               "int");
            dictionary.Add("url",               "nvarchar");
            dictionary.Add("multipicklist",     "nvarchar");

            return dictionary;
        }
    }
}
