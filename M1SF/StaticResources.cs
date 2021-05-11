using M1SF.Models;
using System;
using System.Collections.Generic;
using System.Security;
using System.Text;

namespace M1SF
{
    public static class StaticResources
    {
        public static string BaseURI = "test.salesforce.com";
        public static string DestinationDb = "Data Source=(local);Initial Catalog=m1sf;Integrated Security=True;";
        public static int TokenRefreshSec = 30;
        public static int MaxSoqlColumns = 100;
        public static string TargetSchema = "dbo";
        public static string StageSchema = "stage";
        public static string JsonSchema = "jsn";
        public static string ProcPrefix = "usp";
        public static string ViewPrefix = "uvw";
        public static List<string> GetSObjects()
        {
            List<string> SObjects = new List<string>();
            SObjects.Add("User");
            //SObjects.Add("Lead");
            //SObjects.Add("Contact");
            //SObjects.Add("Account");
            return SObjects;
        }

        public static string GetSfLoginEnvelope(string username, string password)
        {
            return @"<?xml version=""1.0"" encoding=""utf-8"" ?>
<env:Envelope xmlns:xsd=""http://www.w3.org/2001/XMLSchema""
    xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance""
    xmlns:env=""http://schemas.xmlsoap.org/soap/envelope/"">
  <env:Body>
    <n1:login xmlns:n1=""urn:partner.soap.sforce.com"">
      <n1:username>" + SecurityElement.Escape(username) + @"</n1:username>
      <n1:password>" + SecurityElement.Escape(password) + @"</n1:password>
    </n1:login>
  </env:Body>
</env:Envelope>";
        }
        public static string GetSfLoginUri(string baseUri)
        {
            return @"https://" + baseUri + @"/services/Soap/u/51.0";
        }
        public static string GetSobjectListUri(string instanceUri)
        {
            return instanceUri + @"/services/data/v51.0/sobjects/";
        }
        public static string GetSobjectSchemaUri(string instanceUri, string Sobject)
        {
            return GetSobjectListUri(instanceUri) + Sobject + @"/describe";
        }
        public static TargetObject GetTargetTableName(string Sobject)
        {
            return new TargetObject(Sobject, TargetSchema, Sobject);
        }
        public static TargetObject GetStageTableName(string Sobject)
        {
            return new TargetObject(Sobject, StageSchema, Sobject);
        }
    }
}
