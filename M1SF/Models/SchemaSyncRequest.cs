using System;
using System.Collections.Generic;
using System.Text;

namespace M1SF.Models
{
    public class SchemaSyncRequest
    {
        public string Token { get; set; }
        public string InstanceUri { get; set; }
        public string Sobject { get; set; }
        public string TargetConnection { get; set; }

        public SchemaSyncRequest() { }
        public SchemaSyncRequest(string token, string instanceUri, string sobject, string targetConnection)
        {
            Token = token;
            InstanceUri = instanceUri;
            Sobject = sobject;
            TargetConnection = targetConnection;
        }
    }
}
