using M1SF.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;

namespace M1SF.Services
{
    public class EtlInstance
    {
        public string SObject { get; set; }
        private readonly EtlManager etlManager;
        private readonly CredentialManager credential;
        private DateTime Watermark;
        private Thread etlThread;
        private StatusEnum Status;
        public EtlInstance(string sObject, EtlManager etl, CredentialManager cm, DateTime watermark)
        {
            Console.WriteLine(DateTime.Now.ToString() + @": Creating ETL Instance for '"+sObject+"'");
            SObject = sObject;
            Watermark = watermark;
            etlManager = etl;
            credential = cm;
            etlThread = new Thread(Start);
            Status = StatusEnum.Created;
            etlThread.Start();
        }
        public void Stop()
        {

        }
        public void Start()
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Starting ETL Instance for '" + SObject + "'");
            Status = StatusEnum.Starting;
            var schemaResult = SchemaManager.SyncSchema(GetSchemaSyncRequest());
            TruncateStage(schemaResult.Target);
            Status = StatusEnum.Started;
            Console.WriteLine(DateTime.Now.ToString() + ": ETL Instance for '" + SObject + "' started successfully");

            var NextWatermark = DateTime.UtcNow;

            EtlRequest(schemaResult);
            StageRequest(schemaResult.Target);
            MergeRequest(schemaResult.Target);

            Watermark = NextWatermark.AddMinutes(-5);

            while (true)
            {
                schemaResult = SchemaManager.SyncSchema(GetSchemaSyncRequest());
                TruncateStage(schemaResult.Target);

                NextWatermark = DateTime.UtcNow;

                EtlRequest(schemaResult);
                StageRequest(schemaResult.Target);
                MergeRequest(schemaResult.Target);

                Watermark = NextWatermark.AddMinutes(-5);
            }
            
        }
        public StatusEnum GetStatus()
        {
            return Status;
        }
        private SchemaSyncRequest GetSchemaSyncRequest()
        {
            return new SchemaSyncRequest(credential.GetToken(), credential.GetInstanceUri(), SObject, etlManager.GetTargetConnection());
        }
        private void EtlRequest(SchemaSyncResult SyncResult)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Retrieving data for '"+SObject+"' since '"+Watermark.ToString()+"'");
            var batches = GetSoqlBatches(SyncResult.Target.Fields);
            int batchId = 1;
            foreach (var batch in batches)
            {
                string Soql = "SELECT ";
                int c = 1;
                foreach(var field in batch)
                {
                    if (c == 1)
                    {
                        Soql += field;
                    }
                    else
                    {
                        Soql += ", " + field;
                    }
                    c++;
                }
                Soql += " FROM " + SObject + " WHERE systemmodstamp >= " + Watermark.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fffK");

                string Uri = credential.GetInstanceUri() + "/services/data/v51.0/query/?q=" + Soql;
                string response = BasicGetRequest(Uri, credential.GetToken());
                
                SaveJsonResponse(batchId, response);
                dynamic responseObj = JsonConvert.DeserializeObject<ExpandoObject>(response, new ExpandoObjectConverter());
                while (!responseObj.done)
                {
                    Uri = credential.GetInstanceUri() + responseObj.nextRecordsUrl;
                    response = BasicGetRequest(Uri, credential.GetToken());
                    SaveJsonResponse(batchId, response);
                    responseObj = JsonConvert.DeserializeObject<ExpandoObject>(response, new ExpandoObjectConverter());
                }
                batchId++;
            }

        }
        private void StageRequest(TargetObject Target)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Staging data for '" + SObject + "'");
            using (SqlConnection connection = new SqlConnection(etlManager.GetTargetConnection()))
            {
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.GetStageSql(Target), connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        private void MergeRequest(TargetObject Target)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Merging data for '" + SObject + "'");
            string sql = "EXEC [" + StaticResources.StageSchema + "].[" + StaticResources.ProcPrefix + "_MERGE_" + Target.Sobject + "];";
            using (SqlConnection connection = new SqlConnection(etlManager.GetTargetConnection()))
            {
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        private List<List<string>> GetSoqlBatches(List<Field> fields)
        {
            var BatchFields = fields.Where(f => f.Name.ToUpper() != "ID").ToList();

            int Cols = StaticResources.MaxSoqlColumns - 1;
            List<List<string>> jsonBatches = new List<List<string>>();
            List<string> batch = new List<string>();

            for (int i = 0; i < BatchFields.Count; i++)
            {
                if (i == 0)
                {
                    batch.Add("Id");
                }
                else if (i % Cols == 0 && i != 0)
                {
                    jsonBatches.Add(batch);
                    batch = new List<string>();
                    batch.Add("Id");
                }
                batch.Add(BatchFields[i].Name);

                if (i == BatchFields.Count - 1)
                {
                    jsonBatches.Add(batch);
                }
            }

            return jsonBatches;
        }
        private string BasicGetRequest(string Uri, string Token)
        {
            var client = new RestClient(Uri);
            var request = new RestRequest(Method.GET);
            request.AddHeader("Authorization", Token);

            client.Timeout = -1;
            IRestResponse response = client.Execute(request);

            return response.Content;
        }
        private void SaveJsonResponse(int BatchId, string Response)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Saving json response for '" + SObject + "' batch '"+BatchId.ToString()+"'");
            using (SqlConnection connection = new SqlConnection(etlManager.GetTargetConnection()))
            {
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.AddJsonSql(SObject, BatchId, Response), connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        private void TruncateStage(TargetObject Target)
        {
            string sql = "TRUNCATE TABLE [" + StaticResources.StageSchema + "].[" + Target.Sobject + "];";
            sql += "\nTRUNCATE TABLE [" + StaticResources.JsonSchema + "].[" + Target.Sobject + "];";

            Console.WriteLine(DateTime.Now.ToString() + ": Clearing stage data for '" + SObject + "'");
            using (SqlConnection connection = new SqlConnection(etlManager.GetTargetConnection()))
            {
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
