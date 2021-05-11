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

namespace M1SF.Services
{
    public static class SchemaManager
    {
        private static string BasicGetRequest(string Uri, string Token)
        {
            var client = new RestClient(Uri);
            var request = new RestRequest(Method.GET);
            request.AddHeader("Authorization", Token);

            client.Timeout = -1;
            IRestResponse response = client.Execute(request);

            return response.Content;
        }
        private static string GetSObjects(string Token, string InstanceUri)
        {
            string RequestUri = StaticResources.GetSobjectListUri(InstanceUri);
            return BasicGetRequest(RequestUri, Token);
        }
        private static List<TargetObject> GetTargetObjects(string TargetConnection, string Sobject)
        {
            var result = new List<TargetObject>();

            using (SqlConnection connection = new SqlConnection(TargetConnection))
            {
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.GetTargetObjects(), connection))
                {
                    connection.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            result.Add(new TargetObject(Sobject, reader.GetString(0), reader.GetString(1)));
                        }
                    }
                }
            }

            return result;
        }
        private static TargetObject GetSObjectSchema(string Token, string InstanceUri, string Sobject)
        {
            string RequestUri = StaticResources.GetSobjectSchemaUri(InstanceUri, Sobject);
            dynamic ResponseObj = JsonConvert.DeserializeObject<ExpandoObject>(BasicGetRequest(RequestUri, Token), new ExpandoObjectConverter());
            
            var tSchema = StaticResources.GetTargetTableName(Sobject);
            var DataTypes = DataTypeDictionary.GetValuePairs();

            foreach (var item in ((IEnumerable<dynamic>)ResponseObj.fields).OrderBy(t => t.name))
            {
                tSchema.Fields.Add(new Field()
                {
                    Name = item.name,
                    Datatype = item.type,
                    TargetDatatype = DataTypes.Where(t => t.Key == item.type).FirstOrDefault().Value,
                    Scale = (int)item.scale,
                    Precision = (int)item.precision,
                    Length = (int)item.length,
                    Nullable = item.nillable
                });
            }
            return tSchema;
        }
        private static TargetObject GetTargetObjectSchema()
        {
            return new TargetObject();
        }
        private static bool SobjectExists(string Token, string InstanceUri, string Sobject)
        {
            string json = GetSObjects(Token, InstanceUri);
            dynamic Sobjects = JsonConvert.DeserializeObject<ExpandoObject>(json, new ExpandoObjectConverter());
            bool exists = ((IEnumerable<dynamic>)Sobjects.sobjects).Where(t => t.name == Sobject).Any();
            return exists;
        }
        private static bool TargetObjectExists(string TargetConnection, string Sobject)
        {
            var TargetObjs = GetTargetObjects(TargetConnection, Sobject);
            var TargetObj = StaticResources.GetTargetTableName(Sobject);
            return TargetObjs.Where(t => t.Schema == TargetObj.Schema && t.Name == TargetObj.Name).Any();
        }
        private static List<string> GetTargetSchemas(string TargetConnection)
        {
            List<string> Schemas = new List<string>();

            using (SqlConnection connection = new SqlConnection(TargetConnection))
            {
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.GetTargetSchemas(), connection))
                {
                    connection.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Schemas.Add(reader.GetString(0));
                        }
                    }
                }
            }

            return Schemas;
        }
        private static void CreateTargetSchema(string TargetConnection, string SchemaName)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Creating schema '"+SchemaName+"'");
            using (SqlConnection connection = new SqlConnection(TargetConnection))
            {
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.CreateTargetSchema(SchemaName), connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        private static void CreateTargetObject(string TargetConnection, TargetObject Tobject)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Creating destination table for SObject '" + Tobject.Sobject + "'");
            using (SqlConnection connection = new SqlConnection(TargetConnection))
            {
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.TargetTableDefinition(Tobject), connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        private static void CreateStagingObjects(string TargetConnection, TargetObject Tobject)
        {
            Tobject.Schema = StaticResources.StageSchema;

            using (SqlConnection connection = new SqlConnection(TargetConnection))
            {
                Console.WriteLine(DateTime.Now.ToString() + ": Creating staging table for SObject '" + Tobject.Sobject + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.TargetTableDefinition(Tobject), connection))
                {
                    connection.Open();
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine(DateTime.Now.ToString() + ": Dropping MERGE stored proc for SObject '" + Tobject.Sobject + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.DropMergeProcSql(Tobject), connection))
                {
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine(DateTime.Now.ToString() + ": Creating MERGE stored proc for SObject '" + Tobject.Sobject + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.StageMergeProcDefinition(Tobject), connection))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
        private static void CreateJsonObjects(string TargetConnection, TargetObject Tobject)
        {
            Tobject.Schema = StaticResources.JsonSchema;
            using (SqlConnection connection = new SqlConnection(TargetConnection))
            {
                connection.Open();
                Console.WriteLine(DateTime.Now.ToString() + ": Dropping json table '" + Tobject.Name + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.DropTableSql(Tobject), connection))
                {
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine(DateTime.Now.ToString() + ": Creating json table '" + Tobject.Name + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.JsonTableDefinition(Tobject), connection))
                {
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine(DateTime.Now.ToString() + ": Dropping json view '" + Tobject.Name + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.DropJsonViewSql(Tobject), connection))
                {
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine(DateTime.Now.ToString() + ": Creating json view '" + Tobject.Name + "'");
                using (SqlCommand cmd = new SqlCommand(SqlTemplates.JsonViewDefinition(Tobject), connection))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
        public static SchemaSyncResult SyncSchema(SchemaSyncRequest request)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Syncing schema for object '"+request.Sobject+"'");
            SchemaSyncResult result = new SchemaSyncResult();

            result.SourceObjectExists = SobjectExists(request.Token, request.InstanceUri, request.Sobject);
            if (!result.SourceObjectExists) { return result; }

            var SSchema = GetSObjectSchema(request.Token, request.InstanceUri, request.Sobject);
            result.Target = SSchema;
            result.GeneratedTargetObject = !(TargetObjectExists(request.TargetConnection, request.Sobject));

            if (result.GeneratedTargetObject)
            {
                CreateTargetObject(request.TargetConnection, SSchema);
                CreateStagingObjects(request.TargetConnection, SSchema);
                CreateJsonObjects(request.TargetConnection, SSchema);
                return result;
            }
            else
            {
                // compare schemas
                // also need to check that objects exist in stage and create them if necessary
            }

            return result;
        }
        public static void SyncTargetSchemas(string TargetConnection)
        {
            var TargetSchemas = GetTargetSchemas(TargetConnection);
            var SchemaList = new List<string>();

            SchemaList.Add(StaticResources.TargetSchema);
            SchemaList.Add(StaticResources.StageSchema);
            SchemaList.Add(StaticResources.JsonSchema);

            foreach (var schema in SchemaList)
            {
                if (!(TargetSchemas.Where(t => t == schema).Any()))
                {
                    CreateTargetSchema(TargetConnection, schema);
                }
            }
        }
    }
}
