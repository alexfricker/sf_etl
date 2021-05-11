using M1SF.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace M1SF
{
    public static class SqlTemplates
    {
        public static string GetTargetObjects()
        {
            return @"SELECT [schema] = TABLE_SCHEMA
	            ,[name] = TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES

            UNION ALL

            SELECT [schema] = ROUTINE_SCHEMA
	            ,[name] = ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'";
        }
        public static string GetTargetSchemas()
        {
            return @"SELECT [name] = SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA";
        }
        public static string CreateTargetSchema(string SchemaName)
        {
            return @"CREATE SCHEMA [" + SchemaName + "];";
        }
        private static string GetFieldSql(Field field)
        {
            var SqlField = "\n\t\t["
                            + field.Name
                            + "]\t"
                            + GetDataTypeSql(field)
                            + "\t"
                            + (!field.Nullable ? "NOT " : "") + "NULL";

            return SqlField;
        }
        private static string GetDataTypeSql(Field field)
        {
            string result = "[" + field.TargetDatatype + "]";

            switch (field.TargetDatatype)
            {
                case "nvarchar":
                    {
                        if (field.Length == 0 || field.Length > 4000)
                        {
                            result += "(max)";
                        }
                        else
                        {
                            result += "(" + field.Length.ToString() + ")";
                        }
                    }
                    break;
                case "datetime2":
                    result += "(7)";
                    break;
                case "decimal":
                    result += "(" + field.Precision.ToString() + "," + field.Scale.ToString() + ")";
                    break;
            }

            return result;
        }
        public static string TargetTableDefinition(TargetObject Target)
        {
            string Query = "";

            if (Target.Schema == StaticResources.StageSchema)
            {
                Query += DropTableSql(Target);
            }

            Query += "CREATE TABLE [" + Target.Schema + "].[" + Target.Name + @"]
            (";

            int c = 1;
            foreach (var field in Target.Fields)
            {
                if (c != 1) { Query += ","; }
                Query += GetFieldSql(field);
                c++;    // heh
            }
            Query += (",CONSTRAINT [pk_" + Target.Schema + "_" + Target.Name + "_Id] PRIMARY KEY(Id)");
            Query += ");";

            return Query;
        }
        public static string DropTableSql(TargetObject Target)
        {
            return "DROP TABLE IF EXISTS [" + Target.Schema + "].[" + Target.Name + "]\n";
        }
        public static string StageMergeProcDefinition(TargetObject Target)
        {
            int c = 1;
            string ProcName = "[" + StaticResources.StageSchema + "].["
                            + StaticResources.ProcPrefix + "_MERGE_" + Target.Name + "]";

            string StageTable = "[" + StaticResources.StageSchema + "].[" + Target.Name + "]";

            string ProdTable = "[" + StaticResources.TargetSchema + "].[" + Target.Name + "]";

            string Query = "";

            Query += (@"CREATE PROCEDURE " + ProcName + @" AS

-- Process Inserts
INSERT INTO " + ProdTable + @"
(");
            foreach (var field in Target.Fields)
            {
                Query += ("\n\t" + (c == 1 ? " [" : ",[") + field.Name + "]");
                c++;
            }

            Query += "\n)\nSELECT ";

            c = 1;
            foreach (var field in Target.Fields)
            {
                if (c == 1)
                { Query += "[stage].[" + field.Name + "]"; }
                else
                { Query += "\n\t,[stage].[" + field.Name + "]"; }
                c++;
            }

            Query += "\nFROM " + StageTable + @" stage
	LEFT OUTER JOIN " + ProdTable + @" prod ON (stage.Id = prod.Id)
WHERE prod.Id IS NULL;

-- Process Updates
UPDATE prod
SET ";
            c = 1;
            foreach (var field in Target.Fields)
            {
                if (field.Name.ToUpper() != "ID")
                {
                    if (c == 1)
                    { Query += "[" + field.Name + "] = [stage].[" + field.Name + "]"; }
                    else
                    { Query += "\n\t,[" + field.Name + "] = [stage].[" + field.Name + "]"; }
                }
                c++;
            }

            Query += "\nFROM " + StageTable + @" stage
	INNER JOIN " + ProdTable + @" prod ON (stage.Id = prod.Id)
WHERE ";
            c = 1;
            foreach (var field in Target.Fields)
            {
                if (field.Name.ToUpper() != "ID")
                {
                    if (c == 1)
                    {
                        if (field.Nullable)
                        {
                            Query += "ISNULL([prod].[" + field.Name + "], " + GetNullReplacement(field) + ") <> ISNULL([stage].[" + field.Name + "], " + GetNullReplacement(field) + ")";
                        }
                        else
                        {
                            Query += "[prod].[" + field.Name + "] <> [stage].[" + field.Name + "]";
                        }
                    }
                    else
                    {
                        if (field.Nullable)
                        {
                            Query += "\n\tOR ISNULL([prod].[" + field.Name + "], " + GetNullReplacement(field) + ") <> ISNULL([stage].[" + field.Name + "], " + GetNullReplacement(field) + ")";
                        }
                        else
                        {
                            Query += "\n\tOR [prod].[" + field.Name + "] <> [stage].[" + field.Name + "]";
                        }
                    }
                }
                c++;
            }

            Query += ";";

            return Query;
        }
        public static string DropMergeProcSql(TargetObject Target)
        {
            string ProcName = "[" + StaticResources.StageSchema + "].["
                            + StaticResources.ProcPrefix + "_MERGE_" + Target.Name + "]";
            return "DROP PROCEDURE IF EXISTS " + ProcName + ";";
        }
        public static string JsonTableDefinition(TargetObject Target)
        {
            return @"CREATE TABLE [" + Target.Schema + @"].[" + Target.Name + @"]
(
	[JsnBody] NVARCHAR(MAX) NOT NULL
    ,[BatchId] INT NOT NULL
)";
        }
        public static string AddJsonSql(string Sobject, int BatchId, string json)
        {
            return @"INSERT INTO [" + StaticResources.JsonSchema + @"].[" + Sobject + @"] (JsnBody,BatchId)
VALUES (N'" + json.Replace("'", "''") + "', " + BatchId.ToString() + ");";
        }
        public static string JsonViewDefinition(TargetObject Target)
        {
            string ViewName = "[" + Target.Schema + "].[" + StaticResources.ViewPrefix + "_" + Target.Name + "]";
            var batches = GetSoqlBatches(Target.Fields);
            string query = "CREATE VIEW " + ViewName + " AS \nWITH ";

            int c = 1;
            foreach (var batch in batches)
            {
                string cteName = "JsnBase" + c.ToString();
                if (c == 1) { query += cteName; }
                else { query += "," + cteName; }
                query += @" AS
(
    SELECT Records = JSON_QUERY(JsnBody, '$.records')
    FROM[jsn].[User]
    WHERE BatchId = " + c.ToString() + @"
)";
                c++;
            }

            query += "\nSELECT batch1.Id";

            foreach (var field in Target.Fields)
            {
                if (!(field.Name == "Id"))
                {
                    if (field.TargetDatatype == "datetime2") { query += "\n\t,[" + field.Name + "] = CONVERT(DATETIME2(7), STUFF([" + field.Name + "],27,0,':'))"; }
                    else { query += "\n\t,[" + field.Name + "]"; }
                }
            }

            query += "\nFROM";

            c = 1;
            foreach (var batch in batches)
            {
                if (c != 1)
                {
                    query += "\nLEFT OUTER JOIN";
                }
                query += "\n(\n\tSELECT ";

                int i = 1;
                foreach (var field in batch)
                {
                    if (i == 1)
                    {
                        query += "[" + field + "]";
                    }
                    else
                    {
                        query += "\n\t\t,[" + field + "]";
                    }
                    i++;
                }
                query += "\n\tFROM JsnBase" + c.ToString() + "\n\t\tOUTER APPLY OPENJSON(Records)\n\t\tWITH\n\t\t(";

                i = 1;
                foreach (var field in batch)
                {
                    var TargetField = Target.Fields.Where(f => f.Name == field).FirstOrDefault();
                    var DataType = GetDataTypeSql(TargetField);
                    if (DataType == "[datetime2](7)") { DataType = "[nvarchar](100)"; }

                    if (i == 1)
                    {
                        query += "\n\t\t\t[" + field + "]\t" + DataType + "\t'$." + field + "'";
                    }
                    else
                    {
                        query += "\n\t\t\t,[" + field + "]\t" + DataType + "\t'$." + field + "'";
                    }
                    i++;
                }

                query += "\n\t\t)\n) batch" + c.ToString();

                if (c != 1)
                {
                    query += " ON (batch1.Id = batch" + c.ToString() + ".Id)";
                }
                c++;
            }
            query += "\nWHERE batch1.Id IS NOT NULL;";

            return query;
        }
        public static string DropJsonViewSql(TargetObject Target)
        {
            string ViewName = "[" + Target.Schema + "].[" + StaticResources.ViewPrefix + "_" + Target.Name + "]";
            return "DROP VIEW IF EXISTS " + ViewName;
        }
        private static List<List<string>> GetSoqlBatches(List<Field> fields)
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
        public static string GetStageSql(TargetObject Target)
        {
            string query = "INSERT INTO [" + StaticResources.StageSchema + "].[" + Target.Name + "]\n(";

            int c = 1;
            foreach (var field in Target.Fields)
            {
                if (c == 1) { query += "\n\t[" + field.Name + "]"; }
                else { query += "\n\t,[" + field.Name + "]"; }
                c++;
            }
            query += "\n)\nSELECT ";

            c = 1;
            foreach (var field in Target.Fields)
            {
                string select;
                if (field.Nullable && field.TargetDatatype == "nvarchar")
                {
                    select = "[" + field.Name + "] = NULLIF([" + field.Name + "], N'')";
                }
                else { select = "[" + field.Name + "]"; }

                if (c == 1) { query += "\n\t" + select; }
                else { query += "\n\t," + select; }
                c++;
            }

            query += "\nFROM [" + StaticResources.JsonSchema + "].[" + StaticResources.ViewPrefix + "_" + Target.Sobject + "];";

            return query;
        }
        private static string GetNullReplacement(Field field)
        {
            string nullrepl = "";
            switch (field.TargetDatatype)
            {
                case "nvarchar":
                    nullrepl = "N''";
                    break;
                case "bit":
                    nullrepl = "0";
                    break;
                case "datetime2":
                    nullrepl = "N'1900-01-01'";
                    break;
                case "decimal":
                    nullrepl = "0.0";
                    break;
                case "int":
                    nullrepl = "0";
                    break;
            }

            return nullrepl;
        }
        public static void AddTargetColumn()
        {
            // ALTER TABLE x ADD col
        }
        public static void AlterTargetColumn()
        {
            // ALTER TABLE x ALTER COLUMN col
            // handle deletes? set col = nullable
        }

    }
}
