using System;
using System.Collections.Generic;
using System.Text;

namespace M1SF.Models
{
    public class SchemaSyncResult
    {
        public bool SourceObjectExists { get; set; }
        public bool GeneratedTargetObject { get; set; }
        public bool Unmodified { get; set; }
        public List<string> DeletedCols { get; set; }
        public List<string> ModifiedCols { get; set; }
        public TargetObject Target { get; set; }
        public SchemaSyncResult()
        {
            DeletedCols = new List<string>();
            ModifiedCols = new List<string>();
        }
    }
}
