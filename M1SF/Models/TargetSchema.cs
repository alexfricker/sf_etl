using System;
using System.Collections.Generic;
using System.Text;

namespace M1SF.Models
{
    public class TargetSchema
    {
        public string Sobject { get; set; }
        public List<Field> Fields { get; set; }
        public TargetSchema()
        {
            Fields = new List<Field>();
        }
    }
}
