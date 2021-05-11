using System;
using System.Collections.Generic;
using System.Text;

namespace M1SF.Models
{
    public class TargetObject
    {
        public string Sobject { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }
        public List<Field> Fields { get; set; }

        public TargetObject()
        {
            Fields = new List<Field>();
        }
        public TargetObject(string sobject, string schema, string name)
        {
            Sobject = sobject;
            Schema = schema;
            Name = name;
            Fields = new List<Field>();
        }
        public TargetObject(string sobject, string schema, string name, List<Field> fields)
        {
            Sobject = sobject;
            Schema = schema;
            Name = name;
            Fields = fields;
        }
        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (!(obj is TargetObject))
            {
                return false;
            }
            return (this.Sobject == ((TargetObject)obj).Sobject)
                && (this.Schema == ((TargetObject)obj).Schema)
                && (this.Name == ((TargetObject)obj).Name);
        }
        public override int GetHashCode()
        {
            return Sobject.GetHashCode() ^ Schema.GetHashCode() ^ Name.GetHashCode();
        }
    }
}
