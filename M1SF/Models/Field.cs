using System;
using System.Collections.Generic;
using System.Text;

namespace M1SF.Models
{
    public class Field
    {
        public string Name { get; set; }
        public string Datatype { get; set; }
        public string TargetDatatype { get; set; }
        public int Scale { get; set; }
        public int Precision { get; set; }
        public int Length { get; set; }
        public bool Nullable { get; set; }
        //public List<Field> ChildFields (Address DT)
    }
}
