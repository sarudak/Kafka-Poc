using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaDemoWorker
{
    public class Task    {
        public int TimeInMs { get; set; }
        public string TaskName { get; set; }
        public int FailureChance { get; set; }
        public int UserId { get; set; }
    }
}
