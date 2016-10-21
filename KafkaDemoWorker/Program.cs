using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RdKafka;

namespace KafkaDemoWorker
{
    class Program
    {
        private static string _hostIp = "54.191.31.21";

        private static bool ExecuteTask(Task task)
        {
            Thread.Sleep(task.TimeInMs);
            Console.WriteLine(String.Format("After {0} ms of work completed task: {1}", task.TimeInMs, task.TaskName));
            return true;
        }

        static void Main(string[] args)
        {
            var config = new Config() { GroupId = "worker-consumer" };
            using (var consumer = new EventConsumer(config, _hostIp + ":9092"))
            {
                consumer.OnError += (obj, msg) =>
                {
                    Console.Out.WriteLine($"Kafka consumer had an error {msg.ErrorCode} with message {msg.Reason}");
                };

                consumer.OnMessage += (obj, msg) =>
                {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    Console.WriteLine($"Recieved Message Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
                    ExecuteTask(JsonConvert.DeserializeObject<Task>(text));
                };
            
                consumer.Subscribe(new List<String> { "distributed-work" });
                consumer.Start();

                Console.WriteLine("Started consumer, press enter to stop consuming");
                Console.ReadLine();
            }
        }
    }
}
