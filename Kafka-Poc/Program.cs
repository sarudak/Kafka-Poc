using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RdKafka;

namespace Kafka_Poc
{
    class Program
    {
        private static string _hostIp = "192.168.56.1";



        static void Main(string[] args)
        {
            var config = new Config() { GroupId = "example-csharp-consumer" };
            using (var consumer = new EventConsumer(config, _hostIp + ":9092"))
            {
                consumer.OnMessage += (obj, msg) =>
                {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
                };

                consumer.Subscribe(new List<String> {"test"});
                consumer.Start();

                Console.WriteLine("Started consumer, press enter to stop consuming");
                Console.ReadLine();
            }
        }
    }
}
