using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RdKafka;

namespace KafkaDemoProducer
{
    class Program
    {
        private static string _hostIp = "ec2-54-191-31-21.us-west-2.compute.amazonaws.com";


        private static async void WriteToQueue(Topic topic, string message)
        {

                byte[] data = Encoding.UTF8.GetBytes(message);
                DeliveryReport deliveryReport = await topic.Produce(data);
                Console.WriteLine($"Produced to Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
        }

        static void Main(string[] args)
        {
            var workEntries = File.ReadLines("tasks-40000-even.txt");

            using (Producer producer = new Producer(_hostIp + ":9092"))
            using (Topic topic = producer.Topic("distributed-work"))
            {
                producer.OnError += (obj, msg) =>
                {
                    Console.Out.WriteLine($"Kafka producer had an error {msg.ErrorCode} with message {msg.Reason}");
                };

                foreach (var workEntry in workEntries)
                {
                    Thread.Sleep(1000);
                    WriteToQueue(topic, workEntry);
                }
            }
        }
    }
}
