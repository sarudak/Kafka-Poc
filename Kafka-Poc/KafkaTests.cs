using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Internal;
using RdKafka;

namespace Kafka_Poc
{
    [TestFixture]
    public class KafkaTests
    {
        private string _hostIp = "192.168.56.1";

        private async void WriteToQueue(string message)
        {
            using (Producer producer = new Producer(_hostIp + ":9092"))
            using (Topic topic = producer.Topic("test"))
            {
                byte[] data = Encoding.UTF8.GetBytes(message);
                DeliveryReport deliveryReport = await topic.Produce(data);
                Console.WriteLine($"Produced to Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
            }
        }

        [Test]
        public void Add_One_Message_To_Log()
        {
            WriteToQueue("PleaseWork");
        }

        [Test]
        public void TryDeserializeWithExtraFields()
        {
            var json = @"{
                  'TimeInMs': '650',
                  'Message': 'Cowboys are great!',
                  'EnterpriseId': '122131231'
                }";

            var item = JsonConvert.DeserializeObject<WorkItem>(json);

            Assert.That(item.TimeInMs, Is.EqualTo(650));
        }


    }

    public class WorkItem
    {
        public int TimeInMs { get; set; }
        public string Message { get; set; }
    }
}
