using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace Kafka_Fiap
{
    class Program
    {
        static void Main(string[] args)
        {

            var configProducer = new ProducerConfig();
            configProducer.BootstrapServers = "";
            configProducer.SecurityProtocol = SecurityProtocol.SaslSsl;
            configProducer.SaslMechanism = SaslMechanism.ScramSha256;
            configProducer.SaslUsername = "";
            configProducer.SaslPassword = "";

            string topic = "";

           // KafkaProducer(configProducer, topic).Wait();


            var configConsumer = new ConsumerConfig();
            configConsumer.BootstrapServers = "";
            configConsumer.SecurityProtocol = SecurityProtocol.SaslSsl;
            configConsumer.SaslMechanism = SaslMechanism.ScramSha256;
            configConsumer.SaslUsername = "";
            configConsumer.SaslPassword = "";

            configConsumer.GroupId = "";
            configConsumer.AutoOffsetReset = AutoOffsetReset.Earliest;

            KafkaConsumer(configConsumer, topic);
        }


        static async Task KafkaProducer(ProducerConfig config, string topic)
        {
            string nome = "Edenilson";

            var builder = new ProducerBuilder<string, string>(config);

            using (var producer = builder.Build())
            {
                for (int i = 0; i < 10; i++)
                {
                    var message = new Message<string, string>
                    {
                        Key = nome,
                        Value = "Mensagem de " + nome + " : " + i
                    };

                    await producer.ProduceAsync(topic, message);
                }
            }

        }

        static void KafkaConsumer(ConsumerConfig config, string topic)
        {
            int count = 0;

            var builder = new ConsumerBuilder<string, string>(config);

            using (var consumer = builder.Build())
            {
                consumer.Subscribe(topic);

                while (true)
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(1));

                    if (result != null)
                    {
                        string texto = result.Message.Value;

                        int part = result.Partition.Value;

                        Console.WriteLine($"{count++}: recebido {texto} (Particao: {part})");
                    }
                }
            }
        }
    }
}
