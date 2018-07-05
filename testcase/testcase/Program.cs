using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using MassTransit;
using MassTransit.RabbitMqTransport;
using RabbitMQ.Client;

namespace testcase
{
    class Program
    {
        private static string _url = "rabbitmq://nlehvportalbeacc/ontwikkel-smits";
        private static string _user = "console";
        private static string _password = "console";
        
        private static IRabbitMqHost _host;
        private static string ExchangeName = $"testcase-ordered-buffer";

        static void Main(string[] args)
        {
            var busControl = Bus.Factory.CreateUsingRabbitMq(cfg =>
            {
                _host = cfg.Host(new Uri(_url), h =>
                {
                    h.Username(_user);
                    h.Password(_password);
                });
                cfg.Send<MyMessage>(x =>
                {
                    x.UseRoutingKeyFormatter(context => RoutingKeyConvention(context.Message.RoutingKey));
                });
                cfg.Message<MyMessage>(x =>
                {
                    x.SetEntityName(ExchangeName);
                });
            });

            busControl.Start();

            var builder = new ContainerBuilder();
            builder.RegisterType<MyConsumer>();
            var container = builder.Build();

            var ctx = container.Resolve<IComponentContext>();

            Subscribe(ctx, "foo");
            Subscribe(ctx, "bar");
            var input = "";
            do
            {
                Console.WriteLine("ready");
                busControl.Publish(new MyMessage("Hello world", "foo"));
                busControl.Publish(new MyMessage("Hello world", "bar"));    
                input = Console.ReadLine();
            } while (input != "q");

            busControl.Stop();
        }

        static void Subscribe(IComponentContext context, string key)
        {
            var queuename = $"testcase-R-{key}";
            var handle = _host.ConnectReceiveEndpoint(queuename, e =>
            {
                e.BindMessageExchanges = false;
                e.Consumer<MyConsumer>(context); //<-- autofac is used to more closely represent the realworld scenario
                e.Bind(ExchangeName, x =>
                {
                    x.RoutingKey = RoutingKeyConvention(key);
                    x.ExchangeType = ExchangeType.Direct;
                    x.Durable = true;
                });
                //this and SetEntityName are mutually exclusive, causes vague exceptions if you do.
                //Turn this on, and SetEntityName off. And you do receive messages. But routingkey constraints
                //are not enforced. So each consumer receives all published messages. Instead of only the ones belonging to its topic
                //e.Bind<MyMessage>(); 
            });

            handle.Ready.ConfigureAwait(false).GetAwaiter().GetResult();
        }

        static string RoutingKeyConvention(string key)
        {
            Console.WriteLine("routing-convention queried");
            return $"some-prefix-{key}";
        }
    }

    public class MyMessage
    {
        public string RoutingKey { get; set; }

        public string Content { get; set; }

        public MyMessage(string content, string routingKey)
        {
            RoutingKey = routingKey;
            Content = content;
        }
    }

    public class MyConsumer : IConsumer<MyMessage>
    {
        private IComponentContext componentContext;
        public MyConsumer(IComponentContext componentContext)
        {
            this.componentContext = componentContext;
        }

        public Task Consume(ConsumeContext<MyMessage> context)
        {
            Console.WriteLine($"Received {context.Message.Content} for {context.Message.RoutingKey}");
            return Task.CompletedTask;
        }
    }
}
