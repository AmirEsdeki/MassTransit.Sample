using System;
using System.Threading;
using System.Threading.Tasks;
using MassTransit;
using Sample.Components;
using Sample.Contracts;

namespace Sample.Contracts
{
    //this would turn into an exchange in RabbitMQ under this name => Sample.Contracts:UpdateAccount 
    //the exchange type is fanout
    //this could be a class too, nothing forces us to use abstaraction here but it's a better way to do this
    //all the events that publish on this exchange(using send or publish of type IUpdateAccount) will get consumed by all the exchanges bound to it
    //by creating a consumer of type UpdateAccount another exchange and accordingly another queue will bound to this exchange 
    //published events will get consumed by all the registered consumers to this exchange (by inheriting IConsumer<IUpdateAccount>)
    //sent "commands" need to have specified the complete route to the queue to get consumed, on the other hand published events does not have a route and is excuted by every registered service
    public interface IUpdateAccount
    {
        string AccountNumber { get; }
    }
}

namespace Sample.Components
{
    //the consume method in this class will respond every command or event of type "UpdateAccount"
    //when a command or event with type "UpdateAccount" is published or sent, the Consume method will be executed
    //every consumer class needs to implement "IConsumer<type_of_event_or_command>" interface
    public class AccountConsumer : IConsumer<IUpdateAccount>
    {
        //the implementation of this method get executed everytime an event or command of type "IUpdateAccount" is recieved
        public Task Consume(ConsumeContext<IUpdateAccount> context)
        {
            Console.WriteLine($"Command Recieved: {context.Message.AccountNumber}");
            return Task.CompletedTask;
        }
    }
}


namespace MassTransit.Sample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //create a bus to communicate with RabbitMq server
            //this has various configurations like host, RecieveEndpoints and so on.
            var busControl = Bus.Factory.CreateUsingRabbitMq(cfg =>
            {
                //rabbitmq host configs
                cfg.Host("localhost", h =>
                 {
                     h.Username("guest");
                     h.Password("guest");
                 });

                //creates a queue and an exchange named "account-service" that is bound to Sample.Contracts:UpdateAccount exchange; becuase we want to. => r.Consumer<AccountConsumer>();
                //this will only Consume messages of type IUpdateAccount becuase "AccountConsumer" is of type "IUpdateAccount"
                cfg.ReceiveEndpoint("account-service", r =>
                {
                    //here we can set lots of rabbitmq configs before creating the queue

                    //this option will avoid overloading memory, when we set the queue lazy option to true it wont keep all the messages in memory, it saves memory on the broker server
                    //suitable for machines with low available memory, an optimizing tip
                    r.Lazy = true;

                    //must set to an optimal number of count that the application can process in one second.
                    //this number of messages will fetch at once from the queue.
                    r.PrefetchCount = 10;

                    //specify which type of commands or events we are intrested in
                    //in this case this will respond commands of type "IUpdateAccount"
                    //becuase "AccountConsumer" class is of type "IConsumer<IUpdateAccount>"
                    r.Consumer<AccountConsumer>();
                });
            });

             
            //We need to create a cancellation token becuase it might took hours to connect to a dead RabbitMq server.
            using var cancellationToken = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            await busControl.StartAsync(cancellationToken.Token);

            try
            {
                Console.WriteLine("Bus has Started!!");


                //MassTransit supports two mode of sending messages=> send and publish. 
                //Send is for sending commands, they must be sent to a specifiec route (address of queue).
                //Publish is for publishing events, they will sent to every service that is registered to the event type.

                //send a command
                //in order to send a command we must have an endpoint to send the message to.
                var endpoint = await busControl.GetSendEndpoint(new Uri("exchange:account-service"));
                //The Send method by itself does not know where to send the message so we have specify the endpoint explicitly in order to send the command to only that endpoint.
                await endpoint.Send<IUpdateAccount>(new
                {
                    AccountNumber = "Sent:12314424"
                });


                //publish an event
                //In contrast when we publish an event, the Publish method knows where to send the event based on the type of the event
                //Below we have set the type of the event to UpdateAccount so it basically know it needs to publish on Sample.Contracts:UpdateAccount Exchange
                //and this event will publish on every queue that has bound to this exchange
                await busControl.Publish<IUpdateAccount>(new
                {
                    AccountNumber = "Published:54643464"
                });


            }
            finally
            {
                await busControl.StopAsync(CancellationToken.None);
            }
        }
    }
}
