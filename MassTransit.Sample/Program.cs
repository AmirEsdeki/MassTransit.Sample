using System;
using System.Threading;
using System.Threading.Tasks;
using MassTransit;
using MassTransit.Sample.Consumers;
using MassTransit.Sample.Contracts;

namespace MassTransit.Sample.Contracts
{
    //this would turn into an exchange in RabbitMQ under this name => MassTransit.Sample.Contracts:UpdateAccount 
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

    public interface IRemoveAccount
    {
        string AccountNumber { get; }
    }

    public interface ICreateAccount
    {
        string AccountNumber { get; }
    }
}

namespace MassTransit.Sample.Consumers
{
    //the consume method in this class will respond every command exactly to this route has sent or event of type "UpdateAccount" has published
    //when a command or event with type "UpdateAccount" is published or sent, the Consume method will be executed
    //every consumer class needs to implement "IConsumer<type_of_event_or_command>" interface
    public class UpdateAccountConsumer : IConsumer<IUpdateAccount>
    {
        //the implementation of this method get executed everytime an event of type IUpdateAccount has published or command of type "IUpdateAccount" has recieved
        public Task Consume(ConsumeContext<IUpdateAccount> context)
        {
            Console.WriteLine($"Message Recieved: {context.Message.AccountNumber}");
            return Task.CompletedTask;
        }
    }

    //this consumer will be created right after we have created a ReceiveEndpoint for it. without a ReceiveEndpoint it will not workor create anything special
    //before using ReceiveEndpoint in busControl Configs, it will not create any kind of exchange or queues on RabbitMq.
    public class AnotherUpdateAccountConsumer : IConsumer<IUpdateAccount>
    {
        //the implementation of this method get executed everytime an event of type IUpdateAccount has published or command of type "IUpdateAccount" has recieved
        public Task Consume(ConsumeContext<IUpdateAccount> context)
        {
            Console.WriteLine($"Message Recieved In AnotherUpdateAccountConsumer: {context.Message.AccountNumber}");
            return Task.CompletedTask;
        }
    }

    public class RemoveAccountConsumer : IConsumer<IRemoveAccount>
    {
        //the implementation of this method get executed everytime an event of type IUpdateAccount has published or command of type "IUpdateAccount" has recieved
        public Task Consume(ConsumeContext<IRemoveAccount> context)
        {
            Console.WriteLine($"Message Recieved In RemoveAccountConsumer: {context.Message.AccountNumber}");
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

                //creates a queue and an exchange named "account-service" that is bound to MassTransit.Sample.Contracts:UpdateAccount exchange; becuase we want to. => r.Consumer<UpdateAccountConsumer>();
                //this will only Consume messages of type IUpdateAccount becuase "AccountConsumer" is of type "IUpdateAccount"
                //bindings: MassTransit.Sample.Contracts:UpdateAccount exchange => account-service exchange => account-service queue
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
                    //becuase "UpdateAccountConsumer" class is of type "IConsumer<IUpdateAccount>"
                    r.Consumer<UpdateAccountConsumer>();
                    r.Consumer<RemoveAccountConsumer>();
                });

                //bindings: MassTransit.Sample.Contracts:UpdateAccount exchange => another-account-service exchange => another-account-service queue
                cfg.ReceiveEndpoint("another-account-service", r =>
                {

                    //setting ConfigureConsumeTopology to false will tell the rabbitmq not to bind its equivalent exchange to MassTransit.Sample.Contracts:UpdateAccount exchange
                    //(normally it binds the another-account-service exchange to MassTransit.Sample.Contracts:UpdateAccount exchange meaning that every message that has published 
                    //on it wil be sent to another-account-service queue too.)
                    //so with this we must explicitly mention where we want our message to go or it will only receive command messages not an events
                    r.ConfigureConsumeTopology = false;

                    //if ConfigureConsumeTopology has set to false the bind setting can help us with binding the exchanges
                    //for example we have bound another-account-service exchange and the queue as well to MassTransit.Sample.Contracts:UpdateAccount exchange explicitly 
                    r.Bind<IUpdateAccount>();

                    r.PrefetchCount = 20;
                    r.Consumer<AnotherUpdateAccountConsumer>();
                });

                //bindings: some-command-on-user exchange => unused-account-service exchange => unused-account-service queue
                cfg.ReceiveEndpoint("unused-account-service", r =>
                {
                    r.ConfigureConsumeTopology = false;

                    //if ConfigureConsumeTopology has set to false the bind setting can help us with binding the exchanges
                    //for example we have bound unused-account-service exchange and the queue as well to some-command-on-user exchange explicitly 
                    r.Bind("some-command-on-account");

                    r.PrefetchCount = 20;
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


                //======================================================================================
                //                                  send a command
                //======================================================================================
                //in order to send a command we must have an endpoint to send the message to.
                //it will be recieved only by UpdateAccountConsumer and AnotherUpdateAccountConsumer will not ever know that this command has been sent.
                var endpoint = await busControl.GetSendEndpoint(new Uri("exchange:account-service"));
                //The Send method by itself does not know where to send the message so we have specify the endpoint explicitly in order to send the command to only that endpoint.
                await endpoint.Send<IUpdateAccount>(new
                {
                    AccountNumber = "Sent:12314424"
                });

                //another command on account-service endpoint
                await endpoint.Send<IRemoveAccount>(new
                {
                    AccountNumber = "Sent to delete:1233456"

                });

                //there is no consumer with type ICreateAccount on account-service RecieveEndpoint 
                //so it will be led to an exchange and as well a queue named account-service_skipped
                //because it has no consumer the messages will be skipped and will be of kind dead-letter
                await endpoint.Send<ICreateAccount>(new
                {
                    AccountNumber = "Sent to create:1233456"

                });
                //======================================================================================



                //======================================================================================
                //                                  publish an event
                //======================================================================================
                //In contrast when we publish an event, the Publish method knows where to send the event based on the type of the event
                //Below we have set the type of the event to UpdateAccount so it basically know it needs to publish on MassTransit.Sample.Contracts:UpdateAccount Exchange
                //and this event will publish on every queue that has bound to this exchange
                //will be recieved by both UpdateAccountConsumer and AnotherUpdateAccountConsumer
                await busControl.Publish<IUpdateAccount>(new
                {
                    AccountNumber = "Published:54643464"
                });

                //======================================================================================

            }
            finally
            {
                await busControl.StopAsync(CancellationToken.None);
            }
        }
    }
}
