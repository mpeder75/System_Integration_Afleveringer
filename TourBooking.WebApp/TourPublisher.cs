using System.Text;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace TourBooking.WebApp;

public sealed class TourPublisher
{
    // AMQP-URI: amqp://bruger:kodeord@host:port/vhost -> guest/guest er RabbitMQs standardbruger, som kun må logge ind fra localhost. /%2f" er den URL-encodede default vhost
    // exchangeName = "tours_topic" er navnet på det som publisher sender til.
    private const string BrokerUri = "amqp://guest:guest@localhost:5672/%2f";
    private const string ExchangeName = "tours_topic";

    public async Task PublishAsync(string routingKey, string message)
    {
        // Builder-pattern: hvert kald returnerer builderen selv, .Build() laver et immutabelt ConnectionSettings-objekt.
        // ContainerId er et AMQP koncept, der identificerer denne klient i broker management-UI - så den hedder "tours_topic"
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(BrokerUri))
            .ContainerId("tour-webapp-publisher")
            .Build();

        // IEnvironment er indgangspunktet i RabbitMQ.AMQP.Client-biblioteket
        // CreateConnectionAsync() laver den faktiske TCP + AMQP 1.0-handshake mod broker
        IEnvironment environment = AmqpEnvironment.Create(settings);
        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            // opretter exchange til at være af typen "topic" med navnet "tours_topic"
            IExchangeSpecification exchangeSpec = management.Exchange(ExchangeName).Type("topic");
            await exchangeSpec.DeclareAsync();

            // opsætter at beskeder publishes til exchange "tours_topic"
            IPublisher publisher = await connection.PublisherBuilder()
                .Exchange(ExchangeName)
                .Key(routingKey)
                .BuildAsync();
            
            try
            {
                // message transformeres til binær med Encoding.UTF8.GetBytes(...). AmqpMessage er blot en wrapper omkring byte-arrayet.
                var amqpMessage = new AmqpMessage(Encoding.UTF8.GetBytes(message));

                // Besked sendes til broker med await publisher.PublishAsync
                // svar gemmes i PublishResult pr variabel
                PublishResult pr = await publisher.PublishAsync(amqpMessage);

                switch (pr.Outcome.State)
                {
                    case OutcomeState.Accepted:
                        break;
                    case OutcomeState.Released:
                        throw new InvalidOperationException(
                            $"Message '{routingKey}' was released — not routed to any queue.");
                    case OutcomeState.Rejected:
                        throw new InvalidOperationException(
                            $"Message '{routingKey}' was rejected by the broker: {pr.Outcome.Error}");
                    default:
                        throw new InvalidOperationException(
                            $"Publish of '{pr.Message.BodyAsString()}','{routingKey}' got an unexpected outcome: {pr.Outcome.Error}");
                }
            }
            finally
            {
                await publisher.CloseAsync();
            }
        }
        finally
        {
            await connection.CloseAsync();
            await environment.CloseAsync();
        }
    }
}