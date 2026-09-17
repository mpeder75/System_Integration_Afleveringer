using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

// AMQP-URI: amqp://bruger:kodeord@host:port/vhost -> guest/guest er RabbitMQs standardbruger, som kun må logge ind fra localhost. /%2f" er den URL-encodede default vhost
// exchangeName = "tours_topic" er navnet på det som publisher sender til.
const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
const string exchangeName = "tours_topic";

// Binding key for denne consumer: "tour.booked" er et eksakt mønster og matcher kun routing key "tour.booked".
// Derfor fanger Email Service kun bookings — ikke cancellations.
const string bindingKey = "tour.booked";

// Builder-pattern: hvert kald returnerer builderen selv, .Build() laver et immutabelt ConnectionSettings-objekt.
// ContainerId er et AMQP koncept, der identificerer denne klient i broker management-UI - så den hedder "tour-emailservice-consumer"
ConnectionSettings settings = ConnectionSettingsBuilder.Create()
    .Uri(new Uri(brokerUri))
    .ContainerId("tour-emailservice-consumer")
    .Build();

// IEnvironment er indgangspunktet i RabbitMQ.AMQP.Client-biblioteket
// CreateConnectionAsync() laver den faktiske TCP + AMQP handshake mod broker
IEnvironment environment = AmqpEnvironment.Create(settings);
IConnection connection = await environment.CreateConnectionAsync();

try
{
    IManagement management = connection.Management();

    // opretter exchange til at være af typen "topic" med navnet "tours_topic"
    IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("topic");
    await exchangeSpec.DeclareAsync();

    // Opretter midlertidig queue
    // Exclusive(true) = kun denne forbindelse må bruge den, AutoDelete(true) = queue fjernes når forbindelsen lukkes.
    IQueueSpecification tempQueue = management.Queue().Exclusive(true).AutoDelete(true);

    // Broker opretter Queue og her hentes navnet på queue og gemmes i queueName
    IQueueInfo queueInfo = await tempQueue.DeclareAsync();
    string queueName = queueInfo.Name();

    // Binder queue til exchange med den fast routingKey(rolle) "tour.booked" (kun bookings).
    // Vigtigt: koden her ved intet om * og # - den sender bare den rå streng videre.
    // Al mønster-matching sker inde i broker fordi exchange-typen er "topic".
    // Var typen "direct", ville broker'en kræve præcist match i stedet for at forstå * og #.
    IBindingSpecification binding = management.Binding()
        .SourceExchange(exchangeSpec)
        .DestinationQueue(queueName)
        .Key(bindingKey);
    await binding.BindAsync();

    // .Queue(queueName) FORTÆLLES TIL BROKER: "lever beskeder fra denne queue til mig."
    // .MessageHandler(...) er en funktion, VI definerer. Broker ved intet om denne kode den kører kun lokalt i vores egen app, EFTER beskeden allerede er modtaget.
    // "message" er ikke noget vi sender - det er den modtagne besked, leveret TIL os som parameter,
    // "ctx" er håndtaget vi bruger til at svare broker tilbage (ctx.Accept() og broker vil fjerne besked
    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(queueName)
        .MessageHandler((ctx, message) =>
        {
            string body = message.BodyAsString();
            string routingKey = RoutingKey(message);
            Console.WriteLine($" [EmailService] Received '{routingKey}':'{body}'");
            ctx.Accept();
            return Task.CompletedTask;
        })
        .BuildAndStartAsync();

    try
    {
        Console.WriteLine(" [EmailService] Waiting for messages. To exit press CTRL+C");
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        await Task.Delay(Timeout.Infinite, cts.Token);
    }
    catch (OperationCanceledException)
    {
    }
    finally
    {
        await consumer.CloseAsync();
    }
}
finally
{
    await connection.CloseAsync();
    await environment.CloseAsync();
}

static string RoutingKey(IMessage message)
{
    object? rk = message.Annotation("x-routing-key");
    if (rk != null)
    {
        return rk.ToString() ?? "";
    }
    return message.Subject() ?? "";
}