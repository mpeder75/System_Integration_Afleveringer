using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
const string exchangeName = "tours_topic";

// Dead Letter Exchange
const string dlxName = "tours_dlx";
// dead-letter-kø som AdminApp læser
const string deadLetterQueue = "tours_dead_letter";

// EmailService binder KUN på "tour.booked
const string bindingKey = "tour.booked";

ConnectionSettings settings = ConnectionSettingsBuilder.Create()
    .Uri(new Uri(brokerUri))
    .ContainerId("tour-emailservice-consumer")
    .Build();

IEnvironment environment = AmqpEnvironment.Create(settings);
IConnection connection = await environment.CreateConnectionAsync();

try
{
    IManagement management = connection.Management();

    // topic-exchange (uændret)
    IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("topic");
    await exchangeSpec.DeclareAsync();

    // Dead Letter Exchange + dead-letter-kø
    // fanout: alt der dør ryger til den ene dead-letter-kø, uanset routing key
    IExchangeSpecification dlx = management.Exchange(dlxName).Type("fanout");
    await dlx.DeclareAsync();

    IQueueSpecification dlq = management.Queue().Name(deadLetterQueue);
    await dlq.DeclareAsync();

    await management.Binding()
        .SourceExchange(dlx)
        .DestinationQueue(deadLetterQueue)
        .Key("")
        .BindAsync();

    // Navngivet + quorum (altid durable, beskeder persisteres til disk = Guaranteed Delivery)
    // DeadLetterExchange: afviste/døde beskeder herfra sendes til DLX
    IQueueSpecification queue = management.Queue()
        .Name("emailservice_queue")
        .DeadLetterExchange(dlxName);

    IQueueInfo queueInfo = await queue.DeclareAsync();
    string queueName = queueInfo.Name();

    // Binding til topic-exchange (uændret logik)
    IBindingSpecification binding = management.Binding()
        .SourceExchange(exchangeSpec)
        .DestinationQueue(queueName)
        .Key(bindingKey);
    await binding.BindAsync();

    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(queueName)
        .MessageHandler((ctx, message) =>
        {
            string body = message.BodyAsString();
            string routingKey = RoutingKey(message);

            // invalid tjek: dårlig besked -> discard (dead-letter via køens DLX)
            if (!IsValid(body))
            {
                // Besked sendes til DLX og ctx.Discard() kaldes så beskeden ikke genleveres til BackOffice-køen og fjernes fra Queue
                Console.WriteLine($" [EmailService] INVALID '{routingKey}':'{body}' -> discard");
                ctx.Discard();
                return Task.CompletedTask;
            }

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

static bool IsValid(string body) => !string.IsNullOrEmpty(body) && (body.StartsWith("BOOK:") || body.StartsWith("CANCEL:"));

static string RoutingKey(IMessage message)
{
    object? rk = message.Annotation("x-routing-key");
    if (rk != null)
        return rk.ToString() ?? "";
    return message.Subject() ?? "";
}