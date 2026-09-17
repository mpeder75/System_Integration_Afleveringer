using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";

// Dead Letter Exchange
const string dlxName = "tours_dlx";
// dead-letter-kø som AdminApp læser
const string deadLetterQueue = "tours_dead_letter";

ConnectionSettings settings = ConnectionSettingsBuilder.Create()
    .Uri(new Uri(brokerUri))
    .ContainerId("tour-AdminApp-consumer")
    .Build();

IEnvironment environment = AmqpEnvironment.Create(settings);
IConnection connection = await environment.CreateConnectionAsync();

try
{
    IManagement management = connection.Management();

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
    
    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(deadLetterQueue)
        .MessageHandler((ctx, message) =>
        {
            string body = message.BodyAsString();

            Console.WriteLine($" [AdminApp] Received '{body}'");
            ctx.Accept();
            return Task.CompletedTask;
        })
        .BuildAndStartAsync();

    try
    {
        Console.WriteLine(" [AdminApp] Waiting for messages. To exit press CTRL+C");
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

