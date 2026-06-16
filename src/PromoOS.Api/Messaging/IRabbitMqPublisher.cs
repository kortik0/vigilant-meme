namespace PromoOS.Api.Messaging;

public interface IRabbitMqPublisher
{
    void PublishTaskCompleted(TaskCompletedMessage message);
}