namespace PromoOS.Api.Messaging;

public record TaskCompletedMessage(
    Guid TaskId,
    string Title,
    DateTimeOffset CompletedAt,
    string Priority
);