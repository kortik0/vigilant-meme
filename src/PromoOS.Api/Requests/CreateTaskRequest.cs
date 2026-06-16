using PromoOS.Api.Domain;

namespace PromoOS.Api.Requests;

public record CreateTaskRequest(
    string Title,
    Priority Priority = Priority.Medium
);