using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using PromoOS.Api.Data;
using PromoOS.Api.Messaging;
using Xunit;

namespace PromoOS.Tests;

public class FakeRabbitMqPublisher : IRabbitMqPublisher
{
    public ConcurrentBag<TaskCompletedMessage> PublishedMessages { get; } = new();

    public void PublishTaskCompleted(TaskCompletedMessage message)
        => PublishedMessages.Add(message);
}

public class TestWebApplicationFactory : WebApplicationFactory<Program>
{
    private readonly FakeRabbitMqPublisher _fakePublisher = new();

    public FakeRabbitMqPublisher FakePublisher => _fakePublisher;

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Test");

        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IDbContextOptionsConfiguration<AppDbContext>>();
            services.RemoveAll<AppDbContext>();

            var dbName = $"TestDb_{Guid.NewGuid()}";
            services.AddDbContext<AppDbContext>(options =>
                options.UseInMemoryDatabase(dbName));

            services.RemoveAll<IRabbitMqPublisher>();
            services.RemoveAll<RabbitMqPublisher>();
            services.RemoveAll<IHostedService>();

            services.AddSingleton<IRabbitMqPublisher>(_fakePublisher);
        });
    }
}

public class CompleteTaskIntegrationTest : IClassFixture<TestWebApplicationFactory>
{
    private readonly HttpClient _client;
    private readonly FakeRabbitMqPublisher _publisher;

    public CompleteTaskIntegrationTest(TestWebApplicationFactory factory)
    {
        _client = factory.CreateClient();
        _publisher = factory.FakePublisher;
    }

    [Fact]
    public async Task CompleteTask_ShouldPublishMessageToRabbitMq()
    {
        var createResponse = await _client.PostAsJsonAsync("/tasks", new
        {
            title = "Buy milk",
            priority = "High"
        });

        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        var created = await createResponse.Content.ReadFromJsonAsync<TaskDto>();
        Assert.NotNull(created);
        Assert.NotEqual(Guid.Empty, created.Id);

        var completeResponse = await _client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        var completed = await completeResponse.Content.ReadFromJsonAsync<TaskDto>();
        Assert.NotNull(completed);
        Assert.True(completed.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        await Task.Delay(100);

        var message = _publisher.PublishedMessages.SingleOrDefault(m => m.TaskId == created.Id);
        Assert.NotNull(message);
        Assert.Equal("Buy milk", message.Title);
        Assert.Equal("High", message.Priority);
    }

    private record TaskDto(
        Guid Id,
        string Title,
        bool IsCompleted,
        DateTimeOffset CreatedAt,
        DateTimeOffset? CompletedAt,
        string Priority);
}