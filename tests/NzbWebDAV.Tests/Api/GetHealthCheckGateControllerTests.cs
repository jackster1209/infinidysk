using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NzbWebDAV.Api.Controllers.GetHealthCheckGate;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Models;
using NzbWebDAV.Services;

namespace NzbWebDAV.Tests.Api;

public sealed class GetHealthCheckGateControllerTests
{
    [Fact]
    public async Task Get_ReturnsCurrentAndRecentBackgroundPressure()
    {
        var config = CreateConfig(limit: 2);
        using var gate = new HealthCheckConnectionGate(config);
        using var first = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        using var second = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        var waiting = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);

        first.Dispose();
        using var third = await waiting.WaitAsync(TimeSpan.FromSeconds(1));

        var controller = new TestController(gate)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext(),
            },
        };
        var result = await controller.InvokeAsync();
        var response = Assert.IsType<OkObjectResult>(result).Value as GetHealthCheckGateResponse
            ?? throw new Xunit.Sdk.XunitException("Expected health check gate response.");

        Assert.Equal(2, response.Limit);
        Assert.Equal(2, response.Active);
        Assert.Equal(2, response.PeakActive);
        Assert.Equal(0, response.WaitingBackground);
        Assert.Equal(1, response.PeakWaitingBackground);
    }

    private static ConfigManager CreateConfig(int limit)
    {
        var config = new ConfigManager();
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.UsenetProviders,
                ConfigValue = JsonSerializer.Serialize(new UsenetProviderConfig
                {
                    Providers =
                    [
                        new UsenetProviderConfig.ConnectionDetails
                        {
                            ProviderId = Guid.NewGuid(),
                            Type = ProviderType.Pooled,
                            Host = "gate.example",
                            Port = 563,
                            UseSsl = true,
                            User = "user",
                            Pass = "pass",
                            MaxConnections = 20,
                        },
                    ],
                }),
            },
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = limit.ToString(),
            },
        ]);
        return config;
    }

    private sealed class TestController(HealthCheckConnectionGate gate)
        : GetHealthCheckGateController(gate)
    {
        protected override bool RequiresAuthentication => false;

        public Task<IActionResult> InvokeAsync() => HandleApiRequest();
    }
}
