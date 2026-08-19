using System.Text.Json;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Models;

namespace NzbWebDAV.Tests.Config;

public sealed class HealthCheckSchedulingConfigTests
{
    [Theory]
    [InlineData(null, 1)]
    [InlineData("2", 2)]
    [InlineData("8", 8)]
    public void GetHealthCheckWorkers_UsesSafeBounds(string? configured, int expected)
    {
        var config = new ConfigManager();
        if (configured is not null)
        {
            config.UpdateValues([
                new ConfigItem
                {
                    ConfigName = ConfigKeys.RepairHealthcheckWorkers,
                    ConfigValue = configured,
                },
            ]);
        }

        Assert.Equal(expected, config.GetHealthCheckWorkers());
    }

    [Theory]
    [InlineData(ProviderType.Pooled)]
    [InlineData(ProviderType.BackupAndStats)]
    [InlineData(ProviderType.BackupOnly)]
    public void CanBackgroundHealthCoexistWithQueue_RequiresEveryEnabledProviderToBeSplit(
        ProviderType secondProviderType)
    {
        var config = CreateProviderConfig(
            MakeProvider(ProviderType.Pooled, 8),
            MakeProvider(secondProviderType, null));

        Assert.False(config.CanBackgroundHealthCoexistWithQueue());
    }

    [Fact]
    public void CanBackgroundHealthCoexistWithQueue_AllowsFullySplitConfiguration()
    {
        var config = CreateProviderConfig(
            MakeProvider(ProviderType.Pooled, 8),
            MakeProvider(ProviderType.BackupOnly, 2),
            MakeProvider(ProviderType.Disabled, null));

        Assert.True(config.CanBackgroundHealthCoexistWithQueue());
    }

    [Fact]
    public void CanBackgroundHealthCoexistWithQueue_RejectsEmptyConfiguration()
    {
        Assert.False(new ConfigManager().CanBackgroundHealthCoexistWithQueue());
    }

    [Theory]
    [InlineData(ConfigKeys.RepairHealthcheckConcurrency, "0")]
    [InlineData(ConfigKeys.RepairHealthcheckConcurrency, "201")]
    [InlineData(ConfigKeys.RepairHealthcheckWorkers, "0")]
    [InlineData(ConfigKeys.RepairHealthcheckWorkers, "9")]
    public void ValidateConfigItems_RejectsSchedulingValuesOutsideRange(string key, string value)
    {
        Assert.Throws<ArgumentException>(() => ConfigManager.ValidateConfigItems([
            new ConfigItem { ConfigName = key, ConfigValue = value },
        ]));
    }

    private static ConfigManager CreateProviderConfig(
        params UsenetProviderConfig.ConnectionDetails[] providers)
    {
        var config = new ConfigManager();
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.UsenetProviders,
                ConfigValue = JsonSerializer.Serialize(new UsenetProviderConfig
                {
                    Providers = providers.ToList(),
                }),
            },
        ]);
        return config;
    }

    private static UsenetProviderConfig.ConnectionDetails MakeProvider(
        ProviderType type,
        int? maxTransferConnections) => new()
    {
        ProviderId = Guid.NewGuid(),
        Type = type,
        Host = $"{Guid.NewGuid():N}.example",
        Port = 563,
        UseSsl = true,
        User = "user",
        Pass = "pass",
        MaxConnections = 10,
        MaxTransferConnections = maxTransferConnections,
    };
}
