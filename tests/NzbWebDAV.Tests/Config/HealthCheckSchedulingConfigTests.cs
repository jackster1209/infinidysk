using System.Collections;
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

    [Fact]
    public void HeadlessOverlay_MapsHealthCheckWorkers()
    {
        var config = new ConfigManager();
        config.ApplyEnvironmentOverlay(ConfigEnvironmentOverlay.LoadFromEnvironment(new Hashtable
        {
            ["NZBDAV_CONFIG__REPAIR__HEALTHCHECK_WORKERS"] = "3",
        }));

        Assert.Equal(3, config.GetHealthCheckWorkers());
        Assert.True(config.IsEnvironmentManaged(ConfigKeys.RepairHealthcheckWorkers));
        Assert.Equal(
            "NZBDAV_CONFIG__REPAIR__HEALTHCHECK_WORKERS",
            config.GetEnvironmentVariableName(ConfigKeys.RepairHealthcheckWorkers));
    }

    [Theory]
    [InlineData(ConfigKeys.RepairHealthcheckWorkers, "0")]
    [InlineData(ConfigKeys.RepairHealthcheckWorkers, "9")]
    public void ValidateConfigItems_RejectsSchedulingValuesOutsideRange(string key, string value)
    {
        Assert.Throws<ArgumentException>(() => ConfigManager.ValidateConfigItems([
            new ConfigItem { ConfigName = key, ConfigValue = value },
        ]));
    }

    [Theory]
    [InlineData("0", 1)]
    [InlineData("300", 10)]
    [InlineData("9223372036854775807", 10)]
    [InlineData("-50", 1)]
    public void GetHealthCheckCeiling_AcceptsLegacyValuesAndClampsToPool(
        string configured,
        int expected)
    {
        var config = CreateProviderConfig(MakeProvider(ProviderType.Pooled, 8));
        ConfigManager.ValidateConfigItems([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = configured,
            },
        ]);
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = configured,
            },
        ]);

        Assert.Equal(expected, config.GetHealthCheckCeiling());
    }

    [Theory]
    [InlineData("")]
    [InlineData("auto")]
    [InlineData("Auto")]
    [InlineData("AUTO")]
    [InlineData(" auto ")]
    public void ValidateConfigItems_AcceptsAutoHealthCheckCeiling(string value)
    {
        // Validation has to agree with the getter, or Auto could never be saved.
        ConfigManager.ValidateConfigItems([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = value,
            },
        ]);
    }

    [Theory]
    [InlineData("50")]
    [InlineData("200")]
    [InlineData("0")]
    [InlineData("-50")]
    public void ValidateConfigItems_StillAcceptsNumericHealthCheckCeilings(string value)
    {
        ConfigManager.ValidateConfigItems([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = value,
            },
        ]);
    }

    [Fact]
    public void ValidateConfigItems_StillRejectsNonNumericNonAutoCeilings()
    {
        Assert.Throws<ArgumentException>(() => ConfigManager.ValidateConfigItems([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = "automatic",
            },
        ]));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("auto")]
    [InlineData("Auto")]
    [InlineData("AUTO")]
    [InlineData("not-a-number")]
    public void GetHealthCheckCeiling_TreatsAbsentAndAutoAsNoAggregateCeiling(string? configured)
    {
        var config = CreateProviderConfig(MakeProvider(ProviderType.Pooled, 8));
        if (configured is not null)
        {
            config.UpdateValues([
                new ConfigItem
                {
                    ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                    ConfigValue = configured,
                },
            ]);
        }

        // Auto means provider admission is authoritative; there is no aggregate ceiling.
        Assert.Null(config.GetHealthCheckCeiling());
    }

    [Fact]
    public void GetHealthCheckCeiling_PreservesStoredNumericValueAsExplicitOverride()
    {
        // Migration: an upgrade must not silently reinterpret a user's numeric limit as Auto.
        var config = CreateProviderConfig(MakeProvider(ProviderType.Pooled, 8));
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = "6",
            },
        ]);

        Assert.Equal(6, config.GetHealthCheckCeiling());
    }

    [Fact]
    public void HeadlessOverlay_AcceptsLegacyHealthCheckConcurrencyValue()
    {
        var config = CreateProviderConfig(MakeProvider(ProviderType.Pooled, 8));
        config.ApplyEnvironmentOverlay(ConfigEnvironmentOverlay.LoadFromEnvironment(new Hashtable
        {
            ["NZBDAV_CONFIG__REPAIR__HEALTHCHECK_CONCURRENCY"] = "300",
        }));

        Assert.Equal(10, config.GetHealthCheckCeiling());
        Assert.True(config.IsEnvironmentManaged(ConfigKeys.RepairHealthcheckConcurrency));
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
