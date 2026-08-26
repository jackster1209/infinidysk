using NzbWebDAV.Clients.Usenet.Connections;

namespace NzbWebDAV.Tests.Clients.Usenet;

public class ConnectionLockTests
{
    [Fact]
    public void SecondDisposeCallbackAttachment_ReturnsBorrowedConnectionBeforeThrowing()
    {
        var returned = 0;
        var destroyed = 0;
        var firstCallbackInvocations = 0;
        using var connectionLock = new ConnectionLock<object>(
            new object(),
            _ => returned++,
            _ => destroyed++,
            wasReused: false);
        connectionLock.AttachDisposeCallback(() => firstCallbackInvocations++);

        Assert.Throws<InvalidOperationException>(
            () => connectionLock.AttachDisposeCallback(() => { }));

        Assert.Equal(1, returned);
        Assert.Equal(0, destroyed);
        Assert.Equal(1, firstCallbackInvocations);
        Assert.Throws<ObjectDisposedException>(() => _ = connectionLock.Connection);
    }
}
