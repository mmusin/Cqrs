using Inceptum.Cqrs.Routing;
using Inceptum.Messaging.Configuration;
using Inceptum.Messaging.Contract;

namespace Lykke.Cqrs
{
    public interface IEndpointResolver
    {
        Endpoint Resolve(string route, RoutingKey key, IEndpointProvider endpointProvider);
    }
}