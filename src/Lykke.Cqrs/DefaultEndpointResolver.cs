using System;
using Inceptum.Cqrs.Routing;
using Inceptum.Messaging.Configuration;
using Inceptum.Messaging.Contract;

namespace Lykke.Cqrs
{
    public class DefaultEndpointResolver : IEndpointResolver
    {
        public Endpoint Resolve(string route, RoutingKey key, IEndpointProvider endpointProvider)
        {
            if (endpointProvider.Contains(route))
            {
                return endpointProvider.Get(route);
            }
            throw new ApplicationException(string.Format("Endpoint '{0}' not found",route));
        }
    }
}