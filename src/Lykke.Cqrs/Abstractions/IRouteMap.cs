using System.Collections.Generic;
using Inceptum.Cqrs.Routing;

namespace Lykke.Cqrs
{
    public interface IRouteMap : IEnumerable<Route>
    {
        Route this[string name] { get; }
    }
}