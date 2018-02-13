using Inceptum.Messaging.Contract;

namespace Inceptum.Cqrs.InfrastructureCommands
{
    class RoutedCommand<T>
    {
        public RoutedCommand(T command, Endpoint originEndpoint, string originRoute)
        {
            Command = command;
            OriginEndpoint = originEndpoint;
            OriginRoute = originRoute;
        }

        public T Command { get; set; }
        public Endpoint OriginEndpoint { get; set; }
        public string OriginRoute { get; set; }
    }
}