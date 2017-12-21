using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using System.Text;
using Common.Log;
using Inceptum.Cqrs;
using Inceptum.Cqrs.Configuration;
using Inceptum.Messaging.Configuration;
using Inceptum.Messaging.Contract;

namespace Lykke.Cqrs
{
    public interface IDependencyResolver
    {
        object GetService(Type type);
        bool HasService(Type type);
    }

    internal class DefaultDependencyResolver : IDependencyResolver
    {
        public object GetService(Type type)
        {
            return Activator.CreateInstance(type);
        }

        public bool HasService(Type type)
        {
            return !type.IsInterface;
        }
    }


    public class DefaultEndpointProvider : IEndpointProvider
    {
        public bool Contains(string endpointName)
        {
            return false;
        }

        public Endpoint Get(string endpointName)
        {
            throw new ApplicationException(string.Format("Endpoint '{0}' not found",endpointName));
        }
    }

    public class CqrsEngine :ICqrsEngine,IDisposable
    {
        public ILog Log { get; }
        private readonly IMessagingEngine m_MessagingEngine;
        private readonly CompositeDisposable m_Subscription=new CompositeDisposable();
        internal  IEndpointResolver EndpointResolver { get; set; }
     
        private readonly IEndpointProvider m_EndpointProvider;
        private readonly List<Context> m_Contexts;
        internal List<Context> Contexts
        {
            get { return m_Contexts; }
        }
        public RouteMap DefaultRouteMap { get; private set; }

        private readonly IRegistration[] m_Registrations;
        private readonly IDependencyResolver m_DependencyResolver;
        private readonly bool m_CreateMissingEndpoints;

        protected IMessagingEngine MessagingEngine
        {
            get { return m_MessagingEngine; }
        }


        public CqrsEngine(ILog log, IMessagingEngine messagingEngine,  IEndpointProvider endpointProvider, params IRegistration[] registrations)
            : this(log, new DefaultDependencyResolver(), messagingEngine, endpointProvider, registrations)
        {
        }


        public CqrsEngine(ILog log, IMessagingEngine messagingEngine, params IRegistration[] registrations)
            : this(log, new DefaultDependencyResolver(), messagingEngine,   new DefaultEndpointProvider(), registrations)
        {
        }

        public CqrsEngine(ILog log, IDependencyResolver dependencyResolver, IMessagingEngine messagingEngine, IEndpointProvider endpointProvider, params IRegistration[] registrations)
            : this(log, dependencyResolver, messagingEngine, endpointProvider, false, registrations)
        {
        }

        public CqrsEngine(ILog log, IDependencyResolver dependencyResolver, IMessagingEngine messagingEngine, IEndpointProvider endpointProvider, bool createMissingEndpoints, params IRegistration[] registrations)
        {
            Log = log;
            m_CreateMissingEndpoints = createMissingEndpoints;
            m_DependencyResolver = dependencyResolver;
            m_Registrations = registrations;
            EndpointResolver = new DefaultEndpointResolver();
            m_MessagingEngine = messagingEngine;
            m_EndpointProvider = endpointProvider;
            m_Contexts=new List<Context>();
            DefaultRouteMap = new RouteMap("default");
            init();
            
        }


        private void init()
        {
            foreach (var registration in m_Registrations)
            {
                registration.Create(this);
            }
            foreach (var registration in m_Registrations)
            {
                registration.Process(this);
            }

            ensureEndpoints();

            foreach (var boundedContext in Contexts)
            {
                foreach (var route in boundedContext.Routes)
                {
                            
                    Context context = boundedContext;
                    var subscriptions = route.MessageRoutes
                        .Where(r => r.Key.CommunicationType == CommunicationType.Subscribe)
                        .Select(r => new
                        {
                            type = r.Key.MessageType,
                            priority = r.Key.Priority,
                            remoteBoundedContext=r.Key.RemoteBoundedContext,
                            endpoint = new Endpoint(r.Value.TransportId, "", r.Value.Destination.Subscribe, r.Value.SharedDestination, r.Value.SerializationFormat)
                        })
                        .GroupBy(x => Tuple.Create(x.endpoint, x.priority,x.remoteBoundedContext))
                        .Select(g => new
                        {
                            endpoint = g.Key.Item1,
                            priority = g.Key.Item2,
                            remoteBoundedContext =g.Key.Item3,
                            types = g.Select(x=>x.type).ToArray()
                        });


                    foreach (var subscription in subscriptions)
                    {
                        var processingGroup = route.ProcessingGroupName;
                        var routeName = route.Name;
                        var endpoint = subscription.endpoint;
                        var remoteBoundedContext = subscription.remoteBoundedContext;
                        CallbackDelegate<object> callback=null;
                        string messageTypeName=null;
                        switch (route.Type)
                        {
                            case RouteType.Events:
                                callback = (@event, acknowledge,headers) => context.EventDispatcher.Dispatch(remoteBoundedContext, new[] {Tuple.Create(@event, acknowledge)});
                                messageTypeName = "event";
                                break;
                            case RouteType.Commands:
                                callback =
                                    (command, acknowledge, headers) =>
                                        context.CommandDispatcher.Dispatch(command, acknowledge, endpoint, routeName);
                                messageTypeName = "command";
                                break;
                        }
                        
                        m_Subscription.Add(m_MessagingEngine.Subscribe(
                            endpoint,
                            callback,
                            (type, acknowledge) =>
                            {
                                throw new InvalidOperationException(string.Format("Unknown {0} received: {1}",messageTypeName, type));
                                //acknowledge(0, true);
                            },
                            processingGroup,
                            (int)subscription.priority,
                            subscription.types));
                    }
                }
            }

            foreach (var boundedContext in Contexts)
            {
                boundedContext.Processes.ForEach(p => p.Start(boundedContext, boundedContext.EventsPublisher));
            }            
        }

        private void ensureEndpoints()
        {
            var allEndpointsAreValid = true;
            var errorMessage=new StringBuilder("Some endpoints are not valid:").AppendLine();
            var log = new StringBuilder();
            log.Append("Endpoints verification").AppendLine();

            foreach (var context in new []{DefaultRouteMap}.Concat(Contexts))
            {
                foreach (var route in context)
                {
                    m_MessagingEngine.AddProcessingGroup(route.ProcessingGroupName,route.ProcessingGroup);
                }
            }


            foreach (var routeMap in (new[] { DefaultRouteMap }).Concat(Contexts))
            {
                log.AppendFormat("Context '{0}':",routeMap.Name).AppendLine();

                routeMap.ResolveRoutes(m_EndpointProvider);
                foreach (var route in routeMap)
                {
                    log.AppendFormat("\t{0} route '{1}':",route.Type, route.Name).AppendLine();
                    foreach (var messageRoute in route.MessageRoutes)
                    {
                        string error;
                        var routingKey = messageRoute.Key;
                        var endpoint = messageRoute.Value;
                        var messageTypeName = route.Type.ToString().ToLower().TrimEnd('s');
                        bool result = true;

                        if (routingKey.CommunicationType == CommunicationType.Publish)
                        {
                            if (!m_MessagingEngine.VerifyEndpoint(endpoint, EndpointUsage.Publish, m_CreateMissingEndpoints, out error))
                            {
                                errorMessage.AppendFormat(
                                    "Route '{1}' within bounded context '{0}' for {2} type '{3}' has resolved endpoint {4} that is not properly configured for publishing: {5}.",
                                    routeMap.Name, route.Name, messageTypeName, routingKey.MessageType.Name, endpoint, error).AppendLine();
                                result = false;
                            }

                            log.AppendFormat("\t\tPublishing  '{0}' to {1}\t{2}", routingKey.MessageType.Name, endpoint, result ? "OK" : "ERROR:" + error).AppendLine();
                        }

                        if (routingKey.CommunicationType == CommunicationType.Subscribe)
                        {
                            if (!m_MessagingEngine.VerifyEndpoint(endpoint, EndpointUsage.Subscribe, m_CreateMissingEndpoints, out error))
                            {
                                errorMessage.AppendFormat(
                                    "Route '{1}' within bounded context '{0}' for {2} type '{3}' has resolved endpoint {4} that is not properly configured for subscription: {5}.",
                                    routeMap.Name, route.Name, messageTypeName, routingKey.MessageType.Name, endpoint, error).AppendLine();
                                result = false;
                            }

                            log.AppendFormat("\t\tSubscribing '{0}' on {1}\t{2}", routingKey.MessageType.Name, endpoint, result ? "OK" : "ERROR:" + error).AppendLine();
                        }
                        allEndpointsAreValid = allEndpointsAreValid && result;
                    }
                    
                }
            }

            if (!allEndpointsAreValid)
                throw new ApplicationException(errorMessage.ToString());            
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Contexts.Where(b => b != null).Select(c=>c.Processes).Where(p=>p!=null).SelectMany(p=>p).ToList().ForEach(process => process.Dispose());
                Contexts.Where(b => b != null).ToList().ForEach(context => context.Dispose());

                if (m_Subscription != null)
                    m_Subscription.Dispose();
            }
        }

        public void SendCommand<T>(T command, string boundedContext, string remoteBoundedContext, uint priority = 0)
        {
            if (!sendMessage(typeof (T), command, RouteType.Commands, boundedContext, priority, remoteBoundedContext))
            {
                if(boundedContext!=null)
                    throw new InvalidOperationException(string.Format("bound context '{0}' does not support command '{1}' with priority {2}", boundedContext, typeof(T), priority));
                throw new InvalidOperationException(string.Format("Default route map does not contain rout for command '{0}' with priority {1}", typeof(T), priority));
            }
        }

        private bool sendMessage(Type type, object message, RouteType routeType, string context, uint priority, string remoteBoundedContext=null)
        {
            RouteMap routeMap = DefaultRouteMap;
            if (context != null)
            {
                routeMap = Contexts.FirstOrDefault(bc => bc.Name == context);
                if (routeMap == null)
                {
                    throw new ArgumentException(string.Format("bound context {0} not found", context), "context");
                }
            }
            var published = routeMap.PublishMessage(m_MessagingEngine, type, message, routeType, priority, remoteBoundedContext);
            if (!published && routeType == RouteType.Commands)
            {
                published = DefaultRouteMap.PublishMessage(m_MessagingEngine,type,message, routeType, priority, remoteBoundedContext);
            }
            return published;
        }        

        internal void PublishEvent(object @event, string boundedContext)
        {
            if (@event == null) throw new ArgumentNullException("event");
            if (!sendMessage(@event.GetType(), @event, RouteType.Events, boundedContext, 0))
                throw new InvalidOperationException(string.Format("bound context '{0}' does not support event '{1}'", boundedContext, @event.GetType()));
        }

        internal void PublishEvent(object @event, Endpoint endpoint, string processingGroup, Dictionary<string, string> headers = null)
        {
            m_MessagingEngine.Send(@event, endpoint, processingGroup,headers);
        }

        internal IDependencyResolver DependencyResolver {
            get { return m_DependencyResolver; }
        }        
    }

    public interface ICqrsEngine
    {   
        void SendCommand<T>(T command, string boundedContext, string remoteBoundedContext, uint priority = 0);
    }
}