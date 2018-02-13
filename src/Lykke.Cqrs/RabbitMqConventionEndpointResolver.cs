﻿using System;
using System.Collections.Generic;
using Inceptum.Messaging.Configuration;
using Inceptum.Messaging.Contract;
using Lykke.Cqrs.Routing;

namespace Lykke.Cqrs
{
    public class RabbitMqConventionEndpointResolver : IEndpointResolver
    {
        private readonly Dictionary<Tuple<string, RoutingKey>, Endpoint> m_Cache = new Dictionary<Tuple<string, RoutingKey>, Endpoint>();
        private readonly string m_Transport;
        private readonly string m_SerializationFormat;
        private readonly string m_ExclusiveQueuePostfix;
        private readonly string m_EnvironmentPrefix;
        private readonly string m_CommandsKeyword;
        private readonly string m_EventsKeyword;

        public RabbitMqConventionEndpointResolver(
            string transport,
            string serializationFormat,
            string exclusiveQueuePostfix = null,
            string environment = null,
            string commandsKeyword = null,
            string eventsKeyword = null)
        {
            m_EnvironmentPrefix = environment != null ? environment + "." : "";
            m_ExclusiveQueuePostfix = "." + (exclusiveQueuePostfix ?? Environment.MachineName);
            m_Transport = transport;
            m_SerializationFormat = serializationFormat;
            m_CommandsKeyword = commandsKeyword;
            m_EventsKeyword = eventsKeyword;
        }

        private string CreateQueueName(string queue,bool exclusive)
        {
            return string.Format("{0}{1}{2}", m_EnvironmentPrefix, queue, exclusive ? m_ExclusiveQueuePostfix : "");
        }

        private string CreateExchangeName(string exchange)
        {
            return string.Format("topic://{0}{1}", m_EnvironmentPrefix, exchange);
        }

        private Endpoint CreateEndpoint(string route, RoutingKey key)
        {
            var rmqRoutingKey = key.Priority == 0 ? key.MessageType.Name : key.MessageType.Name + "." + key.Priority;
            var queueName = key.Priority == 0 ? route : route + "." + key.Priority;
            if (key.RouteType == RouteType.Commands && key.CommunicationType == CommunicationType.Subscribe)
            {
                return new Endpoint
                {
                    Destination = new Destination
                    {
                        Publish = CreateExchangeName(string.Format(
                            "{0}.{1}.exchange/{2}",
                            key.LocalContext,
                            GetKewordByRoutType(key.RouteType),
                            rmqRoutingKey)),
                        Subscribe = CreateQueueName(
                            string.Format(
                                "{0}.queue.{1}.{2}",
                                key.LocalContext,
                                GetKewordByRoutType(key.RouteType),
                                queueName),
                            key.Exclusive)
                    },
                    SerializationFormat = m_SerializationFormat,
                    SharedDestination = true,
                    TransportId = m_Transport
                };
            }

            if (key.RouteType == RouteType.Commands && key.CommunicationType == CommunicationType.Publish)
            {
                return new Endpoint
                {
                    Destination = new Destination
                    {
                        Publish = CreateExchangeName(string.Format(
                            "{0}.{1}.exchange/{2}",
                            key.RemoteBoundedContext,
                            GetKewordByRoutType(key.RouteType),
                            rmqRoutingKey)),
                        Subscribe = null
                    },
                    SerializationFormat = m_SerializationFormat,
                    SharedDestination = true,
                    TransportId = m_Transport
                };
            }

            if (key.RouteType == RouteType.Events && key.CommunicationType == CommunicationType.Subscribe)
            {
                return new Endpoint
                {
                    Destination = new Destination
                    {
                        Publish = CreateExchangeName(string.Format(
                            "{0}.{1}.exchange/{2}",
                            key.RemoteBoundedContext,
                            GetKewordByRoutType(key.RouteType),
                            key.MessageType.Name)),
                        Subscribe = CreateQueueName(
                            string.Format(
                                "{0}.queue.{1}.{2}.{3}",
                                key.LocalContext,
                                key.RemoteBoundedContext,
                                GetKewordByRoutType(key.RouteType), route),
                            key.Exclusive)
                    },
                    SerializationFormat = m_SerializationFormat,
                    SharedDestination = true,
                    TransportId = m_Transport
                };
            }

            if (key.RouteType == RouteType.Events && key.CommunicationType == CommunicationType.Publish)
            {
                return new Endpoint
                {
                    Destination = new Destination
                    {
                        Publish = CreateExchangeName(string.Format(
                            "{0}.{1}.exchange/{2}",
                            key.LocalContext,
                            GetKewordByRoutType(key.RouteType),
                            key.MessageType.Name)),
                        Subscribe = null
                    },
                    SerializationFormat = m_SerializationFormat,
                    SharedDestination = true,
                    TransportId = m_Transport
                };
            }
            return default(Endpoint);
        }

        private string GetKewordByRoutType(RouteType routeType)
        {
            string keyword = null;
            switch (routeType)
            {
                case RouteType.Commands:
                    keyword=m_CommandsKeyword;
                    break;
                case RouteType.Events:
                    keyword=m_EventsKeyword;
                break;
            }
            return keyword??(routeType.ToString().ToLower());
        }

        public Endpoint Resolve(string route, RoutingKey key, IEndpointProvider endpointProvider)
        {
            lock (m_Cache)
            {
                Endpoint ep;
                if (m_Cache.TryGetValue(Tuple.Create(route,key), out ep))
                    return ep;

                if (endpointProvider.Contains(route))
                {
                    ep = endpointProvider.Get(route);
                    m_Cache.Add(Tuple.Create(route, key), ep);
                    return ep;
                }

                ep = CreateEndpoint(route,key);
                m_Cache.Add(Tuple.Create(route, key), ep);
                return ep;
            }
        }
    }
}