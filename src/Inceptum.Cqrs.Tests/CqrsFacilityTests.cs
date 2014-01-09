﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Castle.Core.Logging;
using Castle.Facilities.Startable;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using Inceptum.Cqrs.Castle;
using Inceptum.Cqrs.Configuration;
using Inceptum.Cqrs.InfrastructureCommands;
using Inceptum.Messaging.Contract;
using NUnit.Framework;
using Rhino.Mocks;

namespace Inceptum.Cqrs.Tests
{

    internal class CommandsHandler
    {
        public readonly List<object> HandledCommands= new List<object>();

        private void Handle(string m)
        {
            Console.WriteLine("Command received:" + m);
            HandledCommands.Add(m);
        } 
        
        private CommandHandlingResult Handle(int m)
        {
            Console.WriteLine("Command received:" + m);
            HandledCommands.Add(m);
            return new CommandHandlingResult{Retry = true,RetryDelay = 100};
        }    

        private CommandHandlingResult Handle(RoutedCommand<DateTime> m)
        {
            Console.WriteLine("Command received:" + m.Command+" Origination Endpoint:"+m.OriginEndpoint);
            HandledCommands.Add(m);
            return new CommandHandlingResult{Retry = true,RetryDelay = 100};
        }    

        private void Handle(long m)
        {
            Console.WriteLine("Command received:" + m);
            throw new Exception();
        }
    }


    internal class EventListener
    {
        public readonly List<Tuple<string, string>> EventsWithBoundedContext= new List<Tuple<string, string>>();
        public readonly List<string> Events= new List<string>();

        void Handle(string m,string boundedContext)
        {
            EventsWithBoundedContext.Add(Tuple.Create(m,boundedContext));
            Console.WriteLine(boundedContext+":"+m);
        } 
        void Handle(string m)
        {
            Events.Add(m);
            Console.WriteLine(m);
        } 
    }
    

    class CqrEngineDependentComponent
    {
        public  static bool Started { get; set; }
        public CqrEngineDependentComponent(ICommandSender engine)
        {
        }
        public void Start()
        {
            Started = true;
        }
    }

    [TestFixture]
    public class CqrsFacilityTests
    {
      

        [Test]
        [ExpectedException(ExpectedMessage = "Component can not be projection and commands handler simultaneousely")]
        public void ComponentCanNotBeProjectionAndCommandsHandlerSimultaneousely()
        {
            using (var container = new WindsorContainer())
            {
                container.AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(LocalBoundedContext.Named("bc")));
                container.Register(Component.For<CommandsHandler>().AsCommandsHandler("bc").AsProjection("bc", "remote"));
            }
        }


        [Test]
        public void CqrsEngineIsResolvableAsDependencyOnlyAfterBootstrapTest()
        {
            bool reslovedCqrsDependentComponentBeforeInit = false;
            using (var container = new WindsorContainer())
            {
                container.Register(Component.For<CqrEngineDependentComponent>());
                container.AddFacility<CqrsFacility>(f => f.RunInMemory());
                try
                {
                    container.Resolve<CqrEngineDependentComponent>();
                    reslovedCqrsDependentComponentBeforeInit = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                container.Resolve<ICqrsEngineBootstrapper>().Start();

                container.Resolve<CqrEngineDependentComponent>();
                Assert.That(reslovedCqrsDependentComponentBeforeInit, Is.False, "ICommandSender was resolved as dependency before it was initialized");
            }
        }

        [Test]
        public void CqrsEngineIsResolvableAsDependencyOnlyAfterBootstrapStartableFacilityTest()
        {
            using (var container = new WindsorContainer())
            {
                container.AddFacility<CqrsFacility>(f => f.RunInMemory())
                    .AddFacility<StartableFacility>(); // (f => f.DeferredTryStart());
                container.Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()));
                container.Register(Component.For<CqrEngineDependentComponent>().StartUsingMethod("Start"));
                Assert.That(CqrEngineDependentComponent.Started, Is.False, "Component was started before commandSender initialization");
                container.Resolve<ICqrsEngineBootstrapper>().Start();
                Assert.That(CqrEngineDependentComponent.Started, Is.True, "Component was not started after commandSender initialization");
            }
        }

        [Test]
        public void ProjectionWiringTest()
        {
            using (var container = new WindsorContainer())
            {
                container.Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()))
                    .AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(LocalBoundedContext.Named("local"), RemoteBoundedContext.Named("remote","local")))
                    .Register(Component.For<EventListener>().AsProjection("local", "remote"))
                    .Resolve<ICqrsEngineBootstrapper>().Start();

                var cqrsEngine = (CqrsEngine)container.Resolve<ICqrsEngine>();
                var eventListener = container.Resolve<EventListener>();
                cqrsEngine.BoundedContexts.First(c => c.Name == "remote").EventDispatcher.Dispacth("test",(delay, acknowledge) => {});
                Assert.That(eventListener.EventsWithBoundedContext, Is.EquivalentTo(new[] {Tuple.Create("test", "remote")}), "Event was not dispatched");
                Assert.That(eventListener.Events, Is.EquivalentTo(new[] {"test"}), "Event was not dispatched");
            }
        }

        [Test]
        public void CommandsHandlerWiringTest()
        {
            using (var container = new WindsorContainer())
            {
                container
                    .Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()))
                    .AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(LocalBoundedContext.Named("bc")))
                    .Register(Component.For<CommandsHandler>().AsCommandsHandler("bc"))
                    .Resolve<ICqrsEngineBootstrapper>().Start();
                var cqrsEngine = (CqrsEngine)container.Resolve<ICqrsEngine>();
                var commandsHandler = container.Resolve<CommandsHandler>();
                cqrsEngine.BoundedContexts.First(c => c.Name == "bc").CommandDispatcher.Dispatch("test", CommandPriority.Low, (delay, acknowledge) => { }, new Endpoint(), "route");
                Thread.Sleep(200);
                Assert.That(commandsHandler.HandledCommands, Is.EqualTo(new[] {"test"}), "Command was not dispatched");
            }
        }
        

        [Test]
        public void DependencyOnICommandSenderTest()
        {
            using (var container = new WindsorContainer())
            {
                var messagingEngine = MockRepository.GenerateMock<IMessagingEngine>();
                container
                    .Register(Component.For<IMessagingEngine>().Instance(messagingEngine))
                    .AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(
                        LocalBoundedContext.Named("bc").ListeningCommands(typeof(string)).On("cmd").RoutedFromSameEndpoint())
                            )
                    .Register(Component.For<EventListenerWithICommandSenderDependency>().AsSaga("bc"))
                    .Register(Component.For<CommandsHandler>().AsCommandsHandler("bc"))
                    .Resolve<ICqrsEngineBootstrapper>().Start();


                var listener = container.Resolve<EventListenerWithICommandSenderDependency>();
                var commandsHandler = container.Resolve<CommandsHandler>();
                Assert.That(listener.Sender,Is.Not.Null);
                listener.Sender.SendCommand("test", "bc");
                Thread.Sleep(200);
                Assert.That(commandsHandler.HandledCommands, Is.EqualTo(new[] { "test" }), "Command was not dispatched");
            }
        }
        
        [Test]
        public void CommandsHandlerWithResultWiringTest()
        {
            using (var container = new WindsorContainer())
            {
                container
                    .Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()))
                    .AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(LocalBoundedContext.Named("bc")))
                    .Register(Component.For<CommandsHandler>().AsCommandsHandler("bc"))
                    .Resolve<ICqrsEngineBootstrapper>().Start();
                var cqrsEngine = (CqrsEngine)container.Resolve<ICqrsEngine>();
                var commandsHandler = container.Resolve<CommandsHandler>();

                bool acknowledged = false;
                long retrydelay = 0;
                cqrsEngine.BoundedContexts.First(c => c.Name == "bc").CommandDispatcher.Dispatch(1,CommandPriority.Low, (delay, acknowledge) =>
                {
                    retrydelay = delay;
                    acknowledged = acknowledge;
                }, new Endpoint(), "route");
                Thread.Sleep(200);
                Assert.That(commandsHandler.HandledCommands, Is.EqualTo(new[] {1}), "Command was not dispatched");
                Assert.That(retrydelay,Is.EqualTo(100));
                Assert.That(acknowledged,Is.EqualTo(false));
            }
        }          


        [Test]
        public void CommandsHandlerWithResultAndCommandOriginEndpointWiringTest()
        {
            using (var container = new WindsorContainer())
            {
                container
                    .Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()))
                    .AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(LocalBoundedContext.Named("bc")))
                    .Register(Component.For<CommandsHandler>().AsCommandsHandler("bc"))
                    .Resolve<ICqrsEngineBootstrapper>().Start();
                var cqrsEngine = (CqrsEngine)container.Resolve<ICqrsEngine>();
                var commandsHandler = container.Resolve<CommandsHandler>();

                bool acknowledged = false;
                long retrydelay = 0;
                var endpoint = new Endpoint();
                var command = DateTime.Now;
                cqrsEngine.BoundedContexts.First(c => c.Name == "bc").CommandDispatcher.Dispatch(command,CommandPriority.Low, (delay, acknowledge) =>
                {
                    retrydelay = delay;
                    acknowledged = acknowledge;
                }, endpoint, "route");
                Thread.Sleep(200);
                Assert.That(commandsHandler.HandledCommands.Count, Is.EqualTo(1), "Command was not dispatched");
                Assert.That(commandsHandler.HandledCommands[0], Is.TypeOf<RoutedCommand<DateTime>>(), "Command was not dispatched with wrong type");
                Assert.That(((RoutedCommand<DateTime>)(commandsHandler.HandledCommands[0])).Command, Is.EqualTo(command), "Routed command was not dispatched with wrong command");
                Assert.That(((RoutedCommand<DateTime>)(commandsHandler.HandledCommands[0])).OriginEndpoint, Is.EqualTo(endpoint), "Routed command was dispatched with wrong origin endpoint");
                Assert.That(((RoutedCommand<DateTime>)(commandsHandler.HandledCommands[0])).OriginRoute, Is.EqualTo("route"), "Routed command was dispatched with wrong origin route");
                Assert.That(retrydelay,Is.EqualTo(100));
                Assert.That(acknowledged,Is.EqualTo(false));
            }
        }        
        [Test]
        public void FailedCommandHandlerCausesRetryTest()
        {
            using (var container = new WindsorContainer())
            {
                container
                    .Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()))
                    .AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(LocalBoundedContext.Named("bc").FailedCommandRetryDelay(100)))
                    .Register(Component.For<CommandsHandler>().AsCommandsHandler("bc"))
                    .Resolve<ICqrsEngineBootstrapper>().Start();
                var cqrsEngine = (CqrsEngine)container.Resolve<ICqrsEngine>();
                var commandsHandler = container.Resolve<CommandsHandler>();

                bool acknowledged = false;
                long retrydelay = 0;
                cqrsEngine.BoundedContexts.First(c => c.Name == "bc").CommandDispatcher.Dispatch((long)1,CommandPriority.Low, (delay, acknowledge) =>
                {
                    retrydelay = delay;
                    acknowledged = acknowledge;
                }, new Endpoint(), "route");
                Thread.Sleep(200);
                Assert.That(retrydelay,Is.EqualTo(100));
                Assert.That(acknowledged,Is.EqualTo(false));
            }
        }


        public void SyntaxTest()
        {
            using (var container = new WindsorContainer())
            {
                container.Register(Component.For<IMessagingEngine>().Instance(MockRepository.GenerateMock<IMessagingEngine>()));
                container.AddFacility<CqrsFacility>(f => f.RunInMemory().BoundedContexts(
                    LocalBoundedContext.Named("local")
                        .PublishingEvents(typeof (int)).To("events").RoutedTo("events")
                        .ListeningCommands(typeof (string)).On("commands1").RoutedFromSameEndpoint()
                        .ListeningCommands(typeof (DateTime)).On("commands2").RoutedFromSameEndpoint()
                        .WithCommandsHandler<CommandHandler>(),
                    LocalBoundedContext.Named("projections")));

                container.Register(
                    Component.For<TestSaga>().AsSaga("local", "projections"),
                    Component.For<CommandHandler>().AsCommandsHandler("local"),
                    Component.For<EventsListener>().AsProjection("projections", "local")
                    );
            }
        }


    }

    public class EventListenerWithICommandSenderDependency
    {
        public ICommandSender Sender { get; set; }

        public EventListenerWithICommandSenderDependency(ICommandSender sender)
        {
            Sender = sender;
        }

        void Handle(string m)
        {
        } 
    }

    public class FakeEndpointResolver : IEndpointResolver
    {
        private readonly Dictionary<string, Endpoint> m_Endpoints = new Dictionary<string, Endpoint>
            {
                {"eventExchange", new Endpoint("test", "unistream.processing.events", true, "json")},
                {"eventQueue", new Endpoint("test", "unistream.processing.UPlusAdapter.TransferPublisher", true, "json")},
                {"commandExchange", new Endpoint("test", "unistream.u1.commands", true, "json")},
                {"commandQueue", new Endpoint("test", "unistream.u1.commands", true, "json")}
            };
        public Endpoint Resolve(string localBoundedContext, string remoteBoundedContext, string endpoint, Type type, RouteType routeType)
        {
            return m_Endpoints[endpoint];
        }
    }

 /*

    internal class Transfer
    {
        public Guid Id { get; set; }
        public string Code { get; set; }
        public DateTime CreationDate { get; set; }
    }

    public class TransferCreatedEvent
    {
        public Guid Id { get; set; }
        public string Code { get; set; }
    }

    public class TransferRegisteredInLigacyProcessingEvent
    {
        public Guid Id { get; set; }
        public DateTime CreationDate { get; set; }
    }

    public class TestCommand
    {

    }


    internal class TransferProjection
    {
        private readonly Dictionary<Guid, Transfer> m_Transfers= new Dictionary<Guid, Transfer>();

        public void Handle(TransferCreatedEvent e)
        {
            Transfer transfer;
            if (!m_Transfers.TryGetValue(e.Id, out transfer))
            {
                transfer= new Transfer { Id = e.Id };
                m_Transfers.Add(e.Id, transfer);
            }
            transfer.Code = e.Code;
        }

        public void Handle(TransferRegisteredInLigacyProcessingEvent e)
        {
            Transfer transfer;
            if (!m_Transfers.TryGetValue(e.Id, out transfer))
            {
                transfer= new Transfer { Id = e.Id };
                m_Transfers.Add(e.Id, transfer);
            }
            transfer.CreationDate = e.CreationDate;
        }
    }
*/
}