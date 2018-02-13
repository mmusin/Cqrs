﻿using System;
using System.Collections.Generic;
using Lykke.Cqrs;

namespace Inceptum.Cqrs.Configuration
{
    public interface IRegistration
    {
        void Create(CqrsEngine cqrsEngine);
        void Process(CqrsEngine cqrsEngine);
        IEnumerable<Type> Dependencies { get; }
    }
}