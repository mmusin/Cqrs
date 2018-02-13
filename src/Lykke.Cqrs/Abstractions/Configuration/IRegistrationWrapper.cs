namespace Inceptum.Cqrs.Configuration
{
    public interface IRegistrationWrapper<out T> : IRegistration
        where T : IRegistration
    {
        T Registration { get; }
    }
}