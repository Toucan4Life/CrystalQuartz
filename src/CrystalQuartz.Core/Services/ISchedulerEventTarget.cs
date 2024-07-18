namespace CrystalQuartz.Core.Services
{
    using CrystalQuartz.Core.Domain.Events;
    using Quartz;

    public interface ISchedulerEventTarget
    {
        void Push(RawSchedulerEvent @event, IJobExecutionContext? context);
    }
}