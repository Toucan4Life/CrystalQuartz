namespace CrystalQuartz.Core.Contracts
{
    using System;
    using CrystalQuartz.Core.Domain.Events;
    using Quartz;

    public class SchedulerEventArgs : EventArgs
    {
        public SchedulerEventArgs(RawSchedulerEvent payload, IJobExecutionContext context)
        {
            Payload = payload;
            Context = context;
        }

        public RawSchedulerEvent Payload { get; }

        public IJobExecutionContext Context { get; }
    }
}