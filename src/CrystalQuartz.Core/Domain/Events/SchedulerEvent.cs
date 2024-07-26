namespace CrystalQuartz.Core.Domain.Events
{
    using System;
    using CrystalQuartz.Core.Domain.Base;
    using CrystalQuartz.Core.Utils;

    public class SchedulerEvent
    {
        public SchedulerEvent(
            long? id,
            long date,
            SchedulerEventScope scope,
            SchedulerEventType eventType,
            string itemKey,
            string fireInstanceId,
            bool faulted,
            ErrorMessage[] errors)
        {
            Id = id;
            Date = date;
            Scope = scope;
            EventType = eventType;
            ItemKey = itemKey;
            FireInstanceId = fireInstanceId;
            Faulted = faulted;
            Errors = errors;
        }

        public long? Id { get; }

        public long Date { get; }

        public SchedulerEventScope Scope { get; }

        public SchedulerEventType EventType { get; }

        public string ItemKey { get; }

        public string FireInstanceId { get; }

        public bool Faulted { get; }

        public ErrorMessage[] Errors { get; }
    }
}