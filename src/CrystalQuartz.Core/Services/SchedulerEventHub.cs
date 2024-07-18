namespace CrystalQuartz.Core.Services
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using CrystalQuartz.Core.Contracts;
    using CrystalQuartz.Core.Domain.Events;
    using CrystalQuartz.Core.Utils;
    using Newtonsoft.Json;
    using Quartz;
    using Quartz.Impl.Matchers;
    using Quartz.Xml.JobSchedulingData20;

    public class SchedulerEventHub : ISchedulerEventHub, ISchedulerEventTarget
    {
        private readonly ConcurrentQueue<SchedulerEvent> _events = new ConcurrentQueue<SchedulerEvent>();

        private readonly int _maxCapacity;
        private readonly long _hubSpanMilliseconds;
        private readonly IEventsTransformer _eventsTransformer;
        private readonly ISchedulerClerk _scheduler;
        private readonly Options _options;
        private int _previousId;

        public SchedulerEventHub(int maxCapacity, TimeSpan hubSpan, IEventsTransformer eventsTransformer, ISchedulerClerk scheduler, Options options)
        {
            _maxCapacity = maxCapacity;
            _eventsTransformer = eventsTransformer;
            _hubSpanMilliseconds = (long)hubSpan.TotalMilliseconds;
            _scheduler = scheduler;
            _options = options;

            _previousId = 0;
        }

        public void Push(RawSchedulerEvent @event, IJobExecutionContext? context)
        {
            int id = Interlocked.Increment(ref _previousId);
            SchedulerEvent item = _eventsTransformer.Transform(id, @event);

            if (_options.IsClustered && context != null)
            {
                var events = new List<SchedulerEvent>();

                IJobDetail jobDetail = context.JobDetail;
                if (jobDetail.JobDataMap.Contains("CrystalQuartz"))
                {
                    var datamap = jobDetail.JobDataMap.GetString("CrystalQuartz");
                    events = JsonConvert.DeserializeObject<List<SchedulerEvent>>(datamap);
                }

                events.Add(item);
                var eventAsString = JsonConvert.SerializeObject(events
                .Where(e => DateTime.Now.UnixTicks() - e.Date < _hubSpanMilliseconds + TimeSpan.FromSeconds(10).Milliseconds)
                .ToList());

                jobDetail.JobDataMap.Put("CrystalQuartz", eventAsString);
            }
            else
            {
                _events.Enqueue(item);
                SchedulerEvent temp;
                while (_events.Count > _maxCapacity && _events.TryDequeue(out temp))
                {
                }

                long now = DateTime.UtcNow.UnixTicks();
                while (!_events.IsEmpty && _events.TryPeek(out temp) && now - temp.Date > _hubSpanMilliseconds &&
                    _events.TryDequeue(out temp))
                {
                }
            }
        }

        public IEnumerable<SchedulerEvent> List(int minId)
        {
            return FetchEvents(minId).ToArray();
        }

        private IEnumerable<SchedulerEvent> FetchEvents(int edgeId)
        {
            if (!_options.IsClustered)
            {
                bool edgeFound = false;

                foreach (SchedulerEvent @event in _events)
                {
                    if (edgeFound)
                    {
                        yield return @event;
                    }
                    else if (@event.Id == edgeId)
                    {
                        edgeFound = true;
                    }
                }

                if (!edgeFound)
                {
                    foreach (var @event in _events)
                    {
                        yield return @event;
                    }
                }
            }
            else
            {
                IScheduler scheduler = _scheduler.GetScheduler();
                var jobs = scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup()).Result.SelectMany(jk =>
                {
                    JobDataMap jobDataMap = scheduler.GetJobDetail(jk).Result.JobDataMap;
                    if (jobDataMap.Contains("CrystalQuartz"))
                    {
                        var datamap = jobDataMap.GetString("CrystalQuartz");
                        return JsonConvert.DeserializeObject<List<SchedulerEvent>>(datamap);
                    }

                    return new List<SchedulerEvent>();
                }).Where(e => e != null)?.ToList() ?? new List<SchedulerEvent>();

                var jobs1 = jobs.Concat(_events).Where(ev => ev.Id >= edgeId).ToList();
                foreach (var job in jobs1)
                {
                    yield return job;
                }
            }
        }
    }
}