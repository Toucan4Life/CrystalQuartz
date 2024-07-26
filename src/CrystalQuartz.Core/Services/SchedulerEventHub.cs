namespace CrystalQuartz.Core.Services
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Net.WebSockets;
    using System.Threading;
    using CrystalQuartz.Core.Contracts;
    using CrystalQuartz.Core.Domain.Base;
    using CrystalQuartz.Core.Domain.Events;
    using CrystalQuartz.Core.Utils;
    using Microsoft.Data.SqlClient;
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

        public SchedulerEventHub(int maxCapacity, TimeSpan hubSpan, IEventsTransformer eventsTransformer, ISchedulerClerk scheduler, Options options)
        {
            _maxCapacity = maxCapacity;
            _eventsTransformer = eventsTransformer;
            _hubSpanMilliseconds = (long)hubSpan.TotalMilliseconds;
            _scheduler = scheduler;
            _options = options;
        }

        public void Push(RawSchedulerEvent @event, IJobExecutionContext? context)
        {
            SchedulerEvent item = _eventsTransformer.Transform(null, @event);

            if (_options.ClusterConnectionString != null && context != null)
            {
                using (SqlConnection connection = new SqlConnection(_options.ClusterConnectionString))
                {
                    connection.Open();
                    SqlCommand cmd = new SqlCommand(
                        @"INSERT INTO QRTZ_CRYSTAL (Date, Scope, EventType,itemKey,fireInstanceId,faulted,ErrorMessages) 
                        VALUES  (@Date, @Scope, @EventType,@itemKey,@fireInstanceId,@faulted,@ErrorMessages)",
                        connection);

                    cmd.Parameters.Add("@Date", SqlDbType.BigInt).Value = item.Date;
                    cmd.Parameters.Add("@Scope", SqlDbType.VarChar).Value = item.Scope;
                    cmd.Parameters.Add("@EventType", SqlDbType.VarChar).Value = item.EventType;
                    cmd.Parameters.Add("@itemKey", SqlDbType.VarChar).Value = item.ItemKey;
                    cmd.Parameters.Add("@fireInstanceId", SqlDbType.VarChar).Value = item.FireInstanceId;
                    cmd.Parameters.Add("@faulted", SqlDbType.VarChar).Value = item.Faulted;
                    cmd.Parameters.Add("@ErrorMessages", SqlDbType.NVarChar).Value = JsonConvert.SerializeObject(item.Errors);
                    cmd.ExecuteNonQuery();

                    SqlCommand cmd2 = new SqlCommand(
                       @"DELETE FROM QRTZ_CRYSTAL WHERE Date < @Date",
                       connection);

                    cmd2.Parameters.Add("@Date", SqlDbType.BigInt).Value = (DateTime.Now - TimeSpan.FromMilliseconds(_hubSpanMilliseconds) - TimeSpan.FromSeconds(10)).UnixTicks();
                    cmd2.ExecuteNonQuery();
                }
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
            if (_options.ClusterConnectionString == null)
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
                List<SchedulerEvent> jobs = new List<SchedulerEvent>();
                using (SqlConnection connection = new SqlConnection(_options.ClusterConnectionString))
                {
                    connection.Open();
                    SqlCommand cmd = new SqlCommand(@"SELECT * FROM QRTZ_CRYSTAL WHERE ID > @ID", connection);
                    cmd.Parameters.Add("@ID", SqlDbType.BigInt).Value = edgeId;

                    using (SqlDataReader rdr = cmd.ExecuteReader())
                    {
                        while (rdr.Read())
                        {
                            jobs.Add(new SchedulerEvent(
                                (long)rdr["Id"],
                                (long)rdr["Date"],
                                (SchedulerEventScope)Enum.Parse(typeof(SchedulerEventScope), (string)rdr["Scope"]),
                                (SchedulerEventType)Enum.Parse(typeof(SchedulerEventType), (string)rdr["EventType"]),
                                (string)rdr["itemKey"],
                                (string)rdr["fireInstanceId"],
                                bool.Parse((string)rdr["faulted"]),
                                JsonConvert.DeserializeObject<List<ErrorMessage>>((string)rdr["ErrorMessages"])?.ToArray()));
                        }
                    }
                }

                var jobs1 = jobs.Concat(_events).Where(ev => ev.Id > edgeId).ToList();
                foreach (var job in jobs1)
                {
                    yield return job;
                }
            }
        }
    }
}