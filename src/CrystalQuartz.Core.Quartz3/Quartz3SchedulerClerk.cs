﻿namespace CrystalQuartz.Core.Quartz3
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CrystalQuartz.Core.Contracts;
    using CrystalQuartz.Core.Domain;
    using CrystalQuartz.Core.Domain.Activities;
    using CrystalQuartz.Core.Utils;
    using Quartz;
    using Quartz.Impl.Matchers;

    internal class Quartz3SchedulerClerk : ISchedulerClerk
    {
        private static readonly TriggerTypeExtractor TriggerTypeExtractor = new TriggerTypeExtractor();
        private readonly IScheduler _scheduler;

        public Quartz3SchedulerClerk(IScheduler scheduler)
        {
            _scheduler = scheduler;
        }

        public async Task<SchedulerData> GetSchedulerData()
        {
            IScheduler scheduler = _scheduler;
            SchedulerMetaData metadata = await scheduler.GetMetaData();

            IReadOnlyCollection<IJobExecutionContext> currentlyExecutingJobs = await scheduler.GetCurrentlyExecutingJobs();
            IList<ExecutingJobInfo> inProgressJobs = metadata.SchedulerRemote ? (IList<ExecutingJobInfo>)new ExecutingJobInfo[0] :
                currentlyExecutingJobs
                    .Select(x => new ExecutingJobInfo
                    {
                        UniqueTriggerKey = x.Trigger.Key.ToString(),
                        FireInstanceId = x.FireInstanceId,
                    })
                    .ToList();

            IReadOnlyCollection<JobKey> jobKeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());

            return new SchedulerData
            {
                Name = scheduler.SchedulerName,
                InstanceId = scheduler.SchedulerInstanceId,
                JobGroups = await GetJobGroups(scheduler),
                Status = await GetSchedulerStatus(scheduler),
                JobsExecuted = metadata.NumberOfJobsExecuted,
                JobsTotal = scheduler.IsShutdown ? 0 : jobKeys.Count,
                RunningSince = metadata.RunningSince.ToDateTime(),
                InProgress = inProgressJobs,
            };
        }

        public async Task<JobDetailsData?> GetJobDetailsData(string name, string group)
        {
            var scheduler = _scheduler;
            if (scheduler.IsShutdown)
            {
                return null;
            }

            IJobDetail job;

            try
            {
                job = await scheduler.GetJobDetail(new JobKey(name, group));
            }
            catch (Exception)
            {
                // GetJobDetail method throws exceptions for remote
                // scheduler in case when JobType requires an external
                // assembly to be referenced.
                // see https://github.com/guryanovev/CrystalQuartz/issues/16 for details
                return new JobDetailsData(null, null);
            }

            if (job == null)
            {
                return null;
            }

            return new JobDetailsData(
                GetJobDetails(job),
                job.JobDataMap.ToDictionary(x => x.Key, x => x.Value));
        }

        public async Task<SchedulerDetails> GetSchedulerDetails()
        {
            IScheduler scheduler = _scheduler;
            SchedulerMetaData metadata = await scheduler.GetMetaData();

            return new SchedulerDetails
            {
                InStandbyMode = metadata.InStandbyMode,
                JobStoreClustered = metadata.JobStoreClustered,
                JobStoreSupportsPersistence = metadata.JobStoreSupportsPersistence,
                JobStoreType = metadata.JobStoreType,
                NumberOfJobsExecuted = metadata.NumberOfJobsExecuted,
                RunningSince = metadata.RunningSince.ToUnixTicks(),
                SchedulerInstanceId = metadata.SchedulerInstanceId,
                SchedulerName = metadata.SchedulerName,
                SchedulerRemote = metadata.SchedulerRemote,
                SchedulerType = metadata.SchedulerType,
                Shutdown = metadata.Shutdown,
                Started = metadata.Started,
                ThreadPoolSize = metadata.ThreadPoolSize,
                ThreadPoolType = metadata.ThreadPoolType,
                Version = metadata.Version,
            };
        }

        public async Task<TriggerDetailsData?> GetTriggerDetailsData(string name, string group)
        {
            var scheduler = _scheduler;
            if (scheduler.IsShutdown)
            {
                return null;
            }

            ITrigger trigger = await scheduler.GetTrigger(new TriggerKey(name, group));
            if (trigger == null)
            {
                return null;
            }

            return new TriggerDetailsData
            {
                PrimaryTriggerData = await GetTriggerData(scheduler, trigger),
                SecondaryTriggerData = GetTriggerSecondaryData(trigger),
                JobDataMap = trigger.JobDataMap.ToDictionary(x => x.Key, x => x.Value),
            };
        }

        public async Task<IEnumerable<Type>> GetScheduledJobTypes()
        {
            var scheduler = _scheduler;
            if (scheduler.IsShutdown)
            {
                return Type.EmptyTypes;
            }

            IReadOnlyCollection<JobKey> jobKeys = await scheduler
                .GetJobKeys(GroupMatcher<JobKey>.AnyGroup());

            IList<Type> result = new List<Type>();

            foreach (JobKey jobKey in jobKeys)
            {
                Type jobType = (await scheduler.GetJobDetail(jobKey)).JobType;

                result.Add(jobType);
            }

            return result;
        }

        public IScheduler GetScheduler()
        {
            return _scheduler;
        }

        private static TriggerSecondaryData GetTriggerSecondaryData(ITrigger trigger)
        {
            return new TriggerSecondaryData
            {
                Description = trigger.Description,
                Priority = trigger.Priority,
                MisfireInstruction = trigger.MisfireInstruction,
            };
        }

        private static async Task<SchedulerStatus> GetSchedulerStatus(IScheduler scheduler)
        {
            if (scheduler.IsShutdown)
            {
                return SchedulerStatus.Shutdown;
            }

            IReadOnlyCollection<string> jobGroupNames = await scheduler.GetJobGroupNames();
            if (jobGroupNames == null || jobGroupNames.Count == 0)
            {
                return SchedulerStatus.Empty;
            }

            if (scheduler.InStandbyMode)
            {
                return SchedulerStatus.Ready;
            }

            if (scheduler.IsStarted)
            {
                return SchedulerStatus.Started;
            }

            return SchedulerStatus.Ready;
        }

        private static async Task<ActivityStatus> GetTriggerStatus(string triggerName, string triggerGroup, IScheduler scheduler)
        {
            var state = await scheduler.GetTriggerState(new TriggerKey(triggerName, triggerGroup));

            switch (state)
            {
                case TriggerState.Paused:
                    return ActivityStatus.Paused;
                case TriggerState.Complete:
                    return ActivityStatus.Complete;
                default:
                    return ActivityStatus.Active;
            }
        }

        private static Task<ActivityStatus> GetTriggerStatus(ITrigger trigger, IScheduler scheduler)
        {
            return GetTriggerStatus(trigger.Key.Name, trigger.Key.Group, scheduler);
        }

        private static async Task<IList<JobGroupData>> GetJobGroups(IScheduler scheduler)
        {
            var result = new List<JobGroupData>();

            if (!scheduler.IsShutdown)
            {
                foreach (var groupName in await scheduler.GetJobGroupNames())
                {
                    var groupData = new JobGroupData(
                        groupName,
                        await GetJobs(scheduler, groupName));

                    result.Add(groupData);
                }
            }

            return result;
        }

        private static async Task<IList<JobData>> GetJobs(IScheduler scheduler, string groupName)
        {
            var result = new List<JobData>();

            foreach (var jobKey in await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)))
            {
                result.Add(await GetJobData(scheduler, jobKey.Name, groupName));
            }

            return result;
        }

        private static async Task<JobData> GetJobData(IScheduler scheduler, string jobName, string group)
        {
            return new JobData(jobName, group, await GetTriggers(scheduler, jobName, group));
        }

        private static async Task<IList<TriggerData>> GetTriggers(IScheduler scheduler, string jobName, string group)
        {
            IReadOnlyCollection<ITrigger> triggers = await scheduler
                .GetTriggersOfJob(new JobKey(jobName, group));

            IList<TriggerData> result = new List<TriggerData>();

            foreach (ITrigger trigger in triggers)
            {
                result.Add(await GetTriggerData(scheduler, trigger));
            }

            return result;
        }

        private static async Task<TriggerData> GetTriggerData(IScheduler scheduler, ITrigger trigger)
        {
            ActivityStatus activityStatus = await GetTriggerStatus(trigger, scheduler);

            return new TriggerData(
                trigger.Key.ToString(),
                trigger.Key.Group,
                trigger.Key.Name,
                activityStatus,
                trigger.StartTimeUtc.ToUnixTicks(),
                trigger.EndTimeUtc.ToUnixTicks(),
                trigger.GetNextFireTimeUtc().ToUnixTicks(),
                trigger.GetPreviousFireTimeUtc().ToUnixTicks(),
                TriggerTypeExtractor.GetFor(trigger));
        }

        private JobDetails GetJobDetails(IJobDetail job)
        {
            return new JobDetails
            {
                ConcurrentExecutionDisallowed = job.ConcurrentExecutionDisallowed,
                Description = job.Description,
                Durable = job.Durable,
                JobType = job.JobType,
                PersistJobDataAfterExecution = job.PersistJobDataAfterExecution,
                RequestsRecovery = job.RequestsRecovery,
            };
        }
    }
}