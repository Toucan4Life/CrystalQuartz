using System.Reflection;
using CrystalQuartz.Core;
using CrystalQuartz.WebFramework.Config;
using CrystalQuartz.WebFramework.Request;
using CrystalQuartz.Core.SchedulerProviders;

namespace CrystalQuartz.Application
{
    using System.Data;
    using System.Threading.Tasks;
    using Commands;
    using Commands.Serialization;
    using Microsoft.Data.SqlClient;
    using WebFramework;
    using WebFramework.HttpAbstractions;

    public class LazyApplication : IRunningApplication
    {
        private readonly SchedulerHostInitializer _initializer;
        private readonly IRunningApplication _nested;

        public LazyApplication(SchedulerHostInitializer initializer, IRunningApplication nested)
        {
            _initializer = initializer;
            _nested = nested;
        }

        public async Task Handle(IRequest request, IResponseRenderer renderer)
        {
            if (_initializer.SchedulerHostCreated && _initializer.SchedulerHost.Faulted)
            {
                _initializer.ResetCreatedSchedulerHost();
            }

            if (!_initializer.SchedulerHostCreated)
            {
                await _initializer.EnsureHostCreated();
            }

            await _nested.Handle(request, renderer);
        }
    }

    public class CrystalQuartzPanelApplication : Application
    {
        private readonly ISchedulerProvider _schedulerProvider;
        private readonly Options _options;
        private readonly SchedulerHostInitializer _schedulerHostInitializer;

        public CrystalQuartzPanelApplication(
            ISchedulerProvider schedulerProvider,
            Options options) :

            base(Assembly.GetAssembly(typeof(CrystalQuartzPanelApplication)),
                "CrystalQuartz.Application.Content.",
                options.ErrorAction)
        {
            _schedulerProvider = schedulerProvider;
            _options = options;
            _schedulerHostInitializer = new SchedulerHostInitializer(_schedulerProvider, _options);
        }

        public override IRunningApplication Run()
        {
            if (_options.ClusterConnectionString != null)
            {
                using (SqlConnection connection = new SqlConnection(_options.ClusterConnectionString))
                {
                    connection.Open();
                    SqlCommand cmd = new SqlCommand(@"IF EXISTS(
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'QRTZ_CRYSTAL') 
                    SELECT 1 ELSE SELECT 0", connection);

                    int exists = (int)cmd.ExecuteScalar();
                    if (exists == 0)
                    {
                        SqlCommand cmd2 = new SqlCommand(@"CREATE TABLE QRTZ_CRYSTAL (
                           Id bigint IDENTITY(1,1) PRIMARY KEY, 
                            Date bigint,
                            Scope varchar(255),
                            EventType varchar(255),
                            itemKey varchar(255),
                            fireInstanceId varchar(255),
                            faulted varchar(255),
                            ErrorMessages nvarchar(max),
                        );", connection);
                        cmd2.ExecuteNonQuery();
                    }
                }

            }
            IRunningApplication result = new LazyApplication(_schedulerHostInitializer, base.Run());

            if (!_options.LazyInit)
            {
                // Please not that we cannot await EnsureHostCreated method here
                // because Run can be called out of async scope, for example in a
                // middleware constructor. So we just do call-and-forget here.
#pragma warning disable CS4014
                _schedulerHostInitializer.EnsureHostCreated();
#pragma warning restore CS4014
            }

            return result;
        }

        public override IHandlerConfig Configure()
        {
            ISchedulerHostProvider schedulerHost = _schedulerHostInitializer;

            var schedulerDataSerializer = new SchedulerDataOutputSerializer();
            if(_options.IsReadOnly){
                return ReadOnlyApp(schedulerHost, schedulerDataSerializer);
            }
            return App(schedulerHost, schedulerDataSerializer);
        }
        private IHandlerConfig ReadOnlyApp(ISchedulerHostProvider schedulerHost, SchedulerDataOutputSerializer schedulerDataSerializer)
        {
            return this

                .WithHandler(new FileRequestHandler(Assembly.GetExecutingAssembly(), Context.DefautResourcePrefix)) 
                /* 
                 * Misc commands
                 */
                .WhenCommand("get_data").Do(new GetDataCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("get_env").Do(new GetEnvironmentDataCommand(schedulerHost, _options.CustomCssUrl, _options.TimelineSpan, _options.FrameworkVersion, _options.IsReadOnly), new EnvironmentDataOutputSerializer())
                .WhenCommand("get_job_details").Do(new GetJobDetailsCommand(schedulerHost, _options.JobDataMapTraversingOptions), new JobDetailsOutputSerializer())
                .WhenCommand("get_trigger_details").Do(new GetTriggerDetailsCommand(schedulerHost, _options.JobDataMapTraversingOptions), new TriggerDetailsOutputSerializer())
                .WhenCommand("get_input_types").Do(new GetInputTypesCommand(_options.JobDataMapInputTypes), new InputTypeOptionsSerializer())
                .WhenCommand("get_input_type_variants").Do(new GetInputTypeVariantsCommand(_options.JobDataMapInputTypes), new InputTypeVariantOutputSerializer())
                .WhenCommand("get_job_types").Do(new GetAllowedJobTypesCommand(schedulerHost), new JobTypesOutputSerializer())

                .Else().MapTo("index.html");
        }

        private IHandlerConfig App(ISchedulerHostProvider schedulerHost, SchedulerDataOutputSerializer schedulerDataSerializer)
        {
            return this

                .WithHandler(new FileRequestHandler(Assembly.GetExecutingAssembly(), Context.DefautResourcePrefix))

                /*
                 * Trigger commands
                 */
                .WhenCommand("pause_trigger").Do(new PauseTriggerCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("resume_trigger").Do(new ResumeTriggerCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("delete_trigger").Do(new DeleteTriggerCommand(schedulerHost), schedulerDataSerializer)

                .WhenCommand("add_trigger").Do(
                    new AddTriggerCommand(schedulerHost, _options.JobDataMapInputTypes),
                    new AddTriggerOutputSerializer())

                /*                                     
                 * Group commands                      
                 */
                .WhenCommand("pause_group").Do(new PauseGroupCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("resume_group").Do(new ResumeGroupCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("delete_group").Do(new DeleteGroupCommand(schedulerHost), schedulerDataSerializer)

                /*                                     
                 * Job commands                        
                 */
                .WhenCommand("pause_job").Do(new PauseJobCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("resume_job").Do(new ResumeJobCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("delete_job").Do(new DeleteJobCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("execute_job").Do(new ExecuteNowCommand(schedulerHost), schedulerDataSerializer)

                /* 
                 * Scheduler commands
                 */
                .WhenCommand("start_scheduler").Do(new StartSchedulerCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("stop_scheduler").Do(new StopSchedulerCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("get_scheduler_details").Do(new GetSchedulerDetailsCommand(schedulerHost), new SchedulerDetailsOutputSerializer())
                .WhenCommand("pause_scheduler").Do(new PauseAllCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("resume_scheduler").Do(new ResumeAllCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("standby_scheduler").Do(new StandbySchedulerCommand(schedulerHost), schedulerDataSerializer)

                /* 
                 * Misc commands
                 */
                .WhenCommand("get_data").Do(new GetDataCommand(schedulerHost), schedulerDataSerializer)
                .WhenCommand("get_env").Do(new GetEnvironmentDataCommand(schedulerHost, _options.CustomCssUrl, _options.TimelineSpan, _options.FrameworkVersion, _options.IsReadOnly), new EnvironmentDataOutputSerializer())
                .WhenCommand("get_job_details").Do(new GetJobDetailsCommand(schedulerHost, _options.JobDataMapTraversingOptions), new JobDetailsOutputSerializer())
                .WhenCommand("get_trigger_details").Do(new GetTriggerDetailsCommand(schedulerHost, _options.JobDataMapTraversingOptions), new TriggerDetailsOutputSerializer())
                .WhenCommand("get_input_types").Do(new GetInputTypesCommand(_options.JobDataMapInputTypes), new InputTypeOptionsSerializer())
                .WhenCommand("get_input_type_variants").Do(new GetInputTypeVariantsCommand(_options.JobDataMapInputTypes), new InputTypeVariantOutputSerializer())
                .WhenCommand("get_job_types").Do(new GetAllowedJobTypesCommand(schedulerHost), new JobTypesOutputSerializer())

                .Else().MapTo("index.html");
        }
    }
}