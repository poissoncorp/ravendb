﻿using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Client.Server.Operations;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class UpdateEtlCommand<T> : UpdateDatabaseCommand where T : EtlDestination
    {
        public long TaskId { get; protected set; }

        public EtlConfiguration<T> Configuration { get; protected set; }

        public EtlType EtlType { get; protected set; }

        protected UpdateEtlCommand() : base(null)
        {
            // for deserialization
        }

        protected UpdateEtlCommand(long taskId, EtlConfiguration<T> configuration, EtlType type, string databaseName) : base(databaseName)
        {
            TaskId = taskId;
            Configuration = configuration;
            EtlType = type;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
            json[nameof(EtlType)] = EtlType;
        }
    }

    public class UpdateRavenEtlCommand : UpdateEtlCommand<RavenDestination>
    {
        public UpdateRavenEtlCommand()
        {
            // for deserialization
        }

        public UpdateRavenEtlCommand(long taskId, EtlConfiguration<RavenDestination> configuration, string databaseName) : base(taskId, configuration, EtlType.Raven, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.RavenEtl, DatabaseName).UpdateDatabaseRecord(record, etag);
            new AddRavenEtlCommand(Configuration, DatabaseName).UpdateDatabaseRecord(record, etag);

            return null;
        }
    }

    public class UpdateSqlEtlCommand : UpdateEtlCommand<SqlDestination>
    {
        public UpdateSqlEtlCommand()
        {
            // for deserialization
        }

        public UpdateSqlEtlCommand(long taskId, EtlConfiguration<SqlDestination> configuration, string databaseName) : base(taskId, configuration, EtlType.Sql, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.SqlEtl, DatabaseName).UpdateDatabaseRecord(record, etag);
            new AddSqlEtlCommand(Configuration, DatabaseName).UpdateDatabaseRecord(record, etag);

            return null;
        }
    }
}