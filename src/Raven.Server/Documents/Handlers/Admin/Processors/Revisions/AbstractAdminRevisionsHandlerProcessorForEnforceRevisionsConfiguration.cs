﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract void AddOperation(long operationId, OperationCancelToken addOpToken);

        public override async ValueTask ExecuteAsync()
        {
            var token = RequestHandler.CreateTimeLimitedOperationToken();
            var operationId = RequestHandler.GetLongQueryString("operationId", false) ?? RequestHandler.ServerStore.Operations.GetNextOperationId();

            AddOperation(operationId, token);

            using (ClusterContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, RequestHandler.ServerStore.NodeTag);
            }
        }
    }
}
