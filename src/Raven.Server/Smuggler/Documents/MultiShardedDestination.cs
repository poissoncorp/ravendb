﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Actions;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Smuggler.Documents
{
    public class MultiShardedDestination : ISmugglerDestination
    {
        private readonly ShardedDatabaseContext _databaseContext;
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly long _operationId;
        private readonly ISmugglerSource _source;
        private readonly Dictionary<int, StreamDestination> _destinations;
        private DatabaseSmugglerOptionsServerSide _options;
        private readonly ByteStringContext _allocator;

        public MultiShardedDestination([NotNull] ISmugglerSource source, [NotNull] ShardedDatabaseContext databaseContext, [NotNull] ShardedDatabaseRequestHandler handler, long operationId)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _databaseContext = databaseContext ?? throw new ArgumentNullException(nameof(databaseContext));
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
            _operationId = operationId;
            _destinations = new Dictionary<int, StreamDestination>(databaseContext.ShardCount);
            _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        }

        public async ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
        {
            _options = options;

            // we will send decrypted data to the shards
            _options.EncryptionKey = null;
            var holders = new Dictionary<int, StreamDestinationHolder>(_databaseContext.ShardCount);

            foreach (var shardNumber in _databaseContext.ShardsTopology.Keys)
            {
                holders.Add(shardNumber, new StreamDestinationHolder());
            }

            var importOperation = new ShardedImportOperation(_handler.HttpContext.Request, options, holders, _operationId);
            var t = _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(importOperation);

            await Task.WhenAll(importOperation.ExposedStreamTasks.Values);

            foreach (int shardNumber in holders.Keys)
            {
                await PrepareShardStreamDestination(holders, shardNumber, result, buildVersion);
            }

            return new AsyncDisposableAction(async () =>
            {
                foreach (var holder in holders.Values)
                {
                    await holder.DisposeAsync();
                }

                await t;
            });
        }

        private async Task PrepareShardStreamDestination(Dictionary<int, StreamDestinationHolder> holders, int shardNumber, SmugglerResult result, long buildVersion)
        {
            var stream = ShardedSmugglerHandlerProcessorForImport.GetOutputStream(holders[shardNumber].OutStream.OutputStream.Result, _options);
            holders[shardNumber].InputStream = stream;
            holders[shardNumber].ContextReturn = _handler.ContextPool.AllocateOperationContext(out JsonOperationContext context);
            var destination = new StreamDestination(stream, context, _source, compressionLevel: _databaseContext.Configuration.Sharding.CompressionLevel);
            holders[shardNumber].DestinationAsyncDisposable = await destination.InitializeAsync(_options, result, buildVersion);
            _destinations[shardNumber] = destination;
        }

        internal class StreamDestinationHolder : IAsyncDisposable
        {
            public Stream InputStream;
            public StreamExposerContent OutStream;
            public IDisposable ContextReturn;
            public IAsyncDisposable DestinationAsyncDisposable;

            public async ValueTask DisposeAsync()
            {
                await DestinationAsyncDisposable.DisposeAsync();
                OutStream.Complete();
                ContextReturn.Dispose();
            }
        }

        // All the NotImplementedException methods are handled on the smuggler level, since they are cluster wide and do no require any specific database
        public IDatabaseRecordActions DatabaseRecord() => throw new NotImplementedException();
        public IIndexActions Indexes() => new DatabaseIndexActions(_databaseContext.Indexes.Create, _databaseContext.Time);
        public IKeyValueActions<long> Identities() => throw new NotImplementedException();
        public ISubscriptionActions Subscriptions() => throw new NotImplementedException();
        public IReplicationHubCertificateActions ReplicationHubCertificates() => throw new NotImplementedException();

        public ICompareExchangeActions CompareExchange(string databaseName, JsonOperationContext context, BackupKind? backupKind, bool withDocuments)
        {
            return withDocuments ? null : new ShardedCompareExchangeActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.CompareExchange(databaseName, context, backupKind, withDocuments: false)), _options);
        }

        public ICompareExchangeActions CompareExchangeTombstones(string databaseName, JsonOperationContext context) =>
            new ShardedCompareExchangeActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.CompareExchangeTombstones(databaseName, context)), _options);

        public IDocumentActions Documents(bool throwOnCollectionMismatchError = true) =>
            new ShardedDocumentActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.Documents(throwOnDuplicateCollection: false)), _options);

        public IDocumentActions RevisionDocuments() =>
            new ShardedDocumentActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.RevisionDocuments()), _options);

        public IDocumentActions Tombstones() =>
            new ShardedDocumentActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.Tombstones()), _options);

        public IDocumentActions Conflicts() =>
            new ShardedDocumentActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.Conflicts()), _options);

        public ICounterActions Counters(SmugglerResult result) =>
            new ShardedCounterActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.Counters(result)), _options);

        public ITimeSeriesActions TimeSeries() =>
            new ShardedTimeSeriesActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.TimeSeries()), _options);

        public ILegacyActions LegacyDocumentDeletions() =>
            new ShardedLegacyActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.LegacyDocumentDeletions()), _options);

        public ILegacyActions LegacyAttachmentDeletions() =>
            new ShardedLegacyActions(_databaseContext, _allocator, _destinations.ToDictionary(x => x.Key, x => x.Value.LegacyAttachmentDeletions()), _options);


        private abstract class ShardedActions<T> : INewDocumentActions, INewCompareExchangeActions where T : IAsyncDisposable
        {
            private JsonOperationContext _context;
            private IDisposable _rtnCtx;
            private IDisposable _prevRtnCtx;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            protected readonly ShardedDatabaseContext DatabaseContext;
            protected readonly Dictionary<int, T> _actions;
            protected readonly T _last;

            protected ShardedActions(ShardedDatabaseContext databaseContext, Dictionary<int, T> actions, DatabaseSmugglerOptionsServerSide options)
            {
                DatabaseContext = databaseContext;
                _actions = actions;
                _last = _actions.Last().Value;
                _options = options;
                _rtnCtx = DatabaseContext.AllocateOperationContext(out _context);
            }

            public JsonOperationContext GetContextForNewCompareExchangeValue() => GetContextForNewDocument();

            public JsonOperationContext GetContextForNewDocument()
            {
                if (_context.AllocatedMemory > 16 * 1024 * 1024)
                {
                    var old = _rtnCtx;
                    using (_prevRtnCtx)
                    {
                        _rtnCtx = DatabaseContext.AllocateOperationContext(out _context);
                    }
                    _prevRtnCtx = old;
                }

                return _context;
            }

            public virtual async ValueTask DisposeAsync()
            {
                foreach (var action in _actions.Values)
                {
                    await action.DisposeAsync();
                }

                _rtnCtx.Dispose();
                _prevRtnCtx?.Dispose();
            }

            public Stream GetTempStream() => StreamDestination.GetTempStream(_options);
        }

        private class ShardedCompareExchangeActions : ShardedActions<ICompareExchangeActions>, ICompareExchangeActions
        {
            private readonly ByteStringContext _allocator;

            public ShardedCompareExchangeActions(ShardedDatabaseContext databaseContext, ByteStringContext allocator, Dictionary<int, ICompareExchangeActions> actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = allocator;
            }

            public async ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value, Document existingDocument)
            {
                if (ClusterTransactionCommand.IsAtomicGuardKey(key, out var docId))
                {
                    var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, docId);
                    await _actions[shardNumber].WriteKeyValueAsync(key, value, existingDocument);
                    return;
                }

                await _last.WriteKeyValueAsync(key, value, existingDocument);
            }

            public async ValueTask WriteTombstoneKeyAsync(string key)
            {
                if (ClusterTransactionCommand.IsAtomicGuardKey(key, out var docId))
                {
                    var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, docId);
                    await _actions[shardNumber].WriteTombstoneKeyAsync(key);
                    return;
                }

                await _last.WriteTombstoneKeyAsync(key);
            }

            public ValueTask FlushAsync()
            {
                return ValueTask.CompletedTask;
            }
        }


        private class ShardedDocumentActions : ShardedActions<IDocumentActions>, IDocumentActions
        {
            private readonly ByteStringContext _allocator;

            public ShardedDocumentActions(ShardedDatabaseContext databaseContext, ByteStringContext allocator, Dictionary<int, IDocumentActions> actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = allocator;
            }

            public async ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress, Func<ValueTask> beforeFlushing = null)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, item.Document.Id);
                await _actions[shardNumber].WriteDocumentAsync(item, progress, beforeFlushing);
            }

            public async ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, tombstone.LowerId);
                await _actions[shardNumber].WriteTombstoneAsync(tombstone, progress);
            }

            public async ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, conflict.Id);
                await _actions[shardNumber].WriteConflictAsync(conflict, progress);
            }

            public ValueTask DeleteDocumentAsync(string id) => ValueTask.CompletedTask;

            public IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection()
            {
                yield break;
            }
        }

        private class ShardedCounterActions : ShardedActions<ICounterActions>, ICounterActions
        {
            private readonly ByteStringContext _allocator;

            public ShardedCounterActions(ShardedDatabaseContext databaseContext, ByteStringContext allocator, Dictionary<int, ICounterActions> actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = allocator;
            }

            public async ValueTask WriteCounterAsync(CounterGroupDetail counterDetail)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, counterDetail.DocumentId);
                await _actions[shardNumber].WriteCounterAsync(counterDetail);
            }

            public async ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, counterDetail.DocumentId);
                await _actions[shardNumber].WriteLegacyCounterAsync(counterDetail);
            }

            public void RegisterForDisposal(IDisposable data)
            {
            }
        }

        private class ShardedTimeSeriesActions : ShardedActions<ITimeSeriesActions>, ITimeSeriesActions
        {
            private readonly ByteStringContext _allocator;

            public ShardedTimeSeriesActions(ShardedDatabaseContext databaseContext, ByteStringContext allocator, Dictionary<int, ITimeSeriesActions> actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = allocator;
            }

            public async ValueTask WriteTimeSeriesAsync(TimeSeriesItem ts)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, ts.DocId);
                await _actions[shardNumber].WriteTimeSeriesAsync(ts);
            }

            public void RegisterForDisposal(IDisposable data)
            {
                // TODO fix this after handling this properly in 5.4
            }

            public void RegisterForReturnToTheContext(AllocatedMemoryData data)
            {
                // TODO fix this after handling this properly in 5.4
            }
        }

        private class ShardedLegacyActions : ShardedActions<ILegacyActions>, ILegacyActions
        {
            private readonly ByteStringContext _allocator;

            public ShardedLegacyActions(ShardedDatabaseContext databaseContext, ByteStringContext allocator, Dictionary<int, ILegacyActions> actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = allocator;
            }

            public async ValueTask WriteLegacyDeletions(string id)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, id);
                await _actions[shardNumber].WriteLegacyDeletions(id);
            }
        }
    }
}