﻿using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Sharding;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding.Background;
using Raven.Server.Documents.Sharding.Smuggler;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public class ShardedDocumentDatabase : DocumentDatabase
{
    public readonly int ShardNumber;
    
    public readonly string ShardedDatabaseName;

    public string ShardedDatabaseId { get; private set; }

    public ShardedDocumentsStorage ShardedDocumentsStorage;

    public ShardedPeriodicDocumentsMigrator PeriodicDocumentsMigrator { get; private set; }

    public ShardedDocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<string> addToInitLog)
        : base(name, configuration, serverStore, addToInitLog)
    {
        ShardNumber = ShardHelper.GetShardNumberFromDatabaseName(name);
        ShardedDatabaseName = ShardHelper.ToDatabaseName(name);
        Smuggler = new ShardedDatabaseSmugglerFactory(this);
    }

    protected override byte[] ReadSecretKey(TransactionOperationContext context) => ServerStore.GetSecretKey(context, ShardedDatabaseName);

    protected override void InitializeSubscriptionStorage()
    {
        SubscriptionStorage.Initialize(ShardedDatabaseName);
    }

    protected override void InitializeCompareExchangeStorage()
    {
        CompareExchangeStorage.Initialize(ShardedDatabaseName);
    }

    protected override void InitializeAndStartPeriodicDocumentsMigrator()
    {
        PeriodicDocumentsMigrator = new ShardedPeriodicDocumentsMigrator(this);
        PeriodicDocumentsMigrator.Start();
    }

    protected override DocumentsStorage CreateDocumentsStorage(Action<string> addToInitLog)
    {
        return ShardedDocumentsStorage = new ShardedDocumentsStorage(this, addToInitLog);
    }

    protected override IndexStore CreateIndexStore(ServerStore serverStore)
    {
        return new ShardedIndexStore(this, serverStore);
    }

    protected override ReplicationLoader CreateReplicationLoader()
    {
        return new ShardReplicationLoader(this, ServerStore);
    }

    internal override void SetIds(DatabaseTopology topology, string shardedDatabaseId)
    {
        base.SetIds(topology, shardedDatabaseId);
        ShardedDatabaseId = shardedDatabaseId;
    }

    public ShardingConfiguration ShardingConfiguration;

    private ConcurrentDictionary<long, Task> _confirmations = new ConcurrentDictionary<long, Task>();

    protected override void OnDatabaseRecordChanged(DatabaseRecord record)
    {
        // this called under lock
        base.OnDatabaseRecordChanged(record);

        ShardingConfiguration = record.Sharding;

        if (ServerStore.Sharding.ManualMigration)
            return;

        HandleReshardingChanges();
    }

    public void HandleReshardingChanges()
    {
        foreach (var migration in ShardingConfiguration.BucketMigrations)
        {
            var process = migration.Value;

            Task t = null;
            var index = 0L;
            if (ShardNumber == process.DestinationShard && process.Status == MigrationStatus.Moved)
            {
                if (process.ConfirmedDestinations.Contains(ServerStore.NodeTag) == false)
                {
                    using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var current = ShardedDocumentsStorage.GetMergedChangeVectorInBucket(context, process.Bucket);
                        var status = ChangeVector.GetConflictStatusForDocument(context.GetChangeVector(process.LastSourceChangeVector), current);
                        if (status == ConflictStatus.AlreadyMerged)
                        {
                            index = process.MigrationIndex;

                            if (_confirmations.TryGetValue(process.MigrationIndex, out t))
                            {
                                if (t.IsCompleted == false)
                                    continue;
                            }

                            t = ServerStore.Sharding.DestinationMigrationConfirm(ShardedDatabaseName, process.Bucket, process.MigrationIndex);
                        }
                    }
                }
            }

            if (process.SourceShard == ShardNumber && process.Status == MigrationStatus.OwnershipTransferred)
            {
                index = (long)Hashing.XXHash64.CalculateRaw(process.LastSourceChangeVector);

                if (_confirmations.TryGetValue(index, out t))
                {
                    if (t.IsCompleted == false)
                        continue;
                }

                // cleanup values
                t = DeleteBucket(process.Bucket, process.MigrationIndex, process.LastSourceChangeVector);
                
            }

            if (t != null)
            {
                _confirmations[index] = t;
                t.ContinueWith(__ => _confirmations.TryRemove(process.MigrationIndex, out _));
            }
        }
    }

    public ShardingConfiguration ReadShardingState()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, ShardedDatabaseName))
        {
            return raw.Sharding.MaterializedConfiguration;
        }
    }

    protected override ClusterTransactionBatchCollector CollectCommandsBatch(ClusterOperationContext context, int take)
    {
        var batchCollector = new ShardedClusterTransactionBatchCollector(this, take);
        var readCommands = ClusterTransactionCommand.ReadCommandsBatch(context, ShardedDatabaseName, fromCount: _nextClusterCommand, take: take);

        foreach (var command in readCommands)
        {
            batchCollector.MaxIndex = command.Index;
            batchCollector.MaxCommandCount = command.PreviousCount + command.Commands.Length;
            if (command.ShardNumber == ShardNumber)
                batchCollector.Add(command);
        }

        return batchCollector;
    }

    protected override void DisposeBackgroundWorkers(ExceptionAggregator exceptionAggregator)
    {
        base.DisposeBackgroundWorkers(exceptionAggregator);

        ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing ShardedPeriodicDocumentsMigrator");
        exceptionAggregator.Execute(() =>
        {
            PeriodicDocumentsMigrator?.Dispose();
        });
        ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed ShardedPeriodicDocumentsMigrator");
    }

    protected class ShardedClusterTransactionBatchCollector : ClusterTransactionBatchCollector
    {
        private readonly ShardedDocumentDatabase _database;

        public long MaxIndex = -1;
        public long MaxCommandCount = -1;

        public ShardedClusterTransactionBatchCollector(ShardedDocumentDatabase database, int maxSize) : base(maxSize)
        {
            _database = database;
        }

        public override void Dispose()
        {
            base.Dispose();
            if (Count == 0 || AllCommandsBeenProcessed)
            {
                if (MaxIndex >= 0)
                {
                    _database.RachisLogIndexNotifications.NotifyListenersAbout(MaxIndex, null);
                    _database._nextClusterCommand = MaxCommandCount;
                }
            }
        }
    }

    public async Task DeleteBucket(int bucket, long migrationIndex, string uptoChangeVector)
    {
        while (true)
        {
            var cmd = new DeleteBucketCommand(this, bucket, uptoChangeVector);
            await TxMerger.Enqueue(cmd);

            switch (cmd.Result)
            {
                // no documents in the bucket / everything was deleted
                case DeleteBucketCommand.DeleteBucketResult.Empty:
                    await ServerStore.Sharding.SourceMigrationCleanup(ShardedDatabaseName, bucket, migrationIndex);
                    return;
                // some documents skipped and left in the bucket
                case DeleteBucketCommand.DeleteBucketResult.Skipped:
                    return;
                // we have more docs, batch limit reached.
                case DeleteBucketCommand.DeleteBucketResult.FullBatch:
                    continue;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    public static ShardedDocumentDatabase CastToShardedDocumentDatabase(DocumentDatabase database) => database as ShardedDocumentDatabase ?? throw new ArgumentException($"Database {database.Name} must be sharded!");

    public class DeleteBucketCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly int _bucket;
        private readonly string _uptoChangeVector;
        public DeleteBucketResult Result;

        public DeleteBucketCommand(ShardedDocumentDatabase database, int bucket, string uptoChangeVector)
        {
            _database = database;
            _bucket = bucket;
            _uptoChangeVector = uptoChangeVector;
        }
        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Critical, "We need to create here proper tombstones so backup can pick it up RavenDB-19197");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Delete revision/attachments/ etc.. RavenDB-19197");

            Result = _database.ShardedDocumentsStorage.DeleteBucket(context, _bucket, context.GetChangeVector(_uptoChangeVector));
            return 1;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
        {
            throw new NotImplementedException();
        }

        public enum DeleteBucketResult
        {
            Empty,
            Skipped,
            FullBatch
        }
    }
}