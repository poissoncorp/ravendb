﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterOperationTests : ClusterTestBase
    {
        public ClusterOperationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReorderDatabaseNodes()
        {
            var db = "ReorderDatabaseNodes";
            var (_, leader) = await CreateRaftCluster(3);
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await ReverseOrderSuccessfully(store, db);
                await FailSuccessfully(store, db);
            }
        }

        public static async Task FailSuccessfully(IDocumentStore store, string db)
        {
            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(db, new List<string>()
                {
                    "A",
                    "B"
                }));
            });
            Assert.True(ex.InnerException is ArgumentException);
            ex = await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(db, new List<string>()
                {
                    "C",
                    "B",
                    "A",
                    "F"
                }));
            });
            Assert.True(ex.InnerException is ArgumentException);
        }

        [Fact]
        public async Task ClusterWideIdentity()
        {
            var db = "ClusterWideIdentity";
            var (_, leader) = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            var nonLeader = Servers.First(x => ReferenceEquals(x, leader) == false);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { nonLeader.WebUrl }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var result = store.Maintenance.SendAsync(new SeedIdentityForOperation("users", 1990));
                    Assert.Equal(1990, result.Result);

                    var user = new User
                    {
                        Name = "Adi",
                        LastName = "Async"
                    };
                    await session.StoreAsync(user, "users|");
                    await session.SaveChangesAsync();
                    var id = session.Advanced.GetDocumentId(user);
                    Assert.Equal("users/1991", id);
                }
            }
        }

        [Fact]
        public async Task NextIdentityForOperationShouldBroadcast()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "15";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "15";

            var database = GetDatabaseName();
            var numberOfNodes = 3;
            var cluster = await CreateRaftCluster(numberOfNodes);
            var createResult = await CreateDatabaseInClusterInner(new DatabaseRecord(database), numberOfNodes, cluster.Leader.WebUrl, null);

            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Leader.WebUrl }
            }.Initialize())
            {

                var re = store.GetRequestExecutor(database);
                var result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.Equal(1, result);

                var preferred = await re.GetPreferredNode();
                var tag = preferred.Item2.ClusterTag;
                var server = createResult.Servers.Single(s => s.ServerStore.NodeTag == tag);
                server.ServerStore.InitializationCompleted.Reset(true);
                server.ServerStore.Initialized = false;
                server.ServerStore.Engine.CurrentLeader?.StepDown();

                var sp = Stopwatch.StartNew();
                result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.True(sp.Elapsed < TimeSpan.FromSeconds(10));
                var newPreferred = await re.GetPreferredNode();

                Assert.NotEqual(tag, newPreferred.Item2.ClusterTag);
                Assert.Equal(2, result);
            }
        }

        [Fact]
        public async Task PreferredNodeShouldBeRestoredAfterBroadcast()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "15";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "15";

            var database = GetDatabaseName();
            var numberOfNodes = 3;
            var cluster = await CreateRaftCluster(numberOfNodes);
            var createResult = await CreateDatabaseInClusterInner(new DatabaseRecord(database), numberOfNodes, cluster.Leader.WebUrl, null);

            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Leader.WebUrl }
            }.Initialize())
            {
                var re = store.GetRequestExecutor(database);
                var preferred = await re.GetPreferredNode();
                var tag = preferred.Item2.ClusterTag;

                var result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.Equal(1, result);

                preferred = await re.GetPreferredNode();
                Assert.Equal(tag, preferred.Item2.ClusterTag);

                var server = createResult.Servers.Single(s => s.ServerStore.NodeTag == tag);
                server.ServerStore.InitializationCompleted.Reset(true);
                server.ServerStore.Initialized = false;
                server.ServerStore.Engine.CurrentLeader?.StepDown();
                var sp = Stopwatch.StartNew();
                result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                sp.Stop();
                Assert.True(sp.Elapsed < TimeSpan.FromSeconds(10));

                var newPreferred = await re.GetPreferredNode();
                Assert.NotEqual(tag, newPreferred.Item2.ClusterTag);
                Assert.Equal(2, result);

                server.ServerStore.Initialized = true;

                var current = await WaitForValueAsync(async() =>
                {
                    var p = await re.GetPreferredNode();

                    return p.Item2.ClusterTag;
                }, tag);

                Assert.Equal(tag, current);
            }
        }

        [Fact]
        public async Task ChangesApiFailOver()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                var db = "ChangesApiFailOver_Test";
                var topology = new DatabaseTopology { DynamicNodesDistribution = true };
                var (clusterNodes, leader) = await CreateRaftCluster(3,
                    customSettings: new Dictionary<string, string>()
                    {
                        [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                        [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                        [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1"
                    });

                var (_, servers) = await CreateDatabaseInCluster(new DatabaseRecord { DatabaseName = db, Topology = topology }, 2, leader.WebUrl);

                using (var store = new DocumentStore { Database = db, Urls = new[] { leader.WebUrl } }.Initialize())
                {
                    var list = new BlockingCollection<DocumentChange>();
                    var taskObservable = store.Changes();
                    await taskObservable.EnsureConnectedNow().WithCancellation(cts.Token);
                    var observableWithTask = taskObservable.ForDocument("users/1");
                    observableWithTask.Subscribe(list.Add);
                    await observableWithTask.EnsureSubscribedNow().WithCancellation(cts.Token);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    Assert.True(await WaitForDocumentInClusterAsync<User>(servers, db, "users/1", null, TimeSpan.FromSeconds(30)));

                    var value = await WaitForValueAsync(() => list.Count, 1);
                    Assert.Equal(1, value);

                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db), cts.Token);
                    var firstTopology = record.Topology;

                    Assert.Equal(2, firstTopology.Members.Count);

                    var toDispose = clusterNodes.Single(n => n.ServerStore.NodeTag == firstTopology.Members[0]);
                    await DisposeServerAndWaitForFinishOfDisposalAsync(toDispose);

                    await taskObservable.EnsureConnectedNow().WithCancellation(cts.Token);

                    List<RavenServer> databaseServers = null;
                    Assert.True(await WaitForValueAsync(async () =>
                        {
                            var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db), cts.Token);
                            topology = databaseRecord.Topology;
                            databaseServers = clusterNodes.Where(s => topology.Members.Contains(s.ServerStore.NodeTag)).ToList();
      
                            if (topology.Rehabs.Count == 1 && databaseServers.Count == 2)
                                return true;

                            return false;
                        }, true, interval: 333));


                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    Assert.True(await WaitForChangeVectorInClusterAsync(databaseServers, db, 30_000), "WaitForChangeVectorInClusterAsync");

                    value = await WaitForValueAsync(() => list.Count, 2);
                    Assert.Equal(2, value);

                    toDispose = clusterNodes.Single(n => firstTopology.Members.Contains(n.ServerStore.NodeTag) == false && topology.Members.Contains(n.ServerStore.NodeTag));
                    await DisposeServerAndWaitForFinishOfDisposalAsync(toDispose);

                    await taskObservable.EnsureConnectedNow().WithCancellation(cts.Token);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/1");
                        session.SaveChanges();
                    }
       
                    value = await WaitForValueAsync(() => list.Count, 3);
                    Assert.Equal(3, value);
                }
            }
        }

        public static async Task ReverseOrderSuccessfully(IDocumentStore store, string db)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db));
            record.Topology.Members.Reverse();
            var copy = new List<string>(record.Topology.Members);
            await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(db, record.Topology.Members));
            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db));
            Assert.True(copy.All(record.Topology.Members.Contains));
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiShouldNotFailOverWhenWaitingForCompletionOfOperation()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                       {
                           Server = leader,
                           ReplicationFactor = 3,
                           ModifyDocumentStore = (documentStore => documentStore.Conventions.DisableTopologyUpdates = true) // so request executor stays on the same node
                       }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );
                    
                    var expectedNode = (await store.GetRequestExecutor().GetPreferredNode()).Node.ClusterTag;

                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(expectedNode, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;
                    
                    var op = await store.Operations.SendAsync(patch, token: cts.Token);
                    Assert.Equal(expectedNode, op.NodeTag);

                    var t = op.WaitForCompletionAsync(cts.Token);
                    
                    var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                    var changes = store.Changes(store.Database, op.NodeTag);
                    changes.ConnectionStatusChanged += (sender, args) => { waitWebSocketError.Set(); };

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //bring down server
                    var serverWithPatchOperation = clusterNodes.Single(x => x.ServerStore.NodeTag == expectedNode);
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(serverWithPatchOperation);

                    //wait for websocket to throw and retry
                    await waitWebSocketError.WaitAsync(cts.Token);
                    
                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    //run the operation again - should work
                    op = await store.Operations.SendAsync(patch, token: cts.Token);
                    
                    // we reconnect to the same node
                    Assert.Equal(expectedNode, op.NodeTag);
                    await op.WaitForCompletionAsync(cts.Token);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiForOperationShouldCleanUpFaultyConnection_AfterUnrecoverableError()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(1, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 1
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    
                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(leader.ServerStore.NodeTag, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);
                    
                    var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                    var changes = store.Changes(store.Database, op.NodeTag);

                    var t = op.WaitForCompletionAsync(cts.Token);

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));
                    
                    //error in changes api will release the mre
                    changes.ConnectionStatusChanged += (sender, args) => { waitWebSocketError.Set(); };

                    //the error will bubble up to user - later await it
                    var waitForCompletionErrorTask = Assert.ThrowsAsync<DatabaseDoesNotExistException>(async () => await t);

                    //delete the database, so when changes api tries reconnecting it will throw db does not exist exception
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));

                    // bring server down - so when it reconnects, api will realize db doesn't exist anymore
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                    //wait for the websocket failure
                    await waitWebSocketError.WaitAsync(cts.Token);
                    
                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    //wait for websocket to throw upon retry connecting - this should result in DatabaseChanges.Dispose - remove connection from the dict
                    await waitForCompletionErrorTask;

                    //create the db again
                    await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)));

                    //run the operation again - the connection should have been removed from the _databaseChanges dictionary and a new one created
                    op = await store.Operations.SendAsync(patch, token: cts.Token);

                    // we reconnect
                    await op.WaitForCompletionAsync(cts.Token);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiShouldNotThrowOnConnectionError()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(1, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 1
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    
                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(leader.ServerStore.NodeTag, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);
                    
                    var changes = store.Changes(store.Database, op.NodeTag);
                    
                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //this error should not bubble up to user
                    var waitForCompletionErrorTask = Assert.ThrowsAsync<WebSocketException>(async () => await op.WaitForCompletionAsync(cts.Token));

                    // bring server down
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    var resTask = op.WaitForCompletionAsync(cts.Token);

                    var task = await Task.WhenAny(waitForCompletionErrorTask, resTask);
                    Assert.Equal(task, resTask);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiShouldNotIndependentlyFailOverWhenNodeTagSpecified_OnlyRequestExecutorWillFailOver()
        {
            // request executor will failover after server is down and changes api will not attempt to failover the same connection, but instead will open a new connection for the new specific node
            // that is in order to avoid having a connection entry in _databaseChanges that contains a certain node as key but the connection inside has failed over to another node
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 3
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    var expectedNode = (await store.GetRequestExecutor().GetPreferredNode()).Node.ClusterTag;

                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(expectedNode, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);
                    Assert.Equal(expectedNode, op.NodeTag);

                    // set up mre for detecting websocket error
                    var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                    var changes = store.Changes(store.Database, op.NodeTag);
                    changes.ConnectionStatusChanged += (sender, args) => { waitWebSocketError.Set(); };

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //bring down server
                    var serverWithPatchOperation = clusterNodes.Single(x => x.ServerStore.NodeTag == expectedNode);
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(serverWithPatchOperation);

                    await waitWebSocketError.WaitAsync(cts.Token);
                    
                    // Run a request to make request executor fail over to a different node
                    await store.Maintenance.SendAsync(new GetStatisticsOperation(), token: cts.Token);

                    // Bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    var re = store.GetRequestExecutor(store.Database);
                    //wait for executor to have a different preferred node
                    await AssertWaitForTrueAsync(async () => (await re.GetPreferredNode()).Node.ClusterTag != expectedNode);

                    //run the operation again - should work
                    op = await store.Operations.SendAsync(patch, token: cts.Token);

                    // Will attempt the operation again on a different node this time
                    Assert.NotEqual(expectedNode, op.NodeTag);
                    
                    await op.WaitForCompletionAsync(cts.Token);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ChangesApiShouldCleanupFaultyConnectionAfterDispose_TrackingSpecificNode()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                var (_, leader) = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                       {
                           Server = leader,
                           ReplicationFactor = 3
                       }))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1");
                        await session.SaveChangesAsync();
                    }

                    for (int i = 0; i < 2; i++)
                    {
                        (string DataDirectory, string Url, string NodeTag) result = default;

                        //set up node-specific tracking
                        using IDatabaseChanges changes = store.Changes(store.Database, "A");

                        var list = new BlockingCollection<DocumentChange>();
                        
                        await changes.EnsureConnectedNow().WithCancellation(cts.Token);
                        var observableWithTask = changes.ForDocument("users/1");
                        observableWithTask.Subscribe(list.Add);
                        await observableWithTask.EnsureSubscribedNow().WithCancellation(cts.Token);

                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new User(), "users/1");
                            await session.SaveChangesAsync();
                        }

                        // check socket is connected
                        await AssertWaitForTrueAsync(() => Task.FromResult(list.Count == 1));

                        var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                        changes.ConnectionStatusChanged += (sender, args) =>
                        {
                            waitWebSocketError.Set();
                            throw new Exception("Test exception");
                        };

                        // first run, fail the websocket entirely
                        if (i == 0)
                        {

                            //bring down server
                            result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                            //wait for the exception following socket being closed
                            await waitWebSocketError.WaitAsync(cts.Token);

                            var ex = await Assert.ThrowsAsync<WebSocketException>(async () =>
                                await changes.EnsureConnectedNow().WithCancellation(cts.Token));
                            
                            // Bring server back up
                            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                            var server = GetNewServer(new ServerCreationOptions
                            {
                                RunInMemory = false,
                                DeletePrevious = false,
                                DataDirectory = result.DataDirectory,
                                CustomSettings = settings,
                                NodeTag = result.NodeTag
                            });
                            Servers.Add(server);

                            //connection is now faulty and will be broken when we try to access
                            var changes2 = store.Changes(store.Database, "A");
                            await Assert.ThrowsAsync<WebSocketException>(async () => await changes2.EnsureConnectedNow().WithCancellation(cts.Token));
                        }
                    }
                }
            }
        }
    }
}
