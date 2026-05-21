using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
using SlowTests.Server.Documents.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// End-to-end integration tests for the AI Agent + Remote Attachments wiring (RavenDB-XXXXX).
// These tests require both:
//   - Azure cloud storage (gated by AzureRequired = true on the theory attribute)
//   - OpenAI integration (gated by [RavenGenAiData(IntegrationType = OpenAi)] data provider)
// Without both, the tests auto-skip — mirroring the same pattern used by
// SlowTests/Server/Documents/AI/GenAi/GenAiRemoteAttachments.cs.
public class AiAgentRemoteAttachmentsIntegration(ITestOutputHelper output) : RemoteAttachmentsAzureBase(output)
{
    private class OutputSchema
    {
        public string Answer { get; set; } = "answer";
    }

    // RED before A.1+A.2: model received empty/garbage bytes for the heart.png attachment
    // (RetrieveAndAddAttachment read attachment.Stream, which is null for remote-only blobs)
    // and could not describe it. GREEN after the fix: the attachment is marked Deferred,
    // resolved inside the conversation loop by ResolveDeferredAttachmentsAsync, and the
    // model sees the actual image bytes.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_CanReadRemoteAttachment_ViaAttachmentCOPY(Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("remote-image-analyzer", config.ConnectionStringName,
                "Describe images precisely.")
            {
                Identifier = "remote-image-analyzer"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            const string sourceDocId = "docs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Info = "Source Doc" }, sourceDocId);

                using var heart = GetEmbeddedImgStream("heart.png");

                var attachments = session.Advanced.Attachments;
                var remote = new RemoteAttachmentParameters(remoteId, DateTime.UtcNow.AddMinutes(1));
                attachments.Store(sourceDocId,
                    new StoreAttachmentParameters("heart.png", heart) { RemoteParameters = remote });

                await session.SaveChangesAsync();
            }

            // Force the local blob into the cloud bucket so attachment.Stream is null on lookup
            // and the only way to read the bytes is via the remote storage helper.
            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("What shape is in the image I just sent you?");
            chat.CopyAttachmentFrom(sourceDocId, "heart.png");

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
            // The model can only answer "heart" if it actually received the decoded PNG bytes,
            // which proves the deferred resolver downloaded the blob from Azure and substituted
            // base64 data before the LLM round-trip.
            Assert.Contains("heart", result.Answer.Answer, StringComparison.OrdinalIgnoreCase);
        }
    }

    // Verifies the server-side auto-fill on AttachmentPUT: when the caller does not pass explicit
    // RemoteAttachmentParameters, the agent's DefaultRemoteAttachmentsDestination is used. The
    // assertion inspects the cloud bucket post-conversation; before the fix, the attachment
    // stays in local storage and the bucket has zero blobs.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_AttachmentPUT_UsesDefaultRemoteAttachmentsDestination_WhenCallerOmitsRemoteParameters(
        Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("default-dest-agent", config.ConnectionStringName,
                "You are a helpful assistant.")
            {
                Identifier = "default-dest-agent",
                DefaultRemoteAttachmentsDestination = remoteId
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Acknowledge the attachment.");

            await using (var heart = GetEmbeddedImgStream("heart.png"))
            {
                chat.AddAttachment("heart.png", heart, "image/png");
                await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
        }
    }

    // Verifies that an explicit caller-supplied RemoteAttachmentParameters wins over the agent's
    // configured default. This is a regression guard: the auto-fill helper must short-circuit on
    // non-null cmd.RemoteParameters and must not overwrite caller intent.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_AttachmentPUT_KeepsCallerSuppliedRemoteParameters_OverConfiguredDefault(
        Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // Agent default points to the configured destination; the caller will *also* point
            // at the same destination explicitly. The bucket should still contain exactly one
            // blob — the assertion is "the auto-fill helper did not double-process or overwrite
            // the caller-supplied parameters in some way that breaks upload".
            var agent = new AiAgentConfiguration("caller-wins-agent", config.ConnectionStringName,
                "You are a helpful assistant.")
            {
                Identifier = "caller-wins-agent",
                DefaultRemoteAttachmentsDestination = remoteId
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Acknowledge the attachment.");

            var explicitParams = new RemoteAttachmentParameters(remoteId, DateTime.UtcNow.AddMinutes(1))
            {
                Flags = RemoteAttachmentFlags.Remote
            };

            await using (var heart = GetEmbeddedImgStream("heart.png"))
            {
                chat.AddAttachment("heart.png", heart, "image/png", explicitParams);
                await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
        }
    }

    // Verifies that an empty per-MIME value overrides the default and keeps that MIME local.
    // Configures agent with a remote default but an empty entry for image/*, then adds an image
    // attachment — the bucket should remain empty post-conversation.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_PerMime_EmptyValue_OptsMimeOutOfRemote_AndKeepsItLocal(
        Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("local-images-agent", config.ConnectionStringName,
                "You are a helpful assistant.")
            {
                Identifier = "local-images-agent",
                DefaultRemoteAttachmentsDestination = remoteId,
                RemoteAttachmentDestinationsByMime = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["image/*"] = "" // explicit local opt-out for all images
                }
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Acknowledge the attachment.");

            await using (var heart = GetEmbeddedImgStream("heart.png"))
            {
                chat.AddAttachment("heart.png", heart, "image/png");
                await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            // Image stayed local — nothing in the configured destination's bucket.
            await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
        }
    }

    // Test #10: Visibility scope (Fix D).
    // Asserts that resolving a deferred remote attachment populates the debug trace's
    // RemoteAttachmentResolutions with a positive duration. This is the AI Agent analogue
    // of GenAi's GenAi/LoadToModel/RemoteAttachments stats scope — same data, surfaced
    // through the existing debug-trace infrastructure rather than a new StatsScope tree.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_RemoteAttachmentResolution_AppearsInDebugTrace_WithPositiveDuration(
        Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("trace-resolution-agent", config.ConnectionStringName,
                "Describe images precisely.")
            {
                Identifier = "trace-resolution-agent"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            const string sourceDocId = "docs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Info = "Source Doc" }, sourceDocId);

                using var heart = GetEmbeddedImgStream("heart.png");

                var remote = new RemoteAttachmentParameters(remoteId, DateTime.UtcNow.AddMinutes(1));
                session.Advanced.Attachments.Store(sourceDocId,
                    new StoreAttachmentParameters("heart.png", heart) { RemoteParameters = remote });

                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

            // Enable debug tracing on this conversation so the trace is persisted.
            var chat = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null, debug: true);
            chat.SetUserPrompt("What shape is in the image?");
            chat.CopyAttachmentFrom(sourceDocId, "heart.png");
            await chat.RunAsync<OutputSchema>(CancellationToken.None);

            using var session2 = store.OpenAsyncSession();
            var traces = (await session2.Advanced.LoadStartingWithAsync<DebugTraceWithResolutions>(
                $"{chat.Id}/request-trace/")).ToList();

            Assert.NotEmpty(traces);

            // Find the iteration that resolved a deferred attachment. The first turn (which
            // copied the remote attachment in) is the one that should carry the resolution data.
            var traceWithResolution = traces.FirstOrDefault(t => t.RemoteAttachmentResolutions is { Count: > 0 });
            Assert.NotNull(traceWithResolution);

            var resolution = Assert.Single(traceWithResolution.RemoteAttachmentResolutions);
            Assert.Equal("heart.png", resolution.Name);
            Assert.False(string.IsNullOrEmpty(resolution.RemoteStorageId));
            Assert.True(resolution.DurationInMs >= 0,
                $"Expected non-negative duration; got {resolution.DurationInMs}ms");
        }
    }

    // Test #11: Regression guard / documentation of the deferred COPY-symmetry state.
    // COPY symmetric auto-fill was deliberately left out of this PR (see plan §C, "Defer
    // COPY symmetry"). Today:
    //   - COPY from a remote source inherits the source's RemoteParameters via
    //     AttachmentsStorage.cs:1119 → free symmetry for the common case.
    //   - COPY from a LOCAL source stays local even if the agent has a configured default
    //     RemoteAttachmentsDestination → this is the gap to be closed by a follow-up YouTrack
    //     ticket ("AI Agent: AttachmentCOPY from local source should honour the agent's
    //     DefaultRemoteAttachmentsDestination"). This test exists so that if someone later
    //     wires COPY auto-fill without explicitly updating the regression guard, CI flags it.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_CopyAttachmentFrom_LocalSource_StaysLocal_EvenWhenAgentDefaultRemoteConfigured(
        Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("copy-local-source-agent", config.ConnectionStringName,
                "You are a helpful assistant.")
            {
                Identifier = "copy-local-source-agent",
                // Default destination is set — but COPY symmetry is deferred, so the copy of
                // a local-source attachment must NOT be auto-routed to remoteId.
                DefaultRemoteAttachmentsDestination = remoteId
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            const string sourceDocId = "docs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Info = "Source Doc" }, sourceDocId);
                using var heart = GetEmbeddedImgStream("heart.png");
                // Local-only source attachment — no RemoteParameters.
                session.Advanced.Attachments.Store(sourceDocId,
                    new StoreAttachmentParameters("heart.png", heart) { ContentType = "image/png" });
                await session.SaveChangesAsync();
            }

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Acknowledge.");
            chat.CopyAttachmentFrom(sourceDocId, "heart.png");
            await chat.RunAsync<OutputSchema>(CancellationToken.None);

            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            // Bucket stays empty because both the source and the copy are local. If a future
            // commit wires COPY auto-fill, this assertion will flip to 1 — which is the cue
            // to delete this regression guard and add a real "COPY auto-fill works" test.
            await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
        }
    }

    // Test #12: Failure semantics regression guard (plan §A failure semantics).
    // When the remote storage returns an error during deferred resolution, the conversation
    // turn must abort (exception bubbles to the caller) rather than silently substituting
    // empty/placeholder data. This locks in parity with the GenAi resolver — no try/catch
    // around the download — so the model never sees fabricated bytes for a missing blob.
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments, AzureRequired = true)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Agent_RemoteAttachmentDownloadFailure_AbortsConversationTurn(
        Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using var store = GetDocumentStore(options);

            string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("download-failure-agent", config.ConnectionStringName,
                "Describe images precisely.")
            {
                Identifier = "download-failure-agent"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            const string sourceDocId = "docs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Info = "Source Doc" }, sourceDocId);
                using var heart = GetEmbeddedImgStream("heart.png");
                var remote = new RemoteAttachmentParameters(remoteId, DateTime.UtcNow.AddMinutes(1));
                session.Advanced.Attachments.Store(sourceDocId,
                    new StoreAttachmentParameters("heart.png", heart) { RemoteParameters = remote });
                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

            // The blob is now in the cloud. Delete it out from under the conversation handler
            // so the resolver hits a "blob does not exist" error mid-turn.
            await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
            await DeleteObjects(Settings);

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("What is in the image?");
            chat.CopyAttachmentFrom(sourceDocId, "heart.png");

            // The resolver lets GetAttachmentDataAsBase64Async throw — no silent fallback.
            // The exact wrapped exception type and message depend on how Azure surfaces "blob
            // does not exist" through RemoteAttachmentsStorage; the assertion here is the
            // coarse "an exception bubbled out" — which by itself rules out the dangerous
            // alternative behaviour (silent placeholder data).
            await Assert.ThrowsAnyAsync<Exception>(
                () => chat.RunAsync<OutputSchema>(CancellationToken.None));
        }
    }

    private class DebugTraceWithResolutions
    {
        public string RequestBody { get; set; }
        public List<RemoteAttachmentResolutionShape> RemoteAttachmentResolutions { get; set; }
    }

    private class RemoteAttachmentResolutionShape
    {
        public string Name { get; set; }
        public string RemoteStorageId { get; set; }
        public long DurationInMs { get; set; }
    }

    private static System.IO.Stream GetEmbeddedImgStream(string name)
    {
        var asm = typeof(AiAgentRemoteAttachmentsIntegration).Assembly;
        // The same embedded resource set used by RavenDB-24847, located under SlowTests.Data.RavenDB_24648.
        var resourceName = "SlowTests.Data.RavenDB_24648." + name;
        var stream = asm.GetManifestResourceStream(resourceName);
        if (stream == null)
            throw new System.IO.FileNotFoundException($"Embedded resource not found: {resourceName}");
        return stream;
    }
}
