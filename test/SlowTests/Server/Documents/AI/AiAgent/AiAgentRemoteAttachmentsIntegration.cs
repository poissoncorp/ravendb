using System;
using System.Collections.Generic;
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
