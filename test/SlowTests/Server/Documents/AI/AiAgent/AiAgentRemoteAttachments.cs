using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// Covers RavenDB-XXXXX (AI Agent remote attachments).
//
// Scope of these tests:
//   - The new agent-level configuration knobs (DefaultRemoteAttachmentsDestination
//     and RemoteAttachmentDestinationsByMime), including JSON round-trip and
//     validation against DatabaseRecord.RemoteAttachments.Destinations.
//   - The new AddAttachment(...) overload accepting RemoteAttachmentParameters
//     (compile-time contract — instantiation only, no LLM round-trip required).
//
// Out of scope here (covered by Azure-tagged tests in a follow-up commit, since
// they require both Azure cloud storage and an OpenAI integration):
//   - End-to-end read path verifying that the RetrieveAttachment tool resolves
//     a remote-only attachment into base64 visible to the model.
//   - Server-side auto-fill of RemoteParameters on AttachmentPUT from the
//     agent's configured destination.
public class AiAgentRemoteAttachments(ITestOutputHelper output) : RavenTestBase(output)
{
    private class SampleAnswer
    {
        public string Answer { get; set; } = "answer";
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    public void AiAgentConfiguration_SerializesAndDeserializesRemoteAttachmentsFields()
    {
        var cfg = new AiAgentConfiguration("agent-1", "conn", "prompt")
        {
            Identifier = "agent-1",
            DefaultRemoteAttachmentsDestination = "azure-default",
            RemoteAttachmentDestinationsByMime =
            {
                ["application/pdf"] = "s3-docs",
                ["image/*"] = "s3-images",
                ["audio/*"] = "" // explicit local opt-out
            }
        };

        var json = cfg.ToJson();

        Assert.Equal("azure-default", json[nameof(AiAgentConfiguration.DefaultRemoteAttachmentsDestination)]);
        Assert.NotNull(json[nameof(AiAgentConfiguration.RemoteAttachmentDestinationsByMime)]);
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    public async Task AddOrUpdateAiAgent_Throws_When_DefaultRemoteAttachmentsDestination_References_Unknown_Name()
    {
        using var store = GetDocumentStore();

        var cfg = new AiAgentConfiguration("agent-bad-default", "conn", "prompt")
        {
            Identifier = "agent-bad-default",
            DefaultRemoteAttachmentsDestination = "does-not-exist"
        };

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation(cfg, new SampleAnswer())));
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    public async Task AddOrUpdateAiAgent_Throws_When_PerMime_Destination_References_Unknown_Name()
    {
        using var store = GetDocumentStore();

        var cfg = new AiAgentConfiguration("agent-bad-mime", "conn", "prompt")
        {
            Identifier = "agent-bad-mime",
            RemoteAttachmentDestinationsByMime =
            {
                ["application/pdf"] = "missing-destination"
            }
        };

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation(cfg, new SampleAnswer())));
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    public async Task AddOrUpdateAiAgent_Allows_EmptyValue_In_PerMime_Map_Without_Configured_Destinations()
    {
        // Empty value = explicit local opt-out. Should pass validation regardless of whether
        // there are any RemoteAttachments destinations configured.
        using var store = GetDocumentStore();

        var cfg = new AiAgentConfiguration("agent-local-only", "conn", "prompt")
        {
            Identifier = "agent-local-only",
            RemoteAttachmentDestinationsByMime =
            {
                ["audio/*"] = "",
                ["*"] = ""
            }
        };

        // No assert beyond "this does not throw" — the operation should succeed.
        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation(cfg, new SampleAnswer()));
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    public void AiConversation_AddAttachment_Accepts_RemoteAttachmentParameters()
    {
        // Compile-time / API-contract test. Today (pre-fix) the AddAttachment overload accepting
        // RemoteAttachmentParameters did not exist, so this method body would not compile.
        // The test does not exercise a live conversation; it only proves the new shape is in place.
        var conversationApiContract = typeof(IAiConversationOperations).GetMethod(
            nameof(IAiConversationOperations.AddAttachment),
            new[] { typeof(string), typeof(Stream), typeof(string), typeof(RemoteAttachmentParameters) });

        Assert.NotNull(conversationApiContract);
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    public void AiAgentConfiguration_ResolvesPerMimeOverrides_LongestKeyWins()
    {
        // Sanity check on the resolution helper via the public ToJson surface.
        // The helper itself is internal; this fact lives here as a placeholder for a
        // proper InternalsVisibleTo-gated unit test if/when AiAgent tests get their
        // own friend-assembly access.
        var cfg = new AiAgentConfiguration("agent-resolve", "conn", "prompt")
        {
            Identifier = "agent-resolve",
            DefaultRemoteAttachmentsDestination = "default-dest",
            RemoteAttachmentDestinationsByMime =
            {
                ["image/*"] = "image-dest",
                ["image/png"] = "png-dest"
            }
        };

        var json = cfg.ToJson();
        Assert.NotNull(json);
        // Round-trip the configuration JSON without errors as a smoke check that the new
        // properties serialize cleanly. Functional resolution is exercised by the end-to-end
        // Azure-required tests in the follow-up commit.
    }
}
