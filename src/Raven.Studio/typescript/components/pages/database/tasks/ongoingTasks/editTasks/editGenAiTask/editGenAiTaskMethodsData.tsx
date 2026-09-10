import React from "react";
import RichAlert from "components/common/RichAlert";
import { MethodEntry, MethodGroup } from "components/common/samples/partials/samplesTypes";
import { dedent } from "components/utils/dedent";

type GenAiScriptScope = "context" | "update";

interface GenAiMethodEntry extends MethodEntry {
    scope?: GenAiScriptScope;
}

interface GenAiMethodGroup {
    category: string;
    methods: GenAiMethodEntry[];
}

/**
 * Methods available in the JavaScript editors of a GenAI task.
 *
 * The list is shared by both script editors in the task wizard:
 *  - "Generate context objects" (the context generation script)
 *  - "Provide update script"    (the update script)
 *
 * The two scripts do NOT run in identical engines.
 * Set `scope` for methods limited to one editor; omit it only for methods available in both.
 * The scope sentence in the description is user-facing copy and must stay in sync with `scope`.
 *
 *  - The context generation script runs in the GenAI ETL transformer (GenAiScriptTransformer).
 *    Its runner is created with `readOnly: true`, so `put`/`del` throw "Cannot make modifications in readonly context".
 *    It is also the only script that gets the `ai` API and the source-document attachment helpers (`loadAttachment`, `hasAttachment`, `getAttachments`).
 *
 *  - The update script runs in a patch runner (PatchRequestType.GenAi) with `readOnly: false`.
 *    It can modify the current document and use `put`/`del`, and it gets the `$input` / `$output` arguments.
 *    It has no `ai` API.
 *
 * Counter, time-series and attachment mutations, and `archived.unarchive`, are NOT usable in a GenAI task.
 * They are still registered in the engine, so they neither throw nor no-op cleanly: `incrementCounter` writes the
 * counter value to storage while the document's `@counters` metadata stays untouched, `archived.unarchive` strips the
 * `@archived` marker while the Archived document flag stays set, and so on. The bookkeeping they rely on lives in
 * `PatchDocumentCommand.AddResolveFlagOrUpdateRelatedDocuments`, which the production update path
 * (`GenAiBatchPatchCommand`) never runs. Rather than omit them silently, they are listed as `isUnavailable` rows so a
 * user who reaches for them finds out why instead of shipping a half-written document.
 *
 * The same applies to the ETL-only entry points (`loadTo`, `loadCounter`, `loadTimeSeries`): they exist in the context
 * script's engine because it is an ETL transformer, but `GenAiScriptTransformer` overrides them to throw.
 */
const genAiMethodGroups: GenAiMethodGroup[] = [
    {
        category: "Context objects",
        methods: [
            {
                signature: "ai.genContext(ctx)",
                returnType: "AIContextItem",
                scope: "context",
                keywords: ["required", "emit", "mandatory"],
                description: (
                    <>
                        Emits one context object, built from the <code>ctx</code> object you pass in. Each context
                        object is sent to the model as a separate request, so call this once per item you want the model
                        to reason about. <code>ctx</code> must be a plain object: an array is rejected outright, and{" "}
                        <code>null</code> fails later with an unhelpful error, so pass an object even when it is empty.
                        Returns the context item, so attachments can be chained onto it. The script is{" "}
                        <strong>required</strong> to call this at least once, otherwise saving the task fails with{" "}
                        <em>&quot;You must call the ai.genContext(ctx) function in your script&quot;</em>.{" "}
                        <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Emit one context object per comment on the source document.
                    for (const comment of this.Comments) {
                        ai.genContext({
                            Text: \`Blog post topic: \${this.Topic}. Comment: \${comment.Text}\`,
                            AuthorName: comment.Author,
                            CommentId: comment.Id
                        });
                    }
                `,
            },
            {
                signature: "AIContextItem.withText(data)",
                returnType: "AIContextItem",
                keywords: ["attachment", "text", "plain"],
                scope: "context",
                description: (
                    <>
                        Attaches <code>data</code> to the context object as <code>text/plain</code>. Returns the context
                        item, so calls can be chained. <code>data</code> must be a string or <code>null</code>.{" "}
                        <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Send the comment together with a text attachment from the source document.
                    for (const comment of this.Comments) {
                        ai.genContext({ CommentId: comment.Id, Text: comment.Text })
                            .withText(loadAttachment("transcript.txt"));
                    }
                `,
            },
            {
                signature: "AIContextItem.withPng(data)",
                returnType: "AIContextItem",
                keywords: ["attachment", "image", "vision", "base64"],
                scope: "context",
                description: (
                    <>
                        Attaches <code>data</code> as <code>image/png</code>. Use an attachment reference returned by{" "}
                        <code>loadAttachment()</code>, a Base64-encoded string, or <code>null</code>. Use it to send an
                        image to a vision-capable model. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Ask the model to look at an image attached to the source document.
                    ai.genContext({ Topic: this.Topic })
                        .withPng(loadAttachment("screenshot.png"));
                `,
            },
            {
                signature: "AIContextItem.withJpeg(data)",
                returnType: "AIContextItem",
                keywords: ["attachment", "image", "vision", "base64"],
                scope: "context",
                description: (
                    <>
                        Attaches <code>data</code> as <code>image/jpeg</code>. A string supplied directly must be
                        Base64-encoded. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({ ProductId: id(this) })
                        .withJpeg(loadAttachment("product-photo.jpg"));
                `,
            },
            {
                signature: "AIContextItem.withWebp(data)",
                returnType: "AIContextItem",
                keywords: ["attachment", "image", "vision", "base64"],
                scope: "context",
                description: (
                    <>
                        Attaches <code>data</code> as <code>image/webp</code>. A string supplied directly must be
                        Base64-encoded. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({ ProductId: id(this) })
                        .withWebp(loadAttachment("product-photo.webp"));
                `,
            },
            {
                signature: "AIContextItem.withGif(data)",
                returnType: "AIContextItem",
                keywords: ["attachment", "image", "base64"],
                scope: "context",
                description: (
                    <>
                        Attaches <code>data</code> as <code>image/gif</code>. A string supplied directly must be
                        Base64-encoded. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({ ProductId: id(this) })
                        .withGif(loadAttachment("animation.gif"));
                `,
            },
            {
                signature: "AIContextItem.withPdf(data)",
                returnType: "AIContextItem",
                keywords: ["attachment", "document", "base64"],
                scope: "context",
                description: (
                    <>
                        Attaches <code>data</code> as <code>application/pdf</code>. A string supplied directly must be
                        Base64-encoded. Useful for sending a whole document to the model instead of extracting its text
                        first. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Send an invoice PDF stored as a document attachment.
                    ai.genContext({ OrderId: id(this) })
                        .withPdf(loadAttachment("invoice.pdf"));
                `,
            },
        ],
    },
    {
        category: "Script input",
        methods: [
            {
                signature: "this",
                returnType: "object",
                keywords: ["source document", "current"],
                description: (
                    <>
                        The source document. In the context generation script it is the document the context objects are
                        built from; in the update script it is the document being modified, and any change you make to
                        it is persisted.
                    </>
                ),
                sampleScript: dedent`
                    // Read fields straight off the source document.
                    output("Topic: " + this.Topic + ", comments: " + this.Comments.length);
                `,
            },
            {
                signature: "$input",
                returnType: "object",
                keywords: ["context object", "correlate"],
                scope: "update",
                description: (
                    <>
                        The context object that produced this model response &mdash; exactly what was passed to{" "}
                        <code>ai.genContext()</code>. Use it to correlate the response back to the part of the document
                        it came from. <strong>Update script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Find the comment this response is about, using the id we put on the context object.
                    const idx = this.Comments.findIndex(comment => comment.Id == $input.CommentId);
                `,
            },
            {
                signature: "$output",
                returnType: "object",
                keywords: ["model response", "schema"],
                scope: "update",
                description: (
                    <>
                        The model response, already parsed into an object matching the JSON schema defined in the
                        previous step. <strong>Update script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Act on the model's verdict.
                    if ($output.IsCommentSpam) {
                        this.SpamReason = $output.Reason;
                    }
                `,
            },
        ],
    },
    {
        category: "Source document attachments",
        methods: [
            {
                signature: "loadAttachment(name)",
                returnType: "attachment reference | null",
                scope: "context",
                keywords: ["missing", "null", "optional"],
                description: (
                    <>
                        Loads an attachment of the source document so it can be passed to one of the{" "}
                        <code>AIContextItem</code> attachment methods. A missing attachment does not throw: in a GenAI
                        task it comes back as <code>null</code>, and the <code>withText()</code> /{" "}
                        <code>withPng()</code> family accepts <code>null</code>, so an optional attachment can be
                        chained without any guard. Test the result yourself, or call <code>hasAttachment()</code>, only
                        when the script has to behave differently for a document without one.{" "}
                        <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // A missing attachment yields null, which withPng() accepts - no guard needed.
                    ai.genContext({ Id: id(this) })
                        .withPng(loadAttachment("heart.png"));

                    // Branch explicitly only when the two cases need different context objects.
                    const transcript = loadAttachment("transcript.txt");
                    if (transcript === null) {
                        ai.genContext({ Id: id(this), Note: "No transcript available" });
                    }
                `,
            },
            {
                signature: "hasAttachment(name)",
                returnType: "boolean",
                keywords: ["attachment", "case-insensitive", "exists"],
                scope: "context",
                description: (
                    <>
                        Returns whether the source document has an attachment with this name. The comparison is
                        case-insensitive. Not needed just to avoid an error, because <code>loadAttachment()</code>{" "}
                        already returns <code>null</code> for a missing attachment.{" "}
                        <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({
                        Topic: this.Topic,
                        HasScreenshot: hasAttachment("screenshot.png")
                    });
                `,
            },
            {
                signature: "getAttachments()",
                returnType: "object[]",
                keywords: ["attachment", "metadata", "content type", "size"],
                scope: "context",
                description: (
                    <>
                        Returns the source document&apos;s attachment metadata &mdash; <code>Name</code>,{" "}
                        <code>ContentType</code>, <code>Hash</code>, <code>Size</code> &mdash; or an empty array when
                        there are none. Takes no arguments. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    // Send every PNG attached to the document as its own context object.
                    for (const attachment of getAttachments()) {
                        if (attachment.ContentType == "image/png") {
                            ai.genContext({ Name: attachment.Name })
                                .withPng(loadAttachment(attachment.Name));
                        }
                    }
                `,
            },
            {
                signature: "getRevisionsCount()",
                returnType: "number",
                keywords: ["revisions", "history"],
                scope: "context",
                description: (
                    <>
                        Returns how many revisions the source document has, or <code>0</code> when revisions are not
                        enabled for it. Takes no arguments. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({
                        Topic: this.Topic,
                        TimesEdited: getRevisionsCount()
                    });
                `,
            },
        ],
    },
    {
        category: "Document operations",
        methods: [
            {
                signature: "id(document)",
                returnType: "string",
                keywords: ["identifier"],
                description: (
                    <>
                        Returns the ID of a document object. Use <code>id(this)</code> for the source document, or pass
                        any document variable available in the script, such as a document returned by{" "}
                        <code>load()</code>.
                    </>
                ),
                sampleScript: `output("Document ID: " + id(this));`,
            },
            {
                signature: "getMetadata(document) / metadataFor(document)",
                returnType: "object",
                keywords: ["metadata", "collection", "last-modified"],
                description: (
                    <>
                        Returns the document&apos;s metadata, e.g. <code>@id</code>, <code>@collection</code>,{" "}
                        <code>@last-modified</code>.
                    </>
                ),
                sampleScript: `output("Collection: " + getMetadata(this)["@collection"]);`,
            },
            {
                signature: "lastModified(document)",
                returnType: "number | undefined",
                keywords: ["utc", "epoch", "timestamp"],
                description: (
                    <>
                        Returns the document&apos;s last modification time as JavaScript milliseconds since the Unix
                        epoch (UTC), or <code>undefined</code> for a document that has none yet.
                    </>
                ),
                sampleScript: `output("Last modified: " + lastModified(this));`,
            },
            {
                signature: "load(documentId)",
                returnType: "object | object[] | undefined",
                keywords: ["related document", "include"],
                description: (
                    <>
                        Loads a related document by ID so you can include its fields. Pass an array of IDs to load
                        several at once. Returns <code>undefined</code> when the document does not exist.
                    </>
                ),
                sampleScript: dedent`
                    // Load a related document and inspect one of its fields.
                    const author = load(this.AuthorId);
                    output("Author: " + (author ? author.Name : "unknown"));
                `,
            },
            {
                signature: "loadPath(document, pathString)",
                returnType: "object | object[] | null",
                keywords: ["related document", "path"],
                description: (
                    <>
                        Loads the document (or documents) referenced by a path inside another document, e.g.{" "}
                        <code>&quot;Comments[].AuthorId&quot;</code>.
                    </>
                ),
                sampleScript: dedent`
                    // Load every author referenced by the comments in one call.
                    const authors = loadPath(this, "Comments[].AuthorId") || [];
                    output("Authors: " + authors.length);
                `,
            },
            {
                signature: "cmpxchg(compareExchangeKey)",
                returnType: "any",
                keywords: ["compare exchange", "cluster"],
                description: (
                    <>
                        Returns the value stored under a compare-exchange key, or <code>null</code>.
                    </>
                ),
                sampleScript: dedent`
                    const policy = cmpxchg("policies/moderation");
                    output("Moderation policy: " + JSON.stringify(policy));
                `,
            },
            {
                signature: "put(id, document[, changeVector])",
                returnType: "string",
                keywords: ["create", "write", "new document"],
                scope: "update",
                description: (
                    <>
                        Creates or overwrites a document and returns its ID. Pass an ID ending in <code>/</code> to get
                        a server-generated identifier. Use it for <em>other</em> documents: the task writes the source
                        document itself at the end of the run, so a <code>put()</code> aimed at <code>id(this)</code> is
                        overwritten. To change the source document, assign to <code>this</code> instead.{" "}
                        <strong>Update script only</strong> &mdash; the context generation script runs read-only and
                        this throws <em>&quot;Cannot make modifications in readonly context&quot;</em>.
                    </>
                ),
                sampleScript: dedent`
                    // Archive the spam comment as its own document.
                    if ($output.IsCommentSpam) {
                        put(id(this) + "/spam/", {
                            Comment: $input.Text,
                            Reason: $output.Reason,
                            "@metadata": { "@collection": "SpamComments" }
                        });
                    }
                `,
            },
            {
                signature: "del(documentId[, changeVector])",
                returnType: "boolean",
                keywords: ["delete", "remove"],
                scope: "update",
                description: (
                    <>
                        Deletes a document and returns whether it existed. Use it for <em>other</em> documents: the task
                        writes the source document back at the end of the run, so deleting <code>id(this)</code> here
                        does not stick. <strong>Update script only</strong> &mdash; the context generation script runs
                        read-only and this throws <em>&quot;Cannot make modifications in readonly context&quot;</em>.
                    </>
                ),
                sampleScript: dedent`
                    if ($output.IsCommentSpam) {
                        del("drafts/" + $input.CommentId);
                    }
                `,
            },
            {
                signature: "archived.archiveAt(document, utcDateString)",
                returnType: "void",
                scope: "update",
                keywords: ["archive", "utc"],
                description: (
                    <>
                        Schedules the document to be archived at the given UTC time by writing <code>@archive-at</code>{" "}
                        into its metadata. A document that is already archived is left alone. Unlike <code>put()</code>{" "}
                        and <code>del()</code> this one does not throw in the context generation script, it simply has
                        no effect there, because that script&apos;s result is never persisted.{" "}
                        <strong>Update script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    if ($output.IsCommentSpam) {
                        archived.archiveAt(this, "2026-12-31T00:00:00.000Z");
                    }
                `,
            },
        ],
    },
    {
        category: "Counter operations (read-only in GenAI scripts)",
        methods: [
            {
                signature: "getCounters()",
                returnType: "string[]",
                keywords: ["counter", "metadata"],
                scope: "context",
                description: (
                    <>
                        Returns the names of the source document&apos;s counters, read from its metadata, or an empty
                        array when there are none. Takes no arguments. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({
                        Topic: this.Topic,
                        Counters: getCounters().join(", ")
                    });
                `,
            },
            {
                signature: "hasCounter(name)",
                returnType: "boolean",
                keywords: ["counter", "case-insensitive", "exists"],
                scope: "context",
                description: (
                    <>
                        Returns whether the source document has a counter with this name. The comparison is
                        case-insensitive. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({
                        Topic: this.Topic,
                        WasReported: hasCounter("Reports")
                    });
                `,
            },
            {
                signature: "counter(document/documentId, name)",
                returnType: "number | null",
                keywords: ["counter", "value"],
                description: (
                    <>
                        Returns a counter&apos;s value, or <code>null</code> when it does not exist. Available in both
                        scripts.
                    </>
                ),
                sampleScript: dedent`
                    const reportedCount = counter(this, "Reports");
                    output("Reports: " + reportedCount);
                `,
            },
            {
                signature: "counterRaw(document/documentId, name)",
                returnType: "object",
                keywords: ["counter", "per node"],
                description: (
                    <>
                        Returns the counter&apos;s per-node values rather than the aggregated total. Available in both
                        scripts.
                    </>
                ),
                sampleScript: dedent`
                    const reportsPerNode = counterRaw(this, "Reports");
                    output(reportsPerNode);
                `,
            },
        ],
    },
    {
        category: "Time series (read-only in GenAI scripts)",
        methods: [
            {
                signature: "getTimeSeries()",
                returnType: "string[] | false",
                keywords: ["time series", "metadata"],
                scope: "context",
                description: (
                    <>
                        Returns the names of the source document&apos;s time series, read from its metadata. Returns{" "}
                        <code>false</code> when the document has no time series. Takes no arguments.{" "}
                        <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    const series = getTimeSeries() || [];
                    ai.genContext({
                        Topic: this.Topic,
                        Series: series.join(", ")
                    });
                `,
            },
            {
                signature: "hasTimeSeries(timeSeriesName)",
                returnType: "boolean",
                keywords: ["time series", "case-insensitive", "exists"],
                scope: "context",
                description: (
                    <>
                        Returns whether the source document has a time series with this name. The comparison is
                        case-insensitive. <strong>Context generation script only.</strong>
                    </>
                ),
                sampleScript: dedent`
                    ai.genContext({
                        Topic: this.Topic,
                        HasScores: hasTimeSeries("SpamScores")
                    });
                `,
            },
            {
                signature: "timeseries(document/documentId, name).get() / .get(from, to)",
                returnType: "object[]",
                keywords: ["time series", "range", "entries"],
                description: (
                    <>Returns time series entries, optionally limited to a range. Available in both scripts.</>
                ),
                sampleScript: dedent`
                    const scores = timeseries(this, "SpamScores").get();
                    output("Spam score entries: " + scores.length);
                `,
            },
            {
                signature: "timeseries(document/documentId, name).getStats()",
                returnType: "object",
                keywords: ["time series", "count", "start", "end"],
                description: (
                    <>
                        Returns <code>Start</code>, <code>End</code>, and <code>Count</code> statistics for the series.
                        Available in both scripts.
                    </>
                ),
                sampleScript: dedent`
                    const spamStats = timeseries(this, "SpamScores").getStats();
                    output("Spam score entries: " + spamStats.Count);
                `,
            },
        ],
    },
    {
        category: "String manipulation",
        methods: [
            {
                signature: "startsWith(inputString, prefix)",
                returnType: "boolean",
                keywords: ["prefix", "case-insensitive"],
                description: <>Returns whether the string starts with the given prefix, ignoring case.</>,
                sampleScript: dedent`
                    for (const comment of this.Comments) {
                        if (startsWith(comment.Text, "http")) {
                            output("Link-like comment: " + comment.Id);
                        }
                    }
                `,
            },
            {
                signature: "endsWith(inputString, suffix)",
                returnType: "boolean",
                keywords: ["suffix", "case-insensitive"],
                description: <>Returns whether the string ends with the given suffix, ignoring case.</>,
                sampleScript: dedent`
                    if (endsWith(this.FileName, ".pdf")) {
                        output("The file is a PDF");
                    }
                `,
            },
            {
                signature: "regex(inputString, regex)",
                returnType: "boolean",
                keywords: ["regexp", "match", "pattern", ".net"],
                description: (
                    <>
                        Returns whether the string matches the regular expression. The pattern uses{" "}
                        <strong>.NET</strong> regular expression syntax, not JavaScript, so inline options such as{" "}
                        <code>(?i)</code> work while JavaScript-style flags do not. Evaluated server-side, with the
                        timeout from the <code>Queries.RegexTimeout</code> configuration.
                    </>
                ),
                sampleScript: dedent`
                    for (const comment of this.Comments) {
                        if (regex(comment.Text, "(?i)(viagra|casino|crypto)")) {
                            output("Possible spam: " + comment.Id);
                        }
                    }
                `,
            },
            {
                signature: "String.prototype.format(arg1, arg2, ...)",
                returnType: "string",
                keywords: ["template", "placeholder", "interpolate"],
                description: (
                    <>
                        Replaces <code>{"{0}"}</code>, <code>{"{1}"}</code>, ... placeholders in the string with the
                        given arguments.
                    </>
                ),
                sampleScript: dedent`
                    const summary = "Topic: {0}, comments: {1}".format(this.Topic, this.Comments.length);
                    output(summary);
                `,
            },
        ],
    },
    {
        category: "Arrays & objects",
        methods: [
            {
                signature: "Object.map(input, mapFunction, context)",
                returnType: "any[]",
                keywords: ["project", "iterate", "array"],
                description: (
                    <>
                        Projects each value of an object or array into an array. <code>mapFunction</code> is called with{" "}
                        <code>(value, key)</code>; the optional <code>context</code> becomes its <code>this</code>.
                    </>
                ),
                sampleScript: dedent`
                    const texts = Object.map(this.Comments, (comment) => comment.Text);
                    output(texts.join("\\n"));
                `,
            },
        ],
    },
    {
        category: "Mathematical operations",
        methods: [
            {
                signature: "Raven_Min(value1, value2)",
                returnType: "number | string | boolean | null | undefined",
                keywords: ["min", "smaller", "compare"],
                description: <>Returns the smaller of the two values, using RavenDB&apos;s comparison rules.</>,
                sampleScript: dedent`
                    const sampleSize = Raven_Min(this.Comments.length, 10);
                    output("Sample size: " + sampleSize);
                `,
            },
            {
                signature: "Raven_Max(value1, value2)",
                returnType: "number | string | boolean | null | undefined",
                keywords: ["max", "larger", "compare"],
                description: <>Returns the larger of the two values, using RavenDB&apos;s comparison rules.</>,
                sampleScript: dedent`
                    const severity = Raven_Max(this.Severity, 5);
                    output("Severity: " + severity);
                `,
            },
        ],
    },
    {
        category: "Spatial",
        methods: [
            {
                signature: 'spatial.distance(lat1, lng1, lat2, lng2, units = "kilometers")',
                returnType: "number",
                keywords: ["geo", "distance", "kilometers", "miles", "cartesian"],
                description: (
                    <>
                        Returns the great-circle distance between two points. <code>units</code> may be{" "}
                        <code>&quot;kilometers&quot;</code>, <code>&quot;miles&quot;</code> or{" "}
                        <code>&quot;cartesian&quot;</code> &mdash; the last one returns a plain cartesian distance
                        instead.
                    </>
                ),
                sampleScript: dedent`
                    const distance = spatial.distance(this.Latitude, this.Longitude, 32.0853, 34.7818, "kilometers");
                    output("Distance from office: " + distance + " km");
                `,
            },
        ],
    },
    {
        category: "Conversion & dates",
        methods: [
            {
                signature: "scalarToRawString(document, lambdaToField)",
                returnType: "string | number | boolean | null",
                keywords: ["raw", "date", "verbatim"],
                description: (
                    <>
                        Reads a field exactly as stored, without RavenDB&apos;s automatic type conversion &mdash; useful
                        for date strings you want to pass to the model verbatim.
                    </>
                ),
                sampleScript: dedent`
                    const postedAt = scalarToRawString(this, x => x.PostedAt);
                    output("Posted at: " + postedAt);
                `,
            },
            {
                signature: "convertJsTimeToTimeSpanString(milliseconds)",
                returnType: "string",
                keywords: ["timespan", "duration", "dotnet"],
                description: (
                    <>
                        Converts a duration in milliseconds to a .NET <code>TimeSpan</code> string, e.g.{" "}
                        <code>&quot;00:05:00&quot;</code>.
                    </>
                ),
                sampleScript: dedent`
                    const elapsed = convertJsTimeToTimeSpanString(Date.now() - lastModified(this));
                    output("Elapsed: " + elapsed);
                `,
            },
            {
                signature:
                    "convertToTimeSpanString(ticks | hours, minutes, seconds | days, hours, minutes, seconds[, milliseconds])",
                returnType: "string",
                keywords: ["timespan", "duration", "ticks", "dotnet"],
                description: (
                    <>
                        Builds a .NET <code>TimeSpan</code> string from ticks, or from the individual time components.
                    </>
                ),
                sampleScript: dedent`
                    const reviewWindow = convertToTimeSpanString(1, 0, 0, 0);
                    output("Review window: " + reviewWindow);
                `,
            },
            {
                signature: 'compareDates(date1, date2, operationType = "Subtract")',
                returnType: "string | boolean",
                keywords: ["date", "subtract", "compare"],
                description: (
                    <>
                        Compares or subtracts two dates. <code>operationType</code> may be{" "}
                        <code>&quot;Subtract&quot;</code>, <code>&quot;GreaterThan&quot;</code>,{" "}
                        <code>&quot;GreaterThanOrEqual&quot;</code>, <code>&quot;LessThan&quot;</code>,{" "}
                        <code>&quot;LessThanOrEqual&quot;</code>, <code>&quot;Equal&quot;</code>, or{" "}
                        <code>&quot;NotEqual&quot;</code>.
                    </>
                ),
                sampleScript: dedent`
                    const age = compareDates(new Date().toISOString(), this.PostedAt, "Subtract");
                    output("Age: " + age);
                `,
            },
            {
                signature: "toStringWithFormat(object, format?, culture?)",
                returnType: "string",
                keywords: ["date", "format", "culture", "dotnet"],
                description: (
                    <>
                        Formats a date, number, boolean, or date string using an optional .NET format string and
                        culture, e.g. <code>&quot;yyyy-MM-dd&quot;</code>. The second argument may be either a format or
                        a culture.
                    </>
                ),
                sampleScript: dedent`
                    const postedOn = toStringWithFormat(new Date(this.PostedAt), "yyyy-MM-dd");
                    output("Posted on: " + postedOn);
                `,
            },
        ],
    },
    {
        category: "Cryptographic methods",
        methods: [
            {
                signature: "crypto.randomUUID()",
                returnType: "string",
                keywords: ["uuid", "guid", "random"],
                description: <>Returns a random RFC 4122 version 4 UUID.</>,
                sampleScript: `output("Request ID: " + crypto.randomUUID());`,
            },
            {
                signature: "crypto.getRandomValues(typedArray)",
                returnType: "TypedArray",
                keywords: ["random", "nonce"],
                description: (
                    <>Fills the given typed array with cryptographically strong random values and returns it.</>
                ),
                sampleScript: dedent`
                    const bytes = crypto.getRandomValues(new Uint8Array(16));
                    output("Nonce: " + Array.from(bytes).join("-"));
                `,
            },
            {
                signature: "crypto.getRandomValuesBase64(lenInBytes)",
                returnType: "string",
                keywords: ["random", "base64", "salt", "iv"],
                description: <>Returns the requested number of random bytes, base64-encoded.</>,
                sampleScript: `output("Salt: " + crypto.getRandomValuesBase64(16));`,
            },
            {
                signature: "crypto.digest(algorithm, data)",
                returnType: "string",
                keywords: ["hash", "sha256", "base64"],
                description: (
                    <>
                        Returns the base64 hash of <code>data</code>. <code>algorithm</code> may be{" "}
                        <code>&quot;SHA-256&quot;</code>, <code>&quot;SHA-384&quot;</code> or{" "}
                        <code>&quot;SHA-512&quot;</code>. A string <code>data</code> is hashed as UTF-8 bytes.
                    </>
                ),
                sampleScript: dedent`
                    const topicHash = crypto.digest("SHA-256", this.Topic);
                    output("Topic hash: " + topicHash);
                `,
            },
            {
                signature: "crypto.sign(hash, key, data)",
                returnType: "string",
                keywords: ["hmac", "base64", "utf-8"],
                description: (
                    <>
                        Returns a base64 HMAC signature of <code>data</code> using <code>key</code>. Note the asymmetry
                        with the AES methods below: here a string <code>key</code> is taken as its{" "}
                        <strong>UTF-8 bytes</strong>, so a passphrase works as is. Any key length is accepted.
                    </>
                ),
                sampleScript: dedent`
                    const signature = crypto.sign("SHA-256", this.SigningKey, this.Text);
                    output("Signature: " + signature);
                `,
            },
            {
                signature: "crypto.verify(hash, key, signature, data)",
                returnType: "boolean",
                keywords: ["hmac", "base64"],
                description: (
                    <>
                        Verifies a base64 HMAC signature produced by <code>crypto.sign</code>, using a fixed-time
                        comparison. <code>key</code> follows the same UTF-8 rule as <code>crypto.sign</code>.
                    </>
                ),
                sampleScript: dedent`
                    const isValid = crypto.verify("SHA-256", this.SigningKey, this.Signature, this.Text);
                    output("Signature valid: " + isValid);
                `,
            },
            {
                signature: "crypto.encryptAesGcm(iv, key, data)",
                returnType: "string",
                keywords: ["aes", "base64", "encrypt"],
                description: (
                    <>
                        Encrypts <code>data</code> with AES-GCM and returns base64 ciphertext (payload followed by the
                        16-byte tag). <code>iv</code> and <code>key</code> given as strings are{" "}
                        <strong>base64-decoded</strong>, not read as text, so a plain passphrase throws. The decoded key
                        must be 16, 24 or 32 bytes, and the IV should be 12 bytes and never reused with the same key.{" "}
                        <code>data</code> as a string is taken as UTF-8.
                    </>
                ),
                sampleScript: dedent`
                    // iv and key are base64, so generate them as base64 too.
                    const iv = crypto.getRandomValuesBase64(12);
                    const encrypted = crypto.encryptAesGcm(iv, this.Base64Key, this.Text);
                    output("Encrypted text: " + encrypted + " (iv: " + iv + ")");
                `,
            },
            {
                signature: "crypto.decryptAesGcm(iv, key, data, outputType?)",
                returnType: "string | ArrayBuffer",
                keywords: ["aes", "base64", "decrypt"],
                description: (
                    <>
                        Decrypts AES-GCM ciphertext produced by <code>crypto.encryptAesGcm</code>. <code>iv</code> and{" "}
                        <code>key</code> follow the same base64 rule as when encrypting, and <code>data</code> as a
                        string is base64 too. <code>outputType</code> defaults to <code>&quot;string&quot;</code> (UTF-8
                        text) and may also be <code>&quot;raw&quot;</code> or <code>&quot;buffer&quot;</code>, which
                        both return an <code>ArrayBuffer</code>.
                    </>
                ),
                sampleScript: dedent`
                    const note = crypto.decryptAesGcm(this.Base64Iv, this.Base64Key, this.EncryptedNote, "string");
                    output(note);
                `,
            },
        ],
    },
    {
        category: "Debugging",
        methods: [
            {
                signature: "output(message) / console.log(message)",
                returnType: "void",
                keywords: ["debug", "log", "test"],
                description: (
                    <>
                        Writes a message to the test output. Use it together with <strong>Test context</strong> or the
                        playground to inspect what the script is doing.
                    </>
                ),
                sampleScript: dedent`
                    for (const comment of this.Comments) {
                        output("Processing comment " + comment.Id);
                    }
                `,
            },
        ],
    },
    {
        category: "Not available in GenAI scripts",
        methods: [
            {
                signature: "incrementCounter(document, name, value = 1) / deleteCounter(document, name)",
                returnType: "do not use",
                isUnavailable: true,
                scope: "update",
                keywords: ["counter", "increment", "unsupported"],
                description: (
                    <>
                        Do not call these in a GenAI task. They do not throw: the counter value is written to storage,
                        but the document&apos;s <code>@counters</code> metadata is not, because a GenAI task does not
                        run the patch bookkeeping that would update it. The result is a counter that exists in storage
                        yet is invisible to <code>getCounters()</code> and <code>hasCounter()</code>. Change counters
                        from a client, a patch operation or a subscription instead.
                    </>
                ),
            },
            {
                signature: "timeseries(document, name).append(...) / .increment(...) / .delete(...)",
                returnType: "do not use",
                isUnavailable: true,
                scope: "update",
                keywords: ["time series", "append", "unsupported"],
                description: (
                    <>
                        Same story as the counter mutations: the entries land in storage while the document&apos;s{" "}
                        <code>@timeseries</code> metadata is left behind. Only <code>.get()</code> and{" "}
                        <code>.getStats()</code> are safe here.
                    </>
                ),
            },
            {
                signature: "attachments(document, name).delete() / .copyFrom(...) / .remote(...)",
                returnType: "do not use",
                isUnavailable: true,
                scope: "update",
                keywords: ["attachment", "unsupported"],
                description: (
                    <>
                        Attachment mutations need the same missing bookkeeping and leave the document&apos;s{" "}
                        <code>@attachments</code> metadata inconsistent. Reading attachments in the context generation
                        script is fine: see <code>loadAttachment()</code> and <code>getAttachments()</code>.
                    </>
                ),
            },
            {
                signature: "archived.unarchive(document)",
                returnType: "do not use",
                isUnavailable: true,
                scope: "update",
                keywords: ["archive", "unsupported"],
                description: (
                    <>
                        Strips the <code>@archived</code> marker from the metadata, but the document&apos;s Archived
                        flag is only cleared by the patch pipeline, which a GenAI task does not run. The document stays
                        archived while claiming otherwise. <code>archived.archiveAt()</code> is fine, because it only
                        writes metadata.
                    </>
                ),
            },
            {
                signature: "loadTo(...) / loadCounter(...) / loadTimeSeries(...)",
                returnType: "throws",
                isUnavailable: true,
                scope: "context",
                keywords: ["etl", "unsupported"],
                description: (
                    <>
                        Present because the context generation script runs inside the ETL engine, but a GenAI task
                        rejects them with <em>&quot;... is not supported in GenAI Task&quot;</em>. Emit context with{" "}
                        <code>ai.genContext()</code> instead, and read counters and time series with{" "}
                        <code>counter()</code> and <code>timeseries()</code>.
                    </>
                ),
            },
            {
                signature: "crypto.subtle.*",
                returnType: "throws",
                isUnavailable: true,
                keywords: ["crypto", "async", "webcrypto", "unsupported"],
                description: (
                    <>
                        The whole async WebCrypto surface throws, including <code>digest</code>, <code>sign</code>,{" "}
                        <code>verify</code>, <code>encrypt</code>, <code>decrypt</code> and the key-management methods.
                        Scripts run synchronously, so use the <code>crypto.*</code> methods above; the thrown error
                        names the replacement for each one.
                    </>
                ),
            },
        ],
    },
];

function methodGroupsForScope(scope: GenAiScriptScope): MethodGroup[] {
    return genAiMethodGroups
        .map((group) => ({
            category: group.category,
            methods: group.methods.filter((method) => !method.scope || method.scope === scope),
        }))
        .filter((group) => group.methods.length > 0);
}

export const contextScriptMethodGroups = methodGroupsForScope("context");
export const updateScriptMethodGroups = methodGroupsForScope("update");

export function GenAiMethodsNotice() {
    return (
        <RichAlert variant="info" title="Scripts run synchronously, on the server">
            Everything listed here is synchronous: there is no <code>await</code>, no <code>fetch</code> and no timer.
            Anything the script needs must come from the source document, from <code>load()</code> or from{" "}
            <code>cmpxchg()</code>. The last group lists methods that exist in the engine but must not be used in a
            GenAI task.
        </RichAlert>
    );
}
