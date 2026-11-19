import React from "react";
import { MethodEntry, MethodGroup } from "components/common/samples/partials/samplesTypes";
import { dedent } from "components/utils/dedent";
import { createTranslator } from "common/i18n/i18n";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const t = createTranslator("editGenAiTask");

const descriptionTags = { code: <code />, strong: <strong />, em: <em /> };

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
 * Counter, time-series, and attachment mutations and `archived.unarchive` are intentionally omitted.
 * The production GenAI update path does not run the PatchDocumentCommand finalization that those operations require.
 */
const genAiMethodGroups: GenAiMethodGroup[] = [
    {
        get category() {
            return t("methods.contextObjects.category");
        },
        methods: [
            {
                signature: "ai.genContext(ctx)",
                returnType: "AIContextItem",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.genContext"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.withText"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.withPng"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.withJpeg"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    ai.genContext({ ProductId: id(this) })
                        .withJpeg(loadAttachment("product-photo.jpg"));
                `,
            },
            {
                signature: "AIContextItem.withWebp(data)",
                returnType: "AIContextItem",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.withWebp"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    ai.genContext({ ProductId: id(this) })
                        .withWebp(loadAttachment("product-photo.webp"));
                `,
            },
            {
                signature: "AIContextItem.withGif(data)",
                returnType: "AIContextItem",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.withGif"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    ai.genContext({ ProductId: id(this) })
                        .withGif(loadAttachment("animation.gif"));
                `,
            },
            {
                signature: "AIContextItem.withPdf(data)",
                returnType: "AIContextItem",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.contextObjects.withPdf"
                        components={descriptionTags}
                    />
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
        get category() {
            return t("methods.scriptInput.category");
        },
        methods: [
            {
                signature: "this",
                returnType: "object",
                get description() {
                    return t("methods.scriptInput.sourceDocument");
                },
                sampleScript: dedent`
                    // Read fields straight off the source document.
                    output("Topic: " + this.Topic + ", comments: " + this.Comments.length);
                `,
            },
            {
                signature: "$input",
                returnType: "object",
                scope: "update",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.scriptInput.input" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    // Find the comment this response is about, using the id we put on the context object.
                    const idx = this.Comments.findIndex(comment => comment.Id == $input.CommentId);
                `,
            },
            {
                signature: "$output",
                returnType: "object",
                scope: "update",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.scriptInput.output" components={descriptionTags} />
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
        get category() {
            return t("methods.attachments.category");
        },
        methods: [
            {
                signature: "loadAttachment(name)",
                returnType: "attachment reference | null",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.attachments.loadAttachment"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    // Only attach the image when the document actually has one.
                    if (hasAttachment("heart.png")) {
                        ai.genContext({ Id: id(this) })
                            .withPng(loadAttachment("heart.png"));
                    }
                `,
            },
            {
                signature: "hasAttachment(name)",
                returnType: "boolean",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.attachments.hasAttachment"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.attachments.getAttachments"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.attachments.getRevisionsCount"
                        components={descriptionTags}
                    />
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
        get category() {
            return t("methods.documents.category");
        },
        methods: [
            {
                signature: "id(document)",
                returnType: "string",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.documents.id" components={descriptionTags} />
                ),
                sampleScript: `output("Document ID: " + id(this));`,
            },
            {
                signature: "getMetadata(document) / metadataFor(document)",
                returnType: "object",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.documents.getMetadata"
                        components={descriptionTags}
                    />
                ),
                sampleScript: `output("Collection: " + getMetadata(this)["@collection"]);`,
            },
            {
                signature: "lastModified(document)",
                returnType: "number",
                get description() {
                    return t("methods.documents.lastModified");
                },
                sampleScript: `output("Last modified: " + lastModified(this));`,
            },
            {
                signature: "load(documentId)",
                returnType: "object | object[] | undefined",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.documents.load" components={descriptionTags} />
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
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.documents.loadPath" components={descriptionTags} />
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
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.documents.cmpxchg" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const policy = cmpxchg("policies/moderation");
                    output("Moderation policy: " + JSON.stringify(policy));
                `,
            },
            {
                signature: "put(id, document[, changeVector])",
                returnType: "string",
                scope: "update",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.documents.put" components={descriptionTags} />
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
                scope: "update",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.documents.del" components={descriptionTags} />
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
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.documents.archiveAt"
                        components={descriptionTags}
                    />
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
        get category() {
            return t("methods.counters.category");
        },
        methods: [
            {
                signature: "getCounters()",
                returnType: "string[]",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.counters.getCounters"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.counters.hasCounter"
                        components={descriptionTags}
                    />
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
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.counters.counter" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const reportedCount = counter(this, "Reports");
                    output("Reports: " + reportedCount);
                `,
            },
            {
                signature: "counterRaw(document/documentId, name)",
                returnType: "object",
                get description() {
                    return t("methods.counters.counterRaw");
                },
                sampleScript: dedent`
                    const reportsPerNode = counterRaw(this, "Reports");
                    output(reportsPerNode);
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.timeSeries.category");
        },
        methods: [
            {
                signature: "getTimeSeries()",
                returnType: "string[] | false",
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.timeSeries.getTimeSeries"
                        components={descriptionTags}
                    />
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
                scope: "context",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.timeSeries.hasTimeSeries"
                        components={descriptionTags}
                    />
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
                get description() {
                    return t("methods.timeSeries.getEntries");
                },
                sampleScript: dedent`
                    const scores = timeseries(this, "SpamScores").get();
                    output("Spam score entries: " + scores.length);
                `,
            },
            {
                signature: "timeseries(document/documentId, name).getStats()",
                returnType: "object",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.timeSeries.getStats"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const spamStats = timeseries(this, "SpamScores").getStats();
                    output("Spam score entries: " + spamStats.Count);
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.strings.category");
        },
        methods: [
            {
                signature: "startsWith(inputString, prefix)",
                returnType: "boolean",
                get description() {
                    return t("methods.strings.startsWith");
                },
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
                get description() {
                    return t("methods.strings.endsWith");
                },
                sampleScript: dedent`
                    if (endsWith(this.FileName, ".pdf")) {
                        output("The file is a PDF");
                    }
                `,
            },
            {
                signature: "regex(inputString, regex)",
                returnType: "boolean",
                get description() {
                    return t("methods.strings.regex");
                },
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
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.strings.format" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const summary = "Topic: {0}, comments: {1}".format(this.Topic, this.Comments.length);
                    output(summary);
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.arraysAndObjects.category");
        },
        methods: [
            {
                signature: "Object.map(input, mapFunction, context)",
                returnType: "any[]",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.arraysAndObjects.map"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const texts = Object.map(this.Comments, (comment) => comment.Text);
                    output(texts.join("\\n"));
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.math.category");
        },
        methods: [
            {
                signature: "Raven_Min(value1, value2)",
                returnType: "number | string | boolean | null | undefined",
                get description() {
                    return t("methods.math.min");
                },
                sampleScript: dedent`
                    const sampleSize = Raven_Min(this.Comments.length, 10);
                    output("Sample size: " + sampleSize);
                `,
            },
            {
                signature: "Raven_Max(value1, value2)",
                returnType: "number | string | boolean | null | undefined",
                get description() {
                    return t("methods.math.max");
                },
                sampleScript: dedent`
                    const severity = Raven_Max(this.Severity, 5);
                    output("Severity: " + severity);
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.spatial.category");
        },
        methods: [
            {
                signature: 'spatial.distance(lat1, lng1, lat2, lng2, units = "kilometers")',
                returnType: "number",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.spatial.distance" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const distance = spatial.distance(this.Latitude, this.Longitude, 32.0853, 34.7818, "kilometers");
                    output("Distance from office: " + distance + " km");
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.conversion.category");
        },
        methods: [
            {
                signature: "scalarToRawString(document, lambdaToField)",
                returnType: "string | number | boolean | null",
                get description() {
                    return t("methods.conversion.scalarToRawString");
                },
                sampleScript: dedent`
                    const postedAt = scalarToRawString(this, x => x.PostedAt);
                    output("Posted at: " + postedAt);
                `,
            },
            {
                signature: "convertJsTimeToTimeSpanString(milliseconds)",
                returnType: "string",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.conversion.convertJsTimeToTimeSpanString"
                        components={descriptionTags}
                    />
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
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.conversion.convertToTimeSpanString"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const reviewWindow = convertToTimeSpanString(1, 0, 0, 0);
                    output("Review window: " + reviewWindow);
                `,
            },
            {
                signature: 'compareDates(date1, date2, operationType = "Subtract")',
                returnType: "string | boolean",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.conversion.compareDates"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const age = compareDates(new Date().toISOString(), this.PostedAt, "Subtract");
                    output("Age: " + age);
                `,
            },
            {
                signature: "toStringWithFormat(object, format?, culture?)",
                returnType: "string",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.conversion.toStringWithFormat"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const postedOn = toStringWithFormat(new Date(this.PostedAt), "yyyy-MM-dd");
                    output("Posted on: " + postedOn);
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.crypto.category");
        },
        methods: [
            {
                signature: "crypto.randomUUID()",
                returnType: "string",
                get description() {
                    return t("methods.crypto.randomUuid");
                },
                sampleScript: `output("Request ID: " + crypto.randomUUID());`,
            },
            {
                signature: "crypto.getRandomValues(typedArray)",
                returnType: "TypedArray",
                get description() {
                    return t("methods.crypto.getRandomValues");
                },
                sampleScript: dedent`
                    const bytes = crypto.getRandomValues(new Uint8Array(16));
                    output("Nonce: " + Array.from(bytes).join("-"));
                `,
            },
            {
                signature: "crypto.getRandomValuesBase64(lenInBytes)",
                returnType: "string",
                get description() {
                    return t("methods.crypto.getRandomValuesBase64");
                },
                sampleScript: `output("Salt: " + crypto.getRandomValuesBase64(16));`,
            },
            {
                signature: "crypto.digest(algorithm, data)",
                returnType: "string",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.crypto.digest" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const topicHash = crypto.digest("SHA-256", this.Topic);
                    output("Topic hash: " + topicHash);
                `,
            },
            {
                signature: "crypto.sign(hash, key, data)",
                returnType: "string",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.crypto.sign" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const signature = crypto.sign("SHA-256", this.SigningKey, this.Text);
                    output("Signature: " + signature);
                `,
            },
            {
                signature: "crypto.verify(hash, key, signature, data)",
                returnType: "boolean",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.crypto.verify" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    const isValid = crypto.verify("SHA-256", this.SigningKey, this.Signature, this.Text);
                    output("Signature valid: " + isValid);
                `,
            },
            {
                signature: "crypto.encryptAesGcm(iv, key, data)",
                returnType: "string",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.crypto.encryptAesGcm"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const encrypted = crypto.encryptAesGcm(this.Iv, this.Key, this.Text);
                    output("Encrypted text: " + encrypted);
                `,
            },
            {
                signature: "crypto.decryptAesGcm(iv, key, data, outputType?)",
                returnType: "string | ArrayBuffer",
                description: (
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="methods.crypto.decryptAesGcm"
                        components={descriptionTags}
                    />
                ),
                sampleScript: dedent`
                    const note = crypto.decryptAesGcm(this.Iv, this.Key, this.EncryptedNote, "string");
                    output(note);
                `,
            },
        ],
    },
    {
        get category() {
            return t("methods.debugging.category");
        },
        methods: [
            {
                signature: "output(message) / console.log(message)",
                returnType: "void",
                description: (
                    <StudioTrans ns="editGenAiTask" i18nKey="methods.debugging.output" components={descriptionTags} />
                ),
                sampleScript: dedent`
                    for (const comment of this.Comments) {
                        output("Processing comment " + comment.Id);
                    }
                `,
            },
        ],
    },
];

function methodGroupsForScope(scope: GenAiScriptScope): MethodGroup[] {
    return genAiMethodGroups
        .map((group) => ({
            get category() {
                return group.category;
            },
            methods: group.methods.filter((method) => !method.scope || method.scope === scope),
        }))
        .filter((group) => group.methods.length > 0);
}

export const contextScriptMethodGroups = methodGroupsForScope("context");
export const updateScriptMethodGroups = methodGroupsForScope("update");
