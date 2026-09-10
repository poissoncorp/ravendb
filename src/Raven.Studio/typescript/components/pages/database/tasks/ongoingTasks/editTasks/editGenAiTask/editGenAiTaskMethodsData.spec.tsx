import { MethodGroup } from "components/common/samples/partials/samplesTypes";
import { contextScriptMethodGroups, updateScriptMethodGroups } from "./editGenAiTaskMethodsData";

const contextOnlySignatures = [
    "ai.genContext(ctx)",
    "AIContextItem.withText(data)",
    "AIContextItem.withPng(data)",
    "AIContextItem.withJpeg(data)",
    "AIContextItem.withWebp(data)",
    "AIContextItem.withGif(data)",
    "AIContextItem.withPdf(data)",
    "loadAttachment(name)",
    "hasAttachment(name)",
    "getAttachments()",
    "getRevisionsCount()",
    "getCounters()",
    "hasCounter(name)",
    "getTimeSeries()",
    "hasTimeSeries(timeSeriesName)",
    "loadTo(...) / loadCounter(...) / loadTimeSeries(...)",
];

const updateOnlySignatures = [
    "$input",
    "$output",
    "put(id, document[, changeVector])",
    "del(documentId[, changeVector])",
    "archived.archiveAt(document, utcDateString)",
    "incrementCounter(document, name, value = 1) / deleteCounter(document, name)",
    "timeseries(document, name).append(...) / .increment(...) / .delete(...)",
    "attachments(document, name).delete() / .copyFrom(...) / .remote(...)",
    "archived.unarchive(document)",
];

function getSignatures(groups: MethodGroup[]) {
    return groups.flatMap((group) => group.methods.map((method) => method.signature));
}

function getMethods(groups: MethodGroup[]) {
    return groups.flatMap((group) => group.methods);
}

function getSampleScripts(groups: MethodGroup[]) {
    return groups.flatMap((group) => group.methods.map((method) => method.sampleScript).filter(Boolean)).join("\n");
}

describe("editGenAiTaskMethodsData", () => {
    it("shows only methods supported by the context generation script", () => {
        const signatures = getSignatures(contextScriptMethodGroups);

        expect(signatures).toEqual(expect.arrayContaining(contextOnlySignatures));
        expect(signatures).toEqual(expect.not.arrayContaining(updateOnlySignatures));
        expect(contextScriptMethodGroups.every((group) => group.methods.length > 0)).toBe(true);
    });

    it("shows only methods supported by the update script", () => {
        const signatures = getSignatures(updateScriptMethodGroups);

        expect(signatures).toEqual(expect.arrayContaining(updateOnlySignatures));
        expect(signatures).toEqual(expect.not.arrayContaining(contextOnlySignatures));
        expect(updateScriptMethodGroups.every((group) => group.methods.length > 0)).toBe(true);
    });

    it("does not load update-only examples into the context generation script", () => {
        expect(getSampleScripts(contextScriptMethodGroups)).not.toMatch(/\$(input|output)\b/);
    });

    it("does not load context-only examples into the update script", () => {
        expect(getSampleScripts(updateScriptMethodGroups)).not.toMatch(
            /\b(ai\.|loadAttachment|hasAttachment|getAttachments|getRevisionsCount|getCounters|hasCounter|getTimeSeries|hasTimeSeries)\s*\(/
        );
    });

    it.each([
        ["context generation script", contextScriptMethodGroups],
        ["update script", updateScriptMethodGroups],
    ])("offers no runnable example for a method that must not be called (%s)", (_label, groups) => {
        const unavailable = getMethods(groups).filter((method) => method.isUnavailable);

        expect(unavailable.length).toBeGreaterThan(0);
        expect(unavailable.every((method) => !method.sampleScript)).toBe(true);
    });

    it.each([
        ["context generation script", contextScriptMethodGroups],
        ["update script", updateScriptMethodGroups],
    ])("warns about the unsupported mutations rather than hiding them (%s)", (_label, groups) => {
        const signatures = getSignatures(groups);

        // crypto.subtle throws in both scripts, so both references have to mention it
        expect(signatures).toContain("crypto.subtle.*");
        expect(groups.some((group) => group.category === "Not available in GenAI scripts")).toBe(true);
    });

    it("gives every usable method an example", () => {
        const usable = [...getMethods(contextScriptMethodGroups), ...getMethods(updateScriptMethodGroups)].filter(
            (method) => !method.isUnavailable
        );

        expect(usable.filter((method) => !method.sampleScript)).toEqual([]);
    });
});
