import { MethodGroup } from "./samplesTypes";
import {
    contextScriptMethodGroups,
    updateScriptMethodGroups,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editGenAiTask/editGenAiTaskMethodsData";
import { methodGroups as patchMethodGroups } from "viewmodels/database/patch/patchSamplesData";

/**
 * The GenAI reference and the Patch reference describe the same scripting engine, but each keeps its
 * own copy of the shared entries because the examples have to differ (RQL patch vs GenAI JavaScript).
 * The signatures and return types are NOT allowed to differ: they are statements about the engine, and
 * the two lists drifting apart is how one panel ends up telling the truth while the other does not.
 */
function flatten(groups: MethodGroup[]) {
    return groups.flatMap((group) => group.methods);
}

function returnTypesBySignature(groups: MethodGroup[]) {
    const map = new Map<string, string>();

    for (const method of flatten(groups)) {
        // an unavailable row puts a status word in that column ("do not use", "throws"), not a type
        if (typeof method.returnType === "string" && !method.isUnavailable) {
            map.set(method.signature, method.returnType);
        }
    }

    return map;
}

describe("method reference consistency", () => {
    const patch = returnTypesBySignature(patchMethodGroups);
    const genAi = returnTypesBySignature([...contextScriptMethodGroups, ...updateScriptMethodGroups]);
    const shared = [...genAi.keys()].filter((signature) => patch.has(signature));

    it("shares a meaningful number of entries between the two references", () => {
        // guards the test itself: a signature rename on one side would silently empty the intersection
        expect(shared.length).toBeGreaterThanOrEqual(20);
    });

    it.each([["shared entries agree on the return type"]])("%s", () => {
        const mismatches = shared
            .filter((signature) => patch.get(signature) !== genAi.get(signature))
            .map((signature) => ({ signature, patch: patch.get(signature), genAi: genAi.get(signature) }));

        expect(mismatches).toEqual([]);
    });

    // a signature may well be usable in Patch and unavailable in GenAI - that is the point of the
    // "Not available in GenAI scripts" group, since the Patch view runs the full patch pipeline.
    // What must never happen is one reference saying both things about the same signature.
    it.each([
        ["context generation script", contextScriptMethodGroups],
        ["update script", updateScriptMethodGroups],
        ["patch", patchMethodGroups],
    ])("does not both recommend and forbid the same signature (%s)", (_label, groups) => {
        const methods = flatten(groups);
        const forbidden = new Set(methods.filter((m) => m.isUnavailable).map((m) => m.signature));
        const contradictions = methods
            .filter((m) => !m.isUnavailable && forbidden.has(m.signature))
            .map((m) => m.signature);

        expect(contradictions).toEqual([]);
    });
});
