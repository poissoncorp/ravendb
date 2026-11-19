import fs from "fs";
import path from "path";
import { i18n, initI18n } from "./i18n";

const viewsRoot = path.resolve(__dirname, "../../../wwwroot/App/views");

function listHtmlFiles(dir: string): string[] {
    return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            return listHtmlFiles(fullPath);
        }
        return entry.name.endsWith(".html") ? [fullPath] : [];
    });
}

// data-bind attributes that use the i18n / i18nAttr bindings, and 'namespace:key' strings inside them
const i18nBindingRegex = /data-bind="([^"]*\bi18n(?:Attr)?\s*:[^"]*)"/g;
const keyRegex = /'([A-Za-z][\w]*:[\w.]+)'/g;
const contextRegex = /\bcontext\s*:([^,}]*)/g;
const stringLiteralRegex = /'([^']*)'/g;
const placeholderRegex = /\{\{(\w+)\}\}/g;
const countRegex = /\bcount\s*:/;

function contextsOf(binding: string): string[] {
    return [...binding.matchAll(contextRegex)].flatMap(([, expression]) =>
        [...expression.matchAll(stringLiteralRegex)].map(([, context]) => context)
    );
}

// The English entries a binding can show: one per context, and the _one and _other forms when it passes a count.
function variantsOf(path: string, contexts: string[], isPlural: boolean): string[] {
    const bases = contexts.length === 0 ? [path] : contexts.map((context) => `${path}_${context}`);
    return isPlural ? bases.flatMap((base) => [`${base}_one`, `${base}_other`]) : bases;
}

function keyResolves(key: string, contexts: string[], isPlural: boolean): boolean {
    if (isPlural) {
        const [ns, path] = key.split(":");
        return variantsOf(path, contexts, true).every((variant) => i18n.getResource("en", ns, variant) != null);
    }
    if (contexts.length === 0) {
        return i18n.exists(key);
    }
    return contexts.every((context) => i18n.exists(key, { context }));
}

function placeholdersOf(key: string, contexts: string[], isPlural: boolean): string[] {
    const [ns, path] = key.split(":");
    return variantsOf(path, contexts, isPlural).flatMap((variant) =>
        [...String(i18n.getResource("en", ns, variant)).matchAll(placeholderRegex)].map(([, name]) => name)
    );
}

function keyProblems(html: string): string[] {
    return [...html.matchAll(i18nBindingRegex)].flatMap(([, binding]) => {
        const contexts = contextsOf(binding);
        const isPlural = countRegex.test(binding);
        return [...binding.matchAll(keyRegex)].flatMap(([, key]) => {
            if (!keyResolves(key, contexts, isPlural)) {
                return [key];
            }
            return placeholdersOf(key, contexts, isPlural)
                .filter((name) => !new RegExp(`\\b${name}\\s*:`).test(binding))
                .map((name) => `${key} needs option ${name}`);
        });
    });
}

describe("Knockout views i18n keys", () => {
    beforeAll(() => {
        initI18n();
    });

    it("has at least one translated view", () => {
        const translatedViews = listHtmlFiles(viewsRoot).filter((file) =>
            fs.readFileSync(file, "utf8").match(i18nBindingRegex)
        );
        expect(translatedViews.length).toBeGreaterThan(0);
    });

    it("accepts context variants only for contexts passed in the binding", () => {
        const withContext = `<h3 data-bind="i18n: { key: 'editCustomSorter:heading', options: { context: isNew() ? 'new' : 'edit' } }"></h3>`;
        const withoutContext = `<h3 data-bind="i18n: 'editCustomSorter:heading'"></h3>`;
        const withUnknownContext = `<h3 data-bind="i18n: { key: 'editCustomSorter:heading', options: { context: 'clone' } }"></h3>`;

        expect(keyProblems(withContext)).toEqual([]);
        expect(keyProblems(withoutContext)).toEqual(["editCustomSorter:heading"]);
        expect(keyProblems(withUnknownContext)).toEqual(["editCustomSorter:heading"]);
    });

    it("requires an option for every placeholder of the key", () => {
        const withOptions = `<span data-bind="i18n: { key: 'conflicts:resolvingConflictFor', options: { documentId: id } }"></span>`;
        const withoutOptions = `<span data-bind="i18n: 'conflicts:resolvingConflictFor'"></span>`;

        expect(keyProblems(withOptions)).toEqual([]);
        expect(keyProblems(withoutOptions)).toEqual(["conflicts:resolvingConflictFor needs option documentId"]);
    });

    it("accepts plural forms when the binding passes a count", () => {
        const withCount = `<span data-bind="i18n: { key: 'deleteDatabaseConfirm:title', options: { count: databases.length } }"></span>`;
        const withoutCount = `<span data-bind="i18n: 'deleteDatabaseConfirm:title'"></span>`;

        expect(keyProblems(withCount)).toEqual([]);
        expect(keyProblems(withoutCount)).toEqual(["deleteDatabaseConfirm:title"]);
    });

    it("every key used in a view exists in en resources and gets its options", () => {
        const problems = listHtmlFiles(viewsRoot).flatMap((file) =>
            keyProblems(fs.readFileSync(file, "utf8")).map((problem) => `${path.relative(viewsRoot, file)}: ${problem}`)
        );

        expect(problems).toEqual([]);
    });
});
