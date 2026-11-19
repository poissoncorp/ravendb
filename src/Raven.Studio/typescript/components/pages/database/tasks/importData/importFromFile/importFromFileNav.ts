import IconName from "typings/server/icons";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("importFromFile");

export interface SectionNavItem {
    id: string;
    label: string;
    icon: IconName;
    children?: { id: string; label: string }[];
}

export const sectionNav: SectionNavItem[] = [
    {
        id: "select-file",
        get label() {
            return t("sections.selectFile");
        },
        icon: "folder",
    },
    {
        id: "data-to-import",
        get label() {
            return t("sections.dataToImport");
        },
        icon: "document",
        children: [
            {
                id: "collections-to-import",
                get label() {
                    return t("sections.collectionsToImport");
                },
            },
            {
                id: "documents-and-extensions",
                get label() {
                    return t("sections.documentsAndExtensions");
                },
            },
        ],
    },
    {
        id: "configuration-to-import",
        get label() {
            return t("sections.configurationToImport");
        },
        icon: "database",
        children: [
            {
                id: "database-entities",
                get label() {
                    return t("sections.databaseEntities");
                },
            },
            {
                id: "database-settings",
                get label() {
                    return t("sections.databaseSettings");
                },
            },
        ],
    },
    {
        id: "import-processing",
        get label() {
            return t("sections.importProcessing");
        },
        icon: "settings",
    },
];

export const sectionIds = sectionNav.flatMap((item) => [item.id, ...(item.children?.map((child) => child.id) ?? [])]);

export function getFirstErrorPath(errors: unknown, prefix = ""): string | null {
    if (!errors || typeof errors !== "object") {
        return null;
    }
    if ("message" in errors && typeof (errors as { message?: unknown }).message === "string") {
        return prefix;
    }
    for (const [key, value] of Object.entries(errors)) {
        const path = prefix ? `${prefix}.${key}` : key;
        const found = getFirstErrorPath(value, path);
        if (found) {
            return found;
        }
    }
    return null;
}

const errorFieldTargets: {
    path: string;
    sectionId: string;
}[] = [
    { path: "processing", sectionId: "import-processing" },
    { path: "file", sectionId: "select-file" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "collections", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
];

export function getSectionIdForErrorPath(errorPath: string): string | null {
    return errorFieldTargets.find((x) => errorPath.startsWith(x.path))?.sectionId ?? null;
}
