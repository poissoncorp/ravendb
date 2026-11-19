import React from "react";
import { CodeLanguage } from "components/common/Code";
import { MethodGroup, SampleScript, SamplesTab } from "./samplesTypes";
import SampleScriptsList from "./SampleScriptsList";
import MethodsTable from "./MethodsTable";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("samples");

export interface CreateSampleScriptsTabOptions {
    /** Nav pill text, when the default does not describe the content (e.g. sample JSON rather than scripts). */
    label?: string;
}

export interface CreateMethodsTabOptions {
    /** Language used to highlight the "Example usage" snippets. Defaults to "rql". */
    language?: CodeLanguage;
}

export function createSampleScriptsTab(scripts: SampleScript[], options?: CreateSampleScriptsTabOptions): SamplesTab {
    return {
        key: "scripts",
        get label() {
            return options?.label ?? t("tabs.sampleScripts");
        },
        icon: "document",
        content: ({ onSelect }) => <SampleScriptsList scripts={scripts} onSelect={onSelect} />,
    };
}

export function createMethodsTab(methodGroups: MethodGroup[], options?: CreateMethodsTabOptions): SamplesTab {
    return {
        key: "methods",
        get label() {
            return t("tabs.methods");
        },
        icon: "indent",
        hasSearch: true,
        get searchPlaceholder() {
            return t("tabs.searchBySignature");
        },
        content: ({ onSelect, search }) => (
            <MethodsTable
                methodGroups={methodGroups}
                search={search}
                onSelect={onSelect}
                language={options?.language}
            />
        ),
    };
}
