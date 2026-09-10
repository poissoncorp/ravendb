import { ReactNode } from "react";
import IconName from "typings/server/icons";
import { CodeLanguage } from "components/common/Code";

export interface MethodEntry {
    signature: string;
    returnType?: ReactNode;
    description: ReactNode;
    sampleScript?: string;
    /**
     * Extra terms the methods search should match, on top of the signature and the category name.
     * Use it for concepts a user is likely to type but that the signature does not contain
     * (e.g. "base64" for the crypto methods, "utc" for the date helpers).
     */
    keywords?: string[];
    /**
     * The method exists in the scripting engine but must not be used in this editor.
     * Such a row is rendered muted, carries no runnable example and cannot be inserted.
     * Always say in the description what happens if it is called anyway.
     */
    isUnavailable?: boolean;
}

export interface MethodGroup {
    category: string;
    methods: MethodEntry[];
}

export interface SampleScript {
    title: string;
    description: string;
    script: string;
    language?: CodeLanguage;
    whiteSpace?: "pre" | "pre-wrap" | "normal";
}

export interface SamplesTabContentContext {
    /** Replaces the whole editor content with the given script. */
    onSelect: (script: string) => void;
    /** Inserts the given snippet at the caret, leaving the rest of the editor untouched. */
    onInsert: (script: string) => void;
    search: string;
}

export interface SamplesTab {
    key: string;
    label: string;
    icon: IconName;
    hasSearch?: boolean;
    searchPlaceholder?: string;
    content: (ctx: SamplesTabContentContext) => ReactNode;
}
