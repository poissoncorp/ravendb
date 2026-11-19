import "./Code.scss";
import { ReactNode, useMemo, useState } from "react";
import Prism from "prismjs";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import queryCriteria from "models/database/query/queryCriteria";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import useConfirm from "components/common/ConfirmDialog";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");
require("prismjs/components/prism-json");
require("prismjs/components/prism-sql");
require("prismjs/components/prism-python");
require("prismjs/components/prism-java");

export const supportedCodeLanguages = [
    "plaintext",
    "markup",
    "html",
    "mathml",
    "svg",
    "xml",
    "ssml",
    "atom",
    "rss",
    "css",
    "clike",
    "javascript",
    "csharp",
    "json",
    "sql",
    "rql",
    "python",
    "java",
] as const;

export type CodeLanguage = (typeof supportedCodeLanguages)[number];

interface CodeProps {
    code: string;
    language: CodeLanguage;
    className?: string;
    codeClassName?: string;
    whiteSpace?: "pre" | "pre-wrap" | "normal";
    // Adds a "Wrap" toggle to the actions bar (on by default) so long lines wrap instead of overflowing horizontally.
    wrappable?: boolean;
    isActionsHidden?: boolean;
    isRunQueryHidden?: boolean;
    extraActions?: ReactNode;
    sourceView?: "chatbot";
    isTitleHidden?: boolean;
}

export default function Code(props: CodeProps) {
    const {
        code,
        className,
        codeClassName,
        whiteSpace,
        wrappable,
        isActionsHidden,
        isRunQueryHidden,
        extraActions,
        sourceView,
        isTitleHidden,
    } = props;

    const t = useStudioTranslation("commonComponents");
    const [wrapped, setWrapped] = useState(true);
    const isWrapped = wrappable === true && wrapped;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const chatbotDatabaseContext = useAppSelector((state) =>
        chatbotSelectors.attachedContextById(state, "DatabaseName")
    );
    const hasDatabase = !!activeDatabaseName || chatbotDatabaseContext?.state === "included";

    const languageTitle = getLanguageTitles(t)[props.language];
    const languageToHighlight = getLanguageToHighlight(props.language);

    const html = useMemo(
        () => Prism.highlight(code, Prism.languages[languageToHighlight], languageToHighlight),
        [code, languageToHighlight]
    );

    const confirm = useConfirm();

    const executeQuery = async (dbName: string) => {
        const query = queryCriteria.empty();
        query.queryText(code);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();

        savedQueriesStorage.saveAndNavigate(dbName, queryDto, {
            newWindow: false,
            extraParameters: sourceView === "chatbot" ? "&sourceView=chatbot" : "",
        });
    };

    const handleRunQuery = async () => {
        if (
            sourceView === "chatbot" &&
            chatbotDatabaseContext?.state === "included" &&
            chatbotDatabaseContext.value !== activeDatabaseName
        ) {
            const isConfirmed = await confirm({
                title: t("code.changeDatabase.title"),
                message: (
                    <div>
                        <StudioTrans
                            ns="commonComponents"
                            i18nKey="code.changeDatabase.message"
                            components={{ strong: <strong />, br: <br /> }}
                            options={{ databaseName: chatbotDatabaseContext.value }}
                        />
                    </div>
                ),
                confirmText: t("code.changeDatabase.confirm"),
                confirmIcon: "rocket",
            });

            if (!isConfirmed) {
                return;
            }

            executeQuery(chatbotDatabaseContext.value);
        } else {
            executeQuery(activeDatabaseName);
        }
    };

    return (
        <div className={classNames("code", className)}>
            {!isActionsHidden && (
                <div className="code-actions">
                    {!isTitleHidden && languageTitle && <div>{languageTitle}</div>}
                    <div className="hstack gap-2 ms-auto">
                        {wrappable && (
                            <Button
                                variant="link"
                                className="text-emphasis"
                                active={wrapped}
                                aria-pressed={wrapped}
                                title={wrapped ? t("code.disableWrapping") : t("code.enableWrapping")}
                                onClick={() => setWrapped((prev) => !prev)}
                            >
                                <Icon icon="newline" />
                                {t("code.wrap")}
                            </Button>
                        )}
                        {extraActions}
                        <Button
                            variant="link"
                            className="text-emphasis"
                            title={t("copyToClipboard")}
                            onClick={() => copyToClipboard.copy(`${code}`, t("code.copied"))}
                        >
                            <Icon icon="copy" />
                            {t("code.copy")}
                        </Button>
                        {!isRunQueryHidden && hasDatabase && props.language === "rql" && (
                            <Button variant="link" className="text-emphasis" onClick={handleRunQuery}>
                                <Icon icon="rocket" />
                                {t("code.runQuery")}
                            </Button>
                        )}
                    </div>
                </div>
            )}
            <pre className="code-classes d-flex flex-grow-1 m-0">
                <code
                    className={classNames(`language-${languageToHighlight}`, "monospace-font", codeClassName, {
                        wrapped: isWrapped,
                    })}
                    style={{ whiteSpace: isWrapped ? "pre-wrap" : (whiteSpace ?? "pre") }}
                >
                    <div dangerouslySetInnerHTML={{ __html: html }} />
                </code>
            </pre>
        </div>
    );
}

function getLanguageToHighlight(language: CodeLanguage): CodeLanguage {
    switch (language) {
        case "rql":
            return "sql";
        default:
            return language;
    }
}

const getLanguageTitles = (t: StudioTranslate<"commonComponents">): Record<CodeLanguage, string> => ({
    plaintext: t("code.language.plaintext"),
    markup: t("code.language.markup"),
    html: "HTML",
    mathml: "MathML",
    svg: "SVG",
    xml: "XML",
    ssml: "SSML",
    atom: "Atom",
    rss: "RSS",
    css: "CSS",
    clike: t("code.language.clike"),
    javascript: "JavaScript",
    csharp: "C#",
    json: "JSON",
    sql: "SQL",
    rql: "RQL",
    python: "Python",
    java: "Java",
});
