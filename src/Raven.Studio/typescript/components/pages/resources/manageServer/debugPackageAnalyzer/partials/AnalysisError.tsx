import React from "react";
import { RichAlert } from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

interface AnalysisErrorProps {
    error: any;
    onReset: () => void;
}

export default function AnalysisError({ error, onReset }: AnalysisErrorProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);

    const message = extractMessage(error, t);
    const details = extractDetails(error);

    return (
        <RichAlert variant="danger" title={t("analysisError.title")}>
            <p className="mb-2">{message}</p>
            <div className="hstack gap-2 mb-2">
                <Button variant="primary" size="sm" className="rounded-pill" onClick={onReset}>
                    <Icon icon="refresh" /> {t("analysisError.tryAnotherPackage")}
                </Button>
                {details && (
                    <Button variant="link" size="sm" onClick={toggleDetails}>
                        {t("analysisError.exceptionDetails", { context: detailsVisible ? "hide" : "show" })}
                    </Button>
                )}
            </div>
            {details && (
                <Collapse in={detailsVisible}>
                    <div>
                        <pre className="debug-package-exception-details mb-0">{details}</pre>
                    </div>
                </Collapse>
            )}
        </RichAlert>
    );
}

function extractMessage(error: any, t: StudioTranslate<"debugPackageAnalyzer">): string {
    if (!error) {
        return t("analysisError.unknownError");
    }

    const parsed = tryParseJson(error.responseText);
    if (parsed?.Message) {
        return parsed.Message;
    }

    return error.statusText || error.message || t("analysisError.notAnalyzable");
}

function extractDetails(error: any): string {
    if (!error) {
        return null;
    }

    const parsed = tryParseJson(error.responseText);
    if (parsed?.Error) {
        return parsed.Error;
    }

    return error.responseText || null;
}

function tryParseJson(text: string): { Message?: string; Error?: string } {
    if (!text) {
        return null;
    }
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}
