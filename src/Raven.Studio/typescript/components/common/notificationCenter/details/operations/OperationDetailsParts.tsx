import operation from "common/notifications/models/operation";
import Code from "components/common/Code";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type OperationExceptionResult = Raven.Client.Documents.Operations.OperationExceptionResult;

export function OperationTimeElapsed({ operation }: { operation: operation }) {
    const t = useStudioTranslation("notificationCenter");
    const duration = useKnockoutValue(operation.duration);

    return (
        <div className="text-nowrap">
            <span className="text-muted">{t("operation.timeElapsed")}</span> {duration}
        </div>
    );
}

export function OperationFailure({ operation }: { operation: operation }) {
    const t = useStudioTranslation("notificationCenter");
    const result = useKnockoutValue(operation.result) as OperationExceptionResult;
    const errorText = [result?.Message, result?.Error].filter(Boolean).join("\n");

    return (
        <div className="vstack gap-2">
            <h4 className="text-danger text-center mb-0">{t("operation.failed")}</h4>
            <Code code={errorText} language="plaintext" whiteSpace="pre-wrap" className="notification-details-code" />
        </div>
    );
}
