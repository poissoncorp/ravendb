import React from "react";
import ConnectionStringError from "./ConnectionTestError";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface ConnectionTestResultProps {
    testResult: Pick<Raven.Server.Web.System.NodeConnectionTestResult, "Success" | "Error">;
}

export default function ConnectionTestResult({ testResult }: ConnectionTestResultProps) {
    const t = useStudioTranslation("connectionTests");

    if (!testResult) {
        return null;
    }

    return testResult.Success ? (
        <RichAlert variant="success">{t("success")}</RichAlert>
    ) : (
        <ConnectionStringError message={testResult.Error} />
    );
}
