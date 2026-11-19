import { DatabaseGroupActions, DatabaseGroupItem, DatabaseGroupNode } from "components/common/DatabaseGroup";
import React from "react";
import Spinner from "react-bootstrap/Spinner";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function DeletionInProgress(props: { nodeTag: string }) {
    const t = useStudioTranslation("manageDatabaseGroup");
    const { nodeTag } = props;
    return (
        <DatabaseGroupItem className="item-busy">
            <DatabaseGroupNode>{nodeTag}</DatabaseGroupNode>
            <DatabaseGroupActions>
                <div className="pulse text-progress" title={t("deletionInProgress.title")}>
                    <Spinner size="sm" className="me-1" /> {t("deletionInProgress.label")}
                </div>
            </DatabaseGroupActions>
        </DatabaseGroupItem>
    );
}
