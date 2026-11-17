import React from "react";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import { UsedIdData } from "components/pages/database/settings/unusedDatabaseIds/useUnusedDatabaseIds";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface UsedIdProps {
    usedIdData: UsedIdData;
}

export default function UsedId({ usedIdData }: UsedIdProps) {
    const { databaseId, nodeTag, shardNumber } = usedIdData;
    const t = useStudioTranslation("unusedDatabaseIds");

    return (
        <div className="used-id-pill">
            <Button
                className="text-truncate rounded-pill"
                title={t("usedIds.copyTitle")}
                variant="dark"
                onClick={() => copyToClipboard.copy(databaseId, t("copied", { id: databaseId }))}
            >
                {databaseId}
            </Button>
            <div className="d-flex gap-1 ms-auto">
                <span className="text-muted" title={t("usedIds.nodeTitle", { nodeTag })}>
                    <Icon icon="node" color="node" />
                    {nodeTag}
                </span>
                {shardNumber && (
                    <span className="text-muted" title={t("usedIds.shardTitle", { shardNumber })}>
                        <Icon icon="shard" color="shard" />
                        {shardNumber}
                    </span>
                )}
            </div>
        </div>
    );
}
