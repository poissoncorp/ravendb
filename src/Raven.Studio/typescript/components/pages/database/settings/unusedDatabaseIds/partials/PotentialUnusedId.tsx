import React from "react";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface PotentialUnusedIdProps {
    id: string;
    addUnusedId: () => void;
    isAdded: boolean;
}

export default function PotentialUnusedId(props: PotentialUnusedIdProps) {
    const { id, addUnusedId, isAdded } = props;
    const t = useStudioTranslation("unusedDatabaseIds");

    return (
        <ConditionalPopover
            conditions={{
                isActive: isAdded,
                message: t("potentialIds.alreadyAdded"),
            }}
            popoverPlacement="top"
        >
            <Button
                className="rounded-pill"
                onClick={addUnusedId}
                title={t("potentialIds.copyTitle")}
                variant="outline-secondary"
                disabled={isAdded}
            >
                <strong>{id}</strong>
            </Button>
        </ConditionalPopover>
    );
}
