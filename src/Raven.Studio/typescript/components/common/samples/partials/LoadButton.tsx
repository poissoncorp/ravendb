import React from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface LoadButtonProps {
    onSelect: () => void;
}

export default function LoadButton({ onSelect }: LoadButtonProps) {
    const t = useStudioTranslation("samples");

    return (
        <Button variant="link" className="text-emphasis" title={t("loadButton.title")} onClick={onSelect}>
            <Icon icon="arrow-left" margin="me-1" />
            {t("loadButton.label")}
        </Button>
    );
}
