import React from "react";
import Col from "react-bootstrap/Col";
import { useOS } from "components/hooks/useOS";
import Dropdown from "react-bootstrap/Dropdown";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

const KeyboardShortcuts = () => {
    const os = useOS();
    const isMac = os === "MacOS";
    const isAiAssistantDisabled = useAppSelector(aiAssistantSelectors.isDisabled);
    const t = useStudioTranslation("studioSearchWithDatabaseSelector");

    return (
        <Col sm={12} className="studio-search__legend-col p-0">
            <div className="studio-search__legend-col__group">
                <Dropdown.Header className="studio-search__legend-col__group__header">
                    <div className="d-flex align-items-center gap-1">
                        <kbd>↑</kbd> <span>{t("search.legend.moveUp")}</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>↓</kbd> <span>{t("search.legend.moveDown")}</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>{isMac ? "⌥" : t("search.legend.keys.alt")}</kbd> <kbd>→</kbd>
                        <span>{t("search.legend.moveRight")}</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>{isMac ? "⌥" : t("search.legend.keys.alt")}</kbd> <kbd>←</kbd>
                        <span>{t("search.legend.moveLeft")}</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>{t("search.legend.keys.enter")}</kbd> <span>{t("search.legend.select")}</span>
                    </div>
                    <div className="d-flex align-items-center gap-1">
                        <kbd>{t("search.legend.keys.esc")}</kbd> <span>{t("search.legend.close")}</span>
                    </div>
                    {!isAiAssistantDisabled && (
                        <div className="d-flex align-items-center gap-1 ms-auto">
                            <kbd>{isMac ? "⌘" : t("search.legend.keys.ctrl")}</kbd> <kbd>/</kbd>{" "}
                            <span>{t("search.askAi")}</span>
                        </div>
                    )}
                </Dropdown.Header>
            </div>
        </Col>
    );
};

export default KeyboardShortcuts;
