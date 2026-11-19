import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import RichAlert from "components/common/RichAlert";
import Modal from "components/common/Modal";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface ConfirmSwapSideBySideIndexProps {
    indexName: string;
    toggle: () => void;
    allActionContexts: DatabaseActionContexts[];
    onConfirm: (contexts: DatabaseActionContexts[]) => void;
}

export function ConfirmSwapSideBySideIndex(props: ConfirmSwapSideBySideIndexProps) {
    const { indexName, toggle, onConfirm, allActionContexts } = props;
    const t = useStudioTranslation("indexesList");
    const commonT = useStudioTranslation("common");

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal show onHide={toggle} contentClassName="modal-border bulge-warning">
            <Modal.Header className="vstack gap-3" onCloseClick={toggle}>
                <Icon icon="index" color="warning" addon="swap" className="fs-1" margin="m-0" />
                <div className="text-center lead">
                    <StudioTrans
                        ns="indexesList"
                        i18nKey="swapConfirm.title"
                        components={{ span: <span className="text-warning" /> }}
                    />
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 position-relative">
                <span className="text-center bg-faded-primary py-1 px-3 w-fit-content rounded-pill mx-auto">
                    <Icon icon="index" />
                    {indexName}
                </span>
                <RichAlert variant="warning">
                    <StudioTrans ns="indexesList" i18nKey="swapConfirm.info" components={{ strong: <strong /> }} />
                </RichAlert>
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4>{t("swapConfirm.selectContext")}</h4>
                        <MultipleDatabaseLocationSelector
                            allActionContexts={allActionContexts}
                            selectedActionContexts={selectedActionContexts}
                            setSelectedActionContexts={setSelectedActionContexts}
                        />
                    </div>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    {commonT("cancel")}
                </Button>
                <Button variant="warning" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="swap" />
                    {t("swapConfirm.confirm")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
