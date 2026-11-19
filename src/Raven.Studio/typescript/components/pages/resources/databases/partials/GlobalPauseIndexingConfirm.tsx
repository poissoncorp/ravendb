import Modal from "components/common/Modal";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { Icon } from "components/common/Icon";
import { useState } from "react";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface GlobalPauseIndexingConfirmProps {
    toggle: () => void;
    onConfirm: (contextPoints: DatabaseActionContexts[]) => void;
    allActionContexts: DatabaseActionContexts[];
}

export function GlobalPauseIndexingConfirm(props: GlobalPauseIndexingConfirmProps) {
    const { allActionContexts, toggle, onConfirm } = props;
    const t = useStudioTranslation("databases");
    const tCommon = useStudioTranslation("common");

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const handlePause = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal show scrollable onHide={toggle} contentClassName="modal-border bulge-warning">
            <Modal.Header className="vstack gap-4" onCloseClick={toggle}>
                <div className="text-center">
                    <Icon icon="index" color="warning" addon="pause" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">{t("globalIndexingConfirm.pauseTitle")}</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 pt-0">
                <p>{t("globalIndexingConfirm.pauseDescription")}</p>
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4>{t("globalIndexingConfirm.selectContext")}</h4>
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
                    {tCommon("cancel")}
                </Button>
                <Button variant="warning" onClick={handlePause} className="rounded-pill">
                    <Icon icon="pause" /> {t("globalIndexingConfirm.pause")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
