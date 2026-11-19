import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import {
    MultipleDatabaseLocationSelector,
    DatabaseActionContexts,
} from "components/common/MultipleDatabaseLocationSelector";
import { Icon } from "components/common/Icon";
import ActionContextUtils from "components/utils/actionContextUtils";
import Modal from "components/common/Modal";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface BulkDatabaseResetConfirm {
    dbName: string;
    localNodeTag: string;
    allActionContexts: DatabaseActionContexts[];
    toggleConfirmation: () => void;
    onConfirm: (locations: databaseLocationSpecifier[]) => void;
}

export default function BulkDatabaseResetConfirm({
    dbName,
    localNodeTag,
    allActionContexts,
    toggleConfirmation: toggle,
    onConfirm,
}: BulkDatabaseResetConfirm) {
    const t = useStudioTranslation("databases");
    const tCommon = useStudioTranslation("common");
    const [actionContexts, setActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(actionContexts);
        toggle();
    };

    return (
        <Modal show>
            <Modal.Header className="vstack gap-2 pb-0" onCloseClick={toggle}>
                <h3>
                    <StudioTrans
                        ns="databases"
                        i18nKey="restartConfirm.title"
                        options={{
                            dbName,
                            nodeTag: localNodeTag,
                            context: ActionContextUtils.showContextSelector(allActionContexts) ? undefined : "onNode",
                        }}
                        components={{
                            db: <small className="d-inline-block bg-faded-primary rounded-pill px-2 py-1 mx-1" />,
                            icon: <Icon icon="database" />,
                            node: <small className="text-node" />,
                            nodeIcon: <Icon icon="node" margin="m-0" />,
                            strong: <strong />,
                        }}
                    />
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <div className="vstack align-items-center">
                    {ActionContextUtils.showContextSelector(allActionContexts) && (
                        <div>
                            <p>{t("restartConfirm.selectContext")}</p>
                            <MultipleDatabaseLocationSelector
                                allActionContexts={allActionContexts}
                                selectedActionContexts={actionContexts}
                                setSelectedActionContexts={setActionContexts}
                            />
                        </div>
                    )}
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={toggle}>
                    {tCommon("cancel")}
                </Button>
                <Button variant="danger" onClick={onSubmit}>
                    <Icon icon="reset" /> {t("restartConfirm.restart")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
