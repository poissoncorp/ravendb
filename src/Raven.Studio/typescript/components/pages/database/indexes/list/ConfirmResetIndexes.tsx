import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { IndexSharedInfo } from "components/models/indexes";
import IndexUtils from "components/utils/IndexUtils";
import RichAlert from "components/common/RichAlert";
import Modal from "components/common/Modal";
import classNames from "classnames";
import Accordion from "react-bootstrap/Accordion";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface ConfirmResetIndexesProps {
    indexes: IndexSharedInfo[];
    allActionContexts: DatabaseActionContexts[];
    mode: Raven.Client.Documents.Indexes.IndexResetMode;
    closeConfirm: () => void;
    onConfirm: (indexNames: string[], contexts: DatabaseActionContexts[]) => void;
}

export function ConfirmResetIndexes(props: ConfirmResetIndexesProps) {
    const { indexes, mode, allActionContexts, onConfirm, closeConfirm } = props;
    const t = useStudioTranslation("indexesList");
    const commonT = useStudioTranslation("common");

    const hasAutoIndexes = indexes.some(IndexUtils.isAutoIndex);
    const hasReplacements = indexes.some(IndexUtils.isSideBySide);

    const getIndexNamesToReset = (): string[] => {
        let indexesToReset = [...indexes];

        if (hasAutoIndexes && mode === "SideBySide") {
            indexesToReset = indexesToReset.filter((x) => !IndexUtils.isAutoIndex(x));
        }
        if (hasReplacements && mode === "SideBySide") {
            indexesToReset = indexesToReset.filter((x) => !IndexUtils.isSideBySide(x));
        }

        return indexesToReset.map((x) => x.name);
    };

    const getSideBySideWarning = (): string => {
        if (mode !== "SideBySide") {
            return null;
        }

        let context: "autoIndexes" | "replacements" | "autoIndexesAndReplacements" = null;

        if (hasAutoIndexes && !hasReplacements) {
            context = "autoIndexes";
        }
        if (hasReplacements && !hasAutoIndexes) {
            context = "replacements";
        }
        if (hasAutoIndexes && hasReplacements) {
            context = "autoIndexesAndReplacements";
        }
        if (context == null) {
            return null;
        }

        return t("resetConfirm.sideBySideWarning", { context });
    };

    const sideBySideWarning = getSideBySideWarning();
    const indexNamesToReset = getIndexNamesToReset();
    const defaultActiveKey = ["0"];

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(indexNamesToReset, selectedActionContexts);
        closeConfirm();
    };

    return (
        <Modal show onHide={closeConfirm} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Header className="hstack pb-2 mb-0" onCloseClick={closeConfirm}>
                <div className="text-center lead">{t("resetConfirm.title")}</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 pt-0">
                <div className="vstack gap-2">
                    <Accordion
                        className="bs5 accordion-inside-modal"
                        alwaysOpen
                        flush
                        defaultActiveKey={defaultActiveKey}
                    >
                        <Accordion.Item eventKey="0">
                            <Accordion.Header>
                                <StudioTrans
                                    ns="indexesList"
                                    i18nKey="resetConfirm.indexesToReset"
                                    components={{ strong: <strong className="text-warning margin-left-xxxs" /> }}
                                />{" "}
                                <Badge className="ms-1 px-1 align-self-center rounded-circle" bg="secondary">
                                    {indexNamesToReset.length}
                                </Badge>
                            </Accordion.Header>
                            <Accordion.Collapse unmountOnExit mountOnEnter eventKey="0">
                                <Accordion.Body className="pb-2 pt-0 overflow-scroll" style={{ maxHeight: "160px" }}>
                                    <div className="vstack gap-1">
                                        {indexNamesToReset.map((indexName) => (
                                            <div key={indexName} className="d-flex">
                                                <div
                                                    className={classNames(
                                                        "bg-secondary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                                    )}
                                                >
                                                    <Icon icon="index" margin="m-0" />
                                                </div>
                                                <div className="word-break align-self-center">{indexName}</div>
                                            </div>
                                        ))}
                                    </div>
                                </Accordion.Body>
                            </Accordion.Collapse>
                        </Accordion.Item>
                    </Accordion>
                    <div className="vstack gap-1">
                        {sideBySideWarning && <RichAlert variant="warning">{sideBySideWarning}</RichAlert>}
                        <RichAlert variant="warning">
                            <div>
                                <StudioTrans
                                    ns="indexesList"
                                    i18nKey="resetConfirm.removeData"
                                    components={{ strong: <strong /> }}
                                    options={{
                                        context: ActionContextUtils.showContextSelector(allActionContexts)
                                            ? "selectedContext"
                                            : "node",
                                        nodeTag: allActionContexts[0].nodeTag,
                                    }}
                                />
                                <br />
                                <span>{t("resetConfirm.reindexInfo")}</span>
                            </div>
                        </RichAlert>
                        <RichAlert variant="info">
                            <StudioTrans
                                ns="indexesList"
                                i18nKey="resetConfirm.mode"
                                components={{ strong: <strong /> }}
                                options={{ context: mode === "InPlace" ? "inPlace" : "sideBySide" }}
                            />
                        </RichAlert>
                    </div>
                </div>
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4 className="fw-light mb-1">{t("resetConfirm.selectContext")}</h4>
                        <MultipleDatabaseLocationSelector
                            allActionContexts={allActionContexts}
                            selectedActionContexts={selectedActionContexts}
                            setSelectedActionContexts={setSelectedActionContexts}
                        />
                    </div>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={closeConfirm} className="link-muted">
                    {commonT("cancel")}
                </Button>
                <Button
                    variant="warning"
                    onClick={onSubmit}
                    className="rounded-pill"
                    disabled={selectedActionContexts.length === 0}
                >
                    <Icon icon="reset-index" />
                    {t("resetConfirm.confirm")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
