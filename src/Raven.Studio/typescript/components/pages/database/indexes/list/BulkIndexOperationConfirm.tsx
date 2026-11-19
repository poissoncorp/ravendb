import React, { ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import { IndexSharedInfo } from "components/models/indexes";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import ActionContextUtils from "components/utils/actionContextUtils";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import Modal from "components/common/Modal";
import Accordion from "react-bootstrap/Accordion";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

type operationType = "pause" | "disable" | "start";

interface IndexGroup {
    title: string | ReactNode;
    indexes: IndexInfo[];
    destinationStatus?: IndexRunningStatus;
}

interface IndexInfo {
    name: string;
    currentStatus: IndexRunningStatus;
}

interface AffectedIndexesGrouped {
    disabling?: IndexInfo[];
    pausing?: IndexInfo[];
    enabling?: IndexInfo[];
    resuming?: IndexInfo[];
    skipping?: IndexInfo[];
}

interface BulkIndexOperationConfirmProps {
    type: operationType;
    indexes: IndexSharedInfo[];
    toggle: () => void;
    allActionContexts: DatabaseActionContexts[];
    onConfirm: (contextPoints: DatabaseActionContexts[]) => void;
}

export function BulkIndexOperationConfirm(props: BulkIndexOperationConfirmProps) {
    const { type, indexes, toggle, allActionContexts, onConfirm } = props;
    const t = useStudioTranslation("indexesList");
    const tCommon = useStudioTranslation("common");

    const icon = getIcon(type);

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const indexGroups = getIndexGroups(type, indexes, t).filter((x) => x.indexes.length > 0);
    const defaultActiveKey = indexGroups.length === 1 ? ["indexGroup0"] : [];

    const onSubmit = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal
            show
            scrollable
            onHide={toggle}
            contentClassName={`modal-border bulge-${getColorForType(type)}`}
            size="lg"
        >
            <Modal.Header className="hstack pb-2 mb-0" onCloseClick={toggle}>
                <div className="text-center lead">{t("bulkConfirm.title", { context: type })}</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-1 pt-0">
                {indexGroups.map((indexGroup, idx) => {
                    const groupEventKey = "indexGroup" + idx;

                    return (
                        <Accordion
                            key={groupEventKey}
                            className="bs5 accordion-inside-modal"
                            defaultActiveKey={defaultActiveKey}
                            alwaysOpen
                            flush
                        >
                            <Accordion.Item eventKey={groupEventKey}>
                                <Accordion.Header>
                                    {indexGroup.title}{" "}
                                    <Badge className="ms-1 px-1 align-self-center rounded-circle" bg="secondary">
                                        {indexGroup.indexes.length}
                                    </Badge>
                                </Accordion.Header>
                                <Accordion.Collapse unmountOnExit mountOnEnter eventKey={groupEventKey}>
                                    <Accordion.Body
                                        className="pb-2 pt-0 overflow-scroll"
                                        style={{ maxHeight: "160px" }}
                                    >
                                        <div className="vstack gap-1">
                                            {indexGroup.indexes.map((index) => (
                                                <div key={index.name} className="d-flex">
                                                    <div
                                                        className={classNames(
                                                            "bg-secondary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                                        )}
                                                    >
                                                        <Icon
                                                            icon={getStatusIcon(index.currentStatus)}
                                                            color={getStatusColor(index.currentStatus)}
                                                            title={getStatusTitle(index.currentStatus, t)}
                                                            margin="m-0"
                                                        />
                                                        {indexGroup.destinationStatus && (
                                                            <>
                                                                <Icon
                                                                    icon="arrow-thin-right"
                                                                    margin="mx-1"
                                                                    className="fs-6 align-self-center"
                                                                />
                                                                <Icon
                                                                    icon={getStatusIcon(indexGroup.destinationStatus)}
                                                                    color={getStatusColor(indexGroup.destinationStatus)}
                                                                    title={getStatusTitle(
                                                                        indexGroup.destinationStatus,
                                                                        t
                                                                    )}
                                                                    margin="m-0"
                                                                />
                                                            </>
                                                        )}
                                                    </div>
                                                    <div className="word-break align-self-center">{index.name}</div>
                                                </div>
                                            ))}
                                        </div>
                                    </Accordion.Body>
                                </Accordion.Collapse>
                            </Accordion.Item>
                        </Accordion>
                    );
                })}
                <div className="mt-2">
                    {ActionContextUtils.showContextSelector(allActionContexts) && (
                        <div>
                            <h4 className="fw-light mb-1">{t("bulkConfirm.selectContext")}</h4>
                            <MultipleDatabaseLocationSelector
                                allActionContexts={allActionContexts}
                                selectedActionContexts={selectedActionContexts}
                                setSelectedActionContexts={setSelectedActionContexts}
                            />
                        </div>
                    )}
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    {tCommon("cancel")}
                </Button>
                <Button variant={getColorForType(type)} onClick={onSubmit} className="rounded-pill">
                    <Icon icon={icon} /> {t("bulkConfirm.confirm", { context: type })}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function getColorForType(type: operationType) {
    switch (type) {
        case "pause":
            return "warning";
        case "disable":
            return "danger";
        case "start":
            return "success";
        default:
            return "primary";
    }
}

function getStatusIcon(status: IndexRunningStatus) {
    switch (status) {
        case "Disabled":
            return "stop";
        case "Paused":
            return "pause";
        case "Running":
            return "play";
        default:
            return "index";
    }
}

function getStatusColor(status: IndexRunningStatus) {
    switch (status) {
        case "Disabled":
            return "danger";
        case "Paused":
            return "warning";
        case "Running":
            return "success";
        default:
            return "primary";
    }
}

function getStatusTitle(status: IndexRunningStatus, t: StudioTranslate<"indexesList">) {
    switch (status) {
        case "Disabled":
            return t("indexStatus.disabled");
        case "Paused":
            return t("indexStatus.paused");
        case "Running":
            return t("indexStatus.running");
        case "Pending":
            return t("indexStatus.pending");
        default:
            assertUnreachable(status);
    }
}

function getIcon(type: operationType) {
    switch (type) {
        case "disable":
            return "stop";
        case "pause":
            return "pause";
        case "start":
            return "play";
        default:
            assertUnreachable(type);
    }
}
function getIndexGroups(
    type: operationType,
    indexes: IndexSharedInfo[],
    t: StudioTranslate<"indexesList">
): IndexGroup[] {
    switch (type) {
        case "disable": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Disabled")) {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Disabled" });
                    } else {
                        if (currentValue.nodesInfo.every((x) => x.details?.status === "Paused")) {
                            accumulator.disabling.push({ name: currentValue.name, currentStatus: "Paused" });
                        } else {
                            accumulator.disabling.push({ name: currentValue.name, currentStatus: "Running" });
                        }
                    }

                    return accumulator;
                },
                {
                    disabling: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="bulkConfirm.toDisable"
                            components={{ strong: <strong className="text-danger margin-left-xxxs" /> }}
                        />
                    ),
                    indexes: affectedIndexGrouped.disabling,

                    destinationStatus: "Disabled",
                },
                {
                    title: t("bulkConfirm.skippingDisabled"),
                    indexes: affectedIndexGrouped.skipping,
                },
            ];
        }
        case "pause": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Paused")) {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Paused" });
                    } else if (currentValue.nodesInfo.every((x) => x.details?.status === "Disabled")) {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Disabled" });
                    } else {
                        accumulator.pausing.push({ name: currentValue.name, currentStatus: "Running" });
                    }

                    return accumulator;
                },
                {
                    pausing: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="bulkConfirm.toPause"
                            components={{ strong: <strong className="text-warning margin-left-xxxs" /> }}
                        />
                    ),
                    indexes: affectedIndexGrouped.pausing,

                    destinationStatus: "Paused",
                },
                {
                    title: t("bulkConfirm.skippingPausedOrDisabled"),
                    indexes: affectedIndexGrouped.skipping,
                },
            ];
        }
        case "start": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Paused")) {
                        accumulator.resuming.push({ name: currentValue.name, currentStatus: "Paused" });
                    } else {
                        if (currentValue.nodesInfo.some((x) => x.details?.status === "Disabled")) {
                            accumulator.enabling.push({ name: currentValue.name, currentStatus: "Disabled" });
                        } else {
                            accumulator.enabling.push({ name: currentValue.name, currentStatus: "Running" });
                        }
                    }

                    return accumulator;
                },
                {
                    enabling: [],
                    resuming: [],
                }
            );
            return [
                {
                    title: (
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="bulkConfirm.toEnable"
                            components={{ strong: <strong className="text-success margin-left-xxxs" /> }}
                        />
                    ),
                    indexes: affectedIndexGrouped.enabling,

                    destinationStatus: "Running",
                },
                {
                    title: (
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="bulkConfirm.toResume"
                            components={{ strong: <strong className="text-success margin-left-xxxs" /> }}
                        />
                    ),
                    indexes: affectedIndexGrouped.resuming,

                    destinationStatus: "Running",
                },
            ];
        }
        default:
            assertUnreachable(type);
    }
}
