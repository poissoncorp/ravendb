import React, { ForwardedRef, forwardRef, MouseEvent, useState } from "react";
import classNames from "classnames";
import { IndexSharedInfo } from "components/models/indexes";
import { useAppUrls } from "hooks/useAppUrls";
import IndexUtils from "../../../../utils/IndexUtils";
import { useEventsCollector } from "hooks/useEventsCollector";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import app from "durandal/app";
import { IndexDistribution, JoinedIndexProgress } from "./IndexDistribution";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
} from "components/common/RichPanel";
import Spinner from "react-bootstrap/Spinner";
import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import assertUnreachable from "components/utils/assertUnreachable";
import useUniqueId from "components/hooks/useUniqueId";
import useBoolean from "hooks/useBoolean";
import { Icon } from "components/common/Icon";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import ResetIndexesButton from "components/pages/database/indexes/list/partials/ResetIndexesButton";
import { ExportIndexes } from "components/pages/database/indexes/list/migration/export/ExportIndexes";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexSourceType = Raven.Client.Documents.Indexes.IndexSourceType;
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

export interface IndexPanelProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    setPriority: (priority: IndexPriority) => Promise<void>;
    setLockMode: (lockMode: IndexLockMode) => Promise<void>;
    startIndexing: () => Promise<void>;
    disableIndexing: () => Promise<void>;
    pauseIndexing: () => Promise<void>;
    deleteIndex: () => Promise<void>;
    resetIndex: (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
    openFaulty: (location: databaseLocationSpecifier) => Promise<void>;
    selected: boolean;
    hasReplacement?: boolean;
    toggleSelection: () => void;
    ref?: any;
}

export const IndexPanel = forwardRef(IndexPanelInternal);

function getPriorityColor(index: IndexSharedInfo) {
    switch (index.priority) {
        case "Normal":
            return "outline-secondary";
        case "High":
            return "outline-warning";
        case "Low":
            return "outline-info";
        default:
            assertUnreachable(index.priority);
    }
}

function getLockColor(index: IndexSharedInfo) {
    switch (index.lockMode) {
        case "LockedIgnore":
            return "outline-warning";
        case "LockedError":
            return "outline-warning";
        case "Unlock":
            return "outline-secondary";
        default:
            assertUnreachable(index.lockMode);
    }
}

export function IndexPanelInternal(props: IndexPanelProps, ref: ForwardedRef<HTMLDivElement>) {
    const { index, selected, toggleSelection, hasReplacement, globalIndexingStatus, resetIndex } = props;
    const t = useStudioTranslation("indexesList");

    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);

    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);

    const isReplacement = IndexUtils.isSideBySide(index);
    const isAnyFaulty = IndexUtils.hasAnyFaultyNode(index);
    const localFaultyNodeInfo = IndexUtils.getFaultyNodeInfo(index, localNodeTag);

    const eventsCollector = useEventsCollector();

    const [updatingLocalPriority, setUpdatingLocalPriority] = useState(false);
    const [updatingLockMode, setUpdatingLockMode] = useState(false);
    const [updatingState, setUpdatingState] = useState(false);

    const { value: isExportIndexModalOpen, toggle: toggleIsExportIndexModalOpen } = useBoolean(false);

    const setPriority = async (e: MouseEvent, priority: IndexPriority) => {
        e.preventDefault();
        if (priority !== index.priority) {
            setUpdatingLocalPriority(true);
            try {
                await props.setPriority(priority);
            } finally {
                setUpdatingLocalPriority(false);
            }
        }
    };

    const setLockMode = async (e: MouseEvent, lockMode: IndexLockMode) => {
        e.preventDefault();
        if (lockMode !== index.lockMode) {
            setUpdatingLockMode(true);
            try {
                await props.setLockMode(lockMode);
            } finally {
                setUpdatingLockMode(false);
            }
        }
    };

    const startIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "set-state", "enabled");
        setUpdatingState(true);
        try {
            await props.startIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const disableIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "set-state", "disabled");
        setUpdatingState(true);
        try {
            await props.disableIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const pauseIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "pause");
        setUpdatingState(true);
        try {
            await props.pauseIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const deleteIndex = async (e: MouseEvent) => {
        e.preventDefault();
        return props.deleteIndex();
    };

    const showStaleReasons = (index: IndexSharedInfo, location: databaseLocationSpecifier) => {
        const view = new indexStalenessReasons(databaseName, index.name, location);
        eventsCollector.reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    };

    const openFaulty = async (location: databaseLocationSpecifier) => {
        await props.openFaulty(location);
    };

    const { forCurrentDatabase: urls } = useAppUrls();
    const queryUrl = urls.query(index.name)();
    const termsUrl = urls.terms(index.name)();
    const editUrl = urls.editIndex(index.name)();

    const priorityButtonColor = getPriorityColor(index);
    const lockButtonColor = getLockColor(index);

    const reduceOutputId = useUniqueId("reduce-output-id");

    return (
        <>
            <RichPanel className={classNames({ "index-sidebyside": hasReplacement || isReplacement })} innerRef={ref}>
                <RichPanelHeader id={indexUniqueId(index)}>
                    <RichPanelInfo>
                        <RichPanelSelect>
                            {hasDatabaseWriteAccess && (
                                <Form.Check type="checkbox" onChange={toggleSelection} checked={selected} />
                            )}
                        </RichPanelSelect>
                        <div className="flex-grow">
                            <RichPanelName>
                                <a href={editUrl} title={index.name}>
                                    {index.name}
                                </a>
                            </RichPanelName>
                        </div>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <>
                                {!IndexUtils.isSideBySide(index) && (
                                    <Dropdown>
                                        <Dropdown.Toggle
                                            as={CustomDropdownToggle}
                                            isCaretHidden
                                            variant={priorityButtonColor}
                                            disabled={!hasDatabaseWriteAccess}
                                        >
                                            {updatingLocalPriority && <Spinner size="sm" className="me-2" />}
                                            {!updatingLocalPriority && index.priority === "Normal" && (
                                                <span>
                                                    <Icon icon="check" />
                                                    <span>{t("panel.priority.normal")}</span>
                                                </span>
                                            )}
                                            {!updatingLocalPriority && index.priority === "Low" && (
                                                <span>
                                                    <Icon icon="coffee" />
                                                    <span>{t("panel.priority.low")}</span>
                                                </span>
                                            )}
                                            {!updatingLocalPriority && index.priority === "High" && (
                                                <span>
                                                    <Icon icon="force" />
                                                    <span>{t("panel.priority.high")}</span>
                                                </span>
                                            )}
                                        </Dropdown.Toggle>

                                        <Dropdown.Menu>
                                            <Dropdown.Item
                                                onClick={(e) => setPriority(e, "Low")}
                                                title={t("panel.priority.lowTitle")}
                                            >
                                                <Icon icon="coffee" /> <span>{t("panel.priority.low")}</span>
                                            </Dropdown.Item>
                                            <Dropdown.Item
                                                onClick={(e) => setPriority(e, "Normal")}
                                                title={t("panel.priority.normalTitle")}
                                            >
                                                <Icon icon="check" /> <span>{t("panel.priority.normal")}</span>
                                            </Dropdown.Item>
                                            <Dropdown.Item
                                                onClick={(e) => setPriority(e, "High")}
                                                title={t("panel.priority.highTitle")}
                                            >
                                                <Icon icon="force" /> <span>{t("panel.priority.high")}</span>
                                            </Dropdown.Item>
                                        </Dropdown.Menu>
                                    </Dropdown>
                                )}

                                {index.type !== "AutoMap" &&
                                    index.type !== "AutoMapReduce" &&
                                    !IndexUtils.isSideBySide(index) && (
                                        <Dropdown>
                                            <Dropdown.Toggle
                                                as={CustomDropdownToggle}
                                                isCaretHidden
                                                variant={lockButtonColor}
                                                disabled={!hasDatabaseWriteAccess}
                                            >
                                                {updatingLockMode && <Spinner size="sm" className="me-2" />}
                                                {index.lockMode === "Unlock" && (
                                                    <span>
                                                        <Icon icon="unlock" />
                                                        <span>{t("panel.lockMode.unlocked")}</span>
                                                    </span>
                                                )}
                                                {index.lockMode === "LockedIgnore" && (
                                                    <span>
                                                        <Icon icon="lock" />
                                                        <span>{t("panel.lockMode.locked")}</span>
                                                    </span>
                                                )}
                                                {index.lockMode === "LockedError" && (
                                                    <span>
                                                        <Icon icon="lock-error" />
                                                        <span>{t("panel.lockMode.lockedError")}</span>
                                                    </span>
                                                )}
                                            </Dropdown.Toggle>

                                            <Dropdown.Menu>
                                                <Dropdown.Item
                                                    onClick={(e) => setLockMode(e, "Unlock")}
                                                    title={t("panel.lockMode.unlockTitle")}
                                                >
                                                    <Icon icon="unlock" /> <span>{t("actions.unlock")}</span>
                                                </Dropdown.Item>
                                                <Dropdown.Divider />
                                                <Dropdown.Item
                                                    onClick={(e) => setLockMode(e, "LockedIgnore")}
                                                    title={t("panel.lockMode.lockTitle")}
                                                >
                                                    <Icon icon="lock" /> <span>{t("actions.lock")}</span>
                                                </Dropdown.Item>
                                                <Dropdown.Item
                                                    onClick={(e) => setLockMode(e, "LockedError")}
                                                    title={t("panel.lockMode.lockErrorTitle")}
                                                >
                                                    <Icon icon="lock-error" /> <span>{t("actions.lockError")}</span>
                                                </Dropdown.Item>
                                            </Dropdown.Menu>
                                        </Dropdown>
                                    )}
                            </>
                        )}

                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <Dropdown>
                                <Dropdown.Toggle
                                    as={CustomDropdownToggle}
                                    isCaretHidden
                                    variant="secondary"
                                    data-bind="css: { 'btn-spinner': _.includes($root.spinners.localState(), name) },
                                           enable: $root.globalIndexingStatus() === 'Running'  && !_.includes($root.spinners.localState(), name),
                                           requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }"
                                >
                                    {updatingState && <Spinner size="sm" className="me-2" />}
                                    <span>{t("panel.setState")}</span>
                                </Dropdown.Toggle>

                                <Dropdown.Menu>
                                    <Dropdown.Item onClick={startIndexing} title={t("actions.startIndexing")}>
                                        <Icon icon="play" /> <span>{t("actions.startIndexing")}</span>
                                    </Dropdown.Item>
                                    <Dropdown.Item onClick={disableIndexing} title={t("actions.disableIndexing")}>
                                        <Icon icon="stop" color="danger" /> <span>{t("actions.disableIndexing")}</span>
                                    </Dropdown.Item>
                                    <Dropdown.Item onClick={pauseIndexing} title={t("panel.pauseIndexingTitle")}>
                                        <Icon icon="pause" color="warning" /> <span>{t("actions.pauseIndexing")}</span>
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        )}

                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <Dropdown as={ButtonGroup}>
                                <Button variant="secondary" href={queryUrl}>
                                    <Icon icon="search" />
                                    <span>{t("panel.query")}</span>
                                </Button>
                                <Dropdown.Toggle variant="secondary" className="dropdown-toggle" />

                                <Dropdown.Menu>
                                    <Dropdown.Item href={termsUrl}>
                                        {" "}
                                        <Icon icon="terms" /> {t("panel.terms")}{" "}
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        )}

                        <ButtonGroup>
                            {!IndexUtils.isAutoIndex(index) && hasDatabaseWriteAccess && (
                                <Button variant="secondary" href={editUrl} title={t("panel.editTitle")}>
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                            )}
                            {(IndexUtils.isAutoIndex(index) || !hasDatabaseWriteAccess) && (
                                <Button href={editUrl} variant="secondary" title={t("panel.viewTitle")}>
                                    <Icon icon="preview" margin="m-0" />
                                </Button>
                            )}
                        </ButtonGroup>

                        {localFaultyNodeInfo && (
                            <Button variant="secondary" onClick={() => openFaulty(localFaultyNodeInfo.location)}>
                                {t("actions.openFaulty")}
                            </Button>
                        )}
                        {!IndexUtils.isAutoIndex(index) && (
                            <>
                                <Button
                                    variant="secondary"
                                    title={t("panel.exportTitle")}
                                    onClick={toggleIsExportIndexModalOpen}
                                >
                                    <Icon icon="export" margin="m-0" />
                                </Button>
                                {isExportIndexModalOpen && (
                                    <ExportIndexes
                                        indexes={[index]}
                                        selectedNames={[index.name]}
                                        toggle={toggleIsExportIndexModalOpen}
                                    />
                                )}
                            </>
                        )}
                        {hasDatabaseWriteAccess && (
                            <>
                                <ResetIndexesButton
                                    resetIndex={resetIndex}
                                    sideBySideDisabledReason={getSideBySideResetDisabledReason(index, t)}
                                />
                                <Button variant="danger" onClick={deleteIndex} title={t("panel.deleteTitle")}>
                                    <Icon icon="trash" margin="m-0" />
                                </Button>
                            </>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails className="pb-1">
                    <RichPanelDetailItem>
                        <Button
                            variant="secondary"
                            onClick={togglePanelCollapsed}
                            title={panelCollapsed ? t("panel.expandDetailsTitle") : t("panel.collapseDetailsTitle")}
                            className="btn-toggle-panel rounded-pill"
                        >
                            <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                        </Button>
                    </RichPanelDetailItem>
                    {(index.reduceOutputCollectionName || index.patternForReferencesToReduceOutputCollection) && (
                        <RichPanelDetailItem>
                            <OverlayTrigger
                                placement="right"
                                overlay={
                                    <Tooltip id={reduceOutputId}>
                                        <>
                                            {index.reduceOutputCollectionName && (
                                                <span>
                                                    <StudioTrans
                                                        ns="indexesList"
                                                        i18nKey="panel.reduceResultsCollection"
                                                        components={{ br: <br />, strong: <strong /> }}
                                                        options={{ collection: index.reduceOutputCollectionName }}
                                                    />
                                                </span>
                                            )}
                                            {index.collectionNameForReferenceDocuments && (
                                                <span>
                                                    <br />
                                                    <StudioTrans
                                                        ns="indexesList"
                                                        i18nKey="panel.referencingDocumentsCollection"
                                                        components={{ br: <br />, strong: <strong /> }}
                                                        options={{
                                                            collection: index.collectionNameForReferenceDocuments,
                                                        }}
                                                    />
                                                </span>
                                            )}
                                            {!index.collectionNameForReferenceDocuments &&
                                                index.patternForReferencesToReduceOutputCollection && (
                                                    <span>
                                                        <br />
                                                        <StudioTrans
                                                            ns="indexesList"
                                                            i18nKey="panel.referencingDocumentsCollection"
                                                            components={{ br: <br />, strong: <strong /> }}
                                                            options={{
                                                                collection:
                                                                    index.reduceOutputCollectionName + "/References",
                                                            }}
                                                        />
                                                    </span>
                                                )}
                                        </>
                                    </Tooltip>
                                }
                            >
                                <div className="index-type-icon" id={reduceOutputId}>
                                    {index.reduceOutputCollectionName &&
                                        !index.patternForReferencesToReduceOutputCollection && (
                                            <span>
                                                <Icon icon="output-collection" margin="m-0" />
                                            </span>
                                        )}
                                    {index.patternForReferencesToReduceOutputCollection && (
                                        <span>
                                            <Icon icon="reference-pattern" margin="m-0" />
                                        </span>
                                    )}
                                </div>
                            </OverlayTrigger>
                        </RichPanelDetailItem>
                    )}
                    <ReferencedCollections collections={index.referencedCollections} />
                    {(hasReplacement || isReplacement) && (
                        <RichPanelDetailItem>
                            {hasReplacement && (
                                <Badge pill bg="warning" className="ms-3">
                                    {t("panel.oldBadge")}
                                </Badge>
                            )}
                            {isReplacement && (
                                <Badge pill bg="warning" className="ms-3">
                                    {t("panel.newBadge")}
                                </Badge>
                            )}
                        </RichPanelDetailItem>
                    )}
                    <RichPanelDetailItem className={isAnyFaulty ? "text-danger" : ""}>
                        <Icon icon={IndexUtils.indexTypeIcon(index.type)} />
                        {IndexUtils.formatType(index.type)}
                    </RichPanelDetailItem>
                    <IndexSourceTypeComponent sourceType={index.sourceType} />
                    <RichPanelDetailItem>
                        <Icon icon="search" />
                        {index.searchEngine}
                    </RichPanelDetailItem>

                    {!isAnyFaulty && (
                        <InlineDetails
                            index={index}
                            toggleLocationDetails={togglePanelCollapsed}
                            errorsUrl={urls.indexErrors()}
                        />
                    )}
                </RichPanelDetails>
                <div className="px-3 pb-2">
                    <Collapse in={!panelCollapsed}>
                        <div>
                            <IndexDistribution
                                index={index}
                                globalIndexingStatus={globalIndexingStatus}
                                showStaleReason={(location) => showStaleReasons(index, location)}
                                openFaulty={openFaulty}
                            />
                        </div>
                    </Collapse>
                </div>
            </RichPanel>
        </>
    );
}

function IndexSourceTypeComponent(props: { sourceType: IndexSourceType }) {
    const { sourceType } = props;
    const t = useStudioTranslation("indexesList");

    return (
        <RichPanelDetailItem>
            {sourceType === "Counters" && (
                <>
                    <Icon icon="new-counter" title={t("panel.source.countersTitle")} />
                    {t("panel.source.counters")}
                </>
            )}
            {sourceType === "TimeSeries" && (
                <>
                    <Icon icon="timeseries" title={t("panel.source.timeSeriesTitle")} />
                    {t("panel.source.timeSeries")}
                </>
            )}
            {sourceType === "Documents" && (
                <>
                    <Icon icon="documents" title={t("panel.source.documentsTitle")} />
                    {t("panel.source.documents")}
                </>
            )}
        </RichPanelDetailItem>
    );
}

interface InlineDetailsProps {
    index: IndexSharedInfo;
    toggleLocationDetails: () => void;
    errorsUrl: string;
}

function InlineDetails(props: InlineDetailsProps) {
    const { index, toggleLocationDetails, errorsUrl } = props;
    const t = useStudioTranslation("indexesList");

    const estimatedEntries = IndexUtils.estimateEntriesCount(index)?.toLocaleString() ?? "-";
    const errorsCount = index.nodesInfo.filter((x) => x.details).reduce((prev, x) => prev + x.details.errorCount, 0);

    return (
        <>
            <RichPanelDetailItem>
                <Icon icon="list" />
                {t("panel.entries")}
                <div className="value">{estimatedEntries}</div>
            </RichPanelDetailItem>
            {errorsCount > 0 && (
                <a href={errorsUrl} title={t("panel.errorsTitle")}>
                    <RichPanelDetailItem className="index-detail-item text-danger">
                        <Icon icon="warning" />
                        {t("panel.errors")}
                        <div className="value">{errorsCount.toLocaleString()}</div>
                    </RichPanelDetailItem>
                </a>
            )}
            <RichPanelDetailItem>
                <JoinedIndexProgress index={index} onClick={toggleLocationDetails} />
            </RichPanelDetailItem>
        </>
    );
}

interface ReferencedCollectionsProps {
    collections: string[];
}

function ReferencedCollections({ collections }: ReferencedCollectionsProps) {
    const t = useStudioTranslation("indexesList");

    if (!collections?.length) {
        return null;
    }

    return (
        <RichPanelDetailItem>
            <Icon icon="referenced-collections" title={t("panel.referencedCollectionsTitle")} />
            <div style={{ maxWidth: "300px" }}>{collections.join(", ")}</div>
        </RichPanelDetailItem>
    );
}

export default ReferencedCollections;

const indexUniqueId = (index: IndexSharedInfo) => "index_" + index.name;

function getSideBySideResetDisabledReason(index: IndexSharedInfo, t: StudioTranslate<"indexesList">) {
    if (IndexUtils.isSideBySide(index)) {
        return t("panel.resetDisabledReason.replacement");
    }
    if (IndexUtils.isAutoIndex(index)) {
        return t("panel.resetDisabledReason.autoIndex");
    }
    if (index.type === "MapReduce" && index.reduceOutputCollectionName != null) {
        return t("panel.resetDisabledReason.mapReduceOutput");
    }

    return null;
}
