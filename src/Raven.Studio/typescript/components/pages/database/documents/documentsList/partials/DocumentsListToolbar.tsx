import queryUtil from "common/queryUtil";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { AccessPopover } from "components/common/AccessPopover";
import { Switch } from "components/common/Checkbox";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import ExportDocumentsDropdown from "components/pages/database/documents/documentsList/partials/ExportDocumentsDropdown";
import { useAppSelector } from "components/store";
import queryCriteria from "models/database/query/queryCriteria";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DocumentsListToolbarProps {
    collectionName: string | null;
    isPaginated: boolean;
    onIsPaginatedChange: (isPaginated: boolean) => void;
    isCustomLayout: boolean;
    onOpenColumnSettings: () => void;
    getVisibleColumnFields: () => string[];
    isDataChanged: boolean;
    onDataChangedRefresh: () => void;
}

export default function DocumentsListToolbar({
    collectionName,
    isPaginated,
    onIsPaginatedChange,
    isCustomLayout,
    onOpenColumnSettings,
    getVisibleColumnFields,
    isDataChanged,
    onDataChangedRefresh,
}: DocumentsListToolbarProps) {
    const t = useStudioTranslation("documentsList");
    const commonT = useStudioTranslation("common");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();

    const navigateToQuery = () => {
        const collectionNameForQuery = collectionName ?? "@all_docs";

        const query = queryCriteria.empty();
        query.queryText("from " + queryUtil.wrapWithSingleQuotes(collectionNameForQuery));
        query.name(t("toolbar.recentQueryName", { collectionName: collectionNameForQuery }));
        query.recentQuery(true);

        savedQueriesStorage.saveAndNavigate(databaseName, query.toStorageDto());
    };

    const handleDataChangedRefresh = () => {
        reportEvent("documents", "refresh");
        onDataChangedRefresh();
    };

    return (
        <div className="d-flex align-items-start justify-content-between gap-3 flex-wrap mb-3">
            <AccessPopover accessRequired="DatabaseReadWrite">
                <Dropdown as={ButtonGroup}>
                    <Button
                        variant="primary"
                        href={appUrl.forNewDoc(databaseName)}
                        onClick={() => reportEvent("document", "new")}
                        disabled={!hasDatabaseWriteAccess}
                    >
                        <Icon icon="new-document" />
                        {t("toolbar.newDocument")}
                    </Button>
                    {collectionName && (
                        <>
                            <Dropdown.Toggle
                                variant="primary"
                                as={CustomDropdownToggle}
                                disabled={!hasDatabaseWriteAccess}
                                title={t("toolbar.moreOptions")}
                            />
                            <Dropdown.Menu>
                                <Dropdown.Item
                                    href={appUrl.forNewDoc(databaseName, collectionName)}
                                    onClick={() => reportEvent("document", "new-in-collection")}
                                >
                                    <Icon icon="new-document" />
                                    {t("toolbar.newDocumentInCollection")}
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </>
                    )}
                </Dropdown>
            </AccessPopover>
            <div className="d-flex align-items-center gap-2 flex-wrap">
                {isDataChanged && (
                    <RichAlert
                        variant="warning"
                        className="py-0 px-1 mb-0 align-items-center"
                        childrenClassName="w-auto"
                        data-testid="data-changed-alert"
                    >
                        {t("toolbar.dataChanged")}{" "}
                        <Button variant="link" className="p-0 align-baseline" onClick={handleDataChangedRefresh}>
                            {commonT("refresh")}
                        </Button>
                    </RichAlert>
                )}
                <div className="d-flex align-items-center gap-2 flex-wrap">
                    <Button variant="secondary" onClick={navigateToQuery} title={t("toolbar.queryTitle")}>
                        <Icon icon="query" />
                        {t("toolbar.query")}
                    </Button>
                    {collectionName && (
                        <ExportDocumentsDropdown
                            collectionName={collectionName}
                            getVisibleColumnFields={getVisibleColumnFields}
                        />
                    )}
                    <Dropdown>
                        <Dropdown.Toggle
                            as={CustomDropdownToggle}
                            variant="secondary"
                            active={isCustomLayout}
                            title={t("toolbar.displayTitle", { context: isCustomLayout ? "custom" : undefined })}
                        >
                            <Icon icon="table" />
                            {t("toolbar.display")}
                        </Dropdown.Toggle>
                        <Dropdown.Menu align="end">
                            <Dropdown.Item onClick={onOpenColumnSettings}>
                                <Icon icon="table" />
                                {t("toolbar.columnLayoutSettings")}
                            </Dropdown.Item>
                            <Dropdown.Divider />
                            <Dropdown.ItemText>
                                <ConditionalPopover
                                    conditions={{
                                        isActive: isSharded,
                                        message: t("toolbar.paginationNotAvailable"),
                                    }}
                                >
                                    <Switch
                                        selected={isPaginated}
                                        toggleSelection={() => onIsPaginatedChange(!isPaginated)}
                                        color="primary"
                                        title={isSharded ? null : t("toolbar.paginationTitle")}
                                        disabled={isSharded}
                                    >
                                        {t("toolbar.pagination")}
                                    </Switch>
                                </ConditionalPopover>
                            </Dropdown.ItemText>
                        </Dropdown.Menu>
                    </Dropdown>
                </div>
            </div>
        </div>
    );
}
