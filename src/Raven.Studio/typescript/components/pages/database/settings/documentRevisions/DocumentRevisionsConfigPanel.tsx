import {
    RichPanel,
    RichPanelStatus,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import {
    DocumentRevisionsConfig,
    DocumentRevisionsConfigName,
    documentRevisionsActions,
    documentRevisionsConfigNames,
} from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import classNames from "classnames";
import { useAppDispatch, useAppSelector } from "components/store";
import { Checkbox } from "components/common/Checkbox";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import generalUtils from "common/generalUtils";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface DocumentRevisionsConfigPanelProps {
    config: DocumentRevisionsConfig;
    onToggle: () => void;
    onEdit: () => void;
    onDelete?: () => void;
}

export default function DocumentRevisionsConfigPanel(props: DocumentRevisionsConfigPanelProps) {
    const { config, onDelete, onToggle, onEdit } = props;
    const t = useStudioTranslation("documentRevisions");

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();

    const originalConfig = useAppSelector(documentRevisionsSelectors.originalConfig(config.Name));
    const isSelected = useAppSelector(documentRevisionsSelectors.isSelectedConfigName(config.Name));

    const isModified = !_.isEqual(originalConfig, config);

    const isDeleteOnUpdateVisible =
        (config.MinimumRevisionsToKeep || config.MinimumRevisionAgeToKeep) &&
        config.MaximumRevisionsToDeleteUponDocumentUpdate;

    const isDetailsVisible = config.PurgeOnDelete || config.MinimumRevisionsToKeep || config.MinimumRevisionAgeToKeep;

    const formattedMinimumRevisionAgeToKeep = config.MinimumRevisionAgeToKeep
        ? generalUtils.formatTimeSpan(generalUtils.timeSpanToSeconds(config.MinimumRevisionAgeToKeep) * 1000, true)
        : null;

    return (
        <RichPanel className="flex-row with-status">
            <RichPanelStatus color={config.Disabled ? "warning" : "success"}>
                <div className={classNames({ "my-1": !isDetailsVisible })}>
                    {config.Disabled ? t("panel.statusDisabled") : t("panel.statusEnabled")}
                </div>
            </RichPanelStatus>
            <div className="flex-grow-1">
                <RichPanelHeader className={classNames({ "h-100": !isDetailsVisible })}>
                    <RichPanelInfo>
                        {hasDatabaseAdminAccess && (
                            <RichPanelSelect>
                                <Checkbox
                                    selected={isSelected}
                                    toggleSelection={() =>
                                        dispatch(documentRevisionsActions.selectedConfigNameToggled(config.Name))
                                    }
                                />
                            </RichPanelSelect>
                        )}
                        <RichPanelName>
                            {getConfigNameLabel(config.Name, t)}
                            {isModified && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        <DefaultConfigInfoIcon name={config.Name} />
                        {hasDatabaseAdminAccess && (
                            <>
                                <Button
                                    variant={config.Disabled ? "success" : "secondary"}
                                    onClick={onToggle}
                                    title={t("panel.toggleTitle", { context: config.Disabled ? "enable" : "disable" })}
                                >
                                    <Icon icon={config.Disabled ? "start" : "disable"} />
                                    {config.Disabled ? t("panel.enable") : t("panel.disable")}
                                </Button>

                                <Button variant="secondary" onClick={onEdit} title={t("panel.editTitle")}>
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                                {onDelete && (
                                    <Button
                                        variant="danger"
                                        onClick={() => {
                                            reportEvent("revisions", "remove");
                                            onDelete();
                                        }}
                                        title={t("panel.deleteTitle")}
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                )}
                            </>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                {isDetailsVisible && (
                    <RichPanelDetails>
                        {config.PurgeOnDelete && (
                            <RichPanelDetailItem>
                                <Icon icon="empty-set" />
                                {t("panel.purgeOnDelete")}
                            </RichPanelDetailItem>
                        )}
                        {config.MinimumRevisionsToKeep && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="documents" />
                                        {t("panel.keepLabel")}
                                    </>
                                }
                            >
                                {t("panel.revisionsCount", { count: config.MinimumRevisionsToKeep })}
                            </RichPanelDetailItem>
                        )}
                        {formattedMinimumRevisionAgeToKeep && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="clock" />
                                        {t("panel.retentionTimeLabel")}
                                    </>
                                }
                            >
                                {formattedMinimumRevisionAgeToKeep}
                            </RichPanelDetailItem>
                        )}
                        {isDeleteOnUpdateVisible && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="trash" />
                                        {t("panel.deleteOnUpdateLabel")}
                                    </>
                                }
                            >
                                {t("panel.revisionsCount", {
                                    count: config.MaximumRevisionsToDeleteUponDocumentUpdate,
                                })}
                            </RichPanelDetailItem>
                        )}
                    </RichPanelDetails>
                )}
            </div>
        </RichPanel>
    );
}

function getConfigNameLabel(name: DocumentRevisionsConfigName, t: StudioTranslate<"documentRevisions">): string {
    switch (name) {
        case documentRevisionsConfigNames.defaultDocument:
            return t("configNames.defaultDocument");
        case documentRevisionsConfigNames.defaultConflicts:
            return t("configNames.defaultConflicts");
        default:
            return name;
    }
}

function DefaultConfigInfoIcon({ name }: { name: DocumentRevisionsConfigName }) {
    const t = useStudioTranslation("documentRevisions");
    const isDefault =
        name === documentRevisionsConfigNames.defaultDocument || name === documentRevisionsConfigNames.defaultConflicts;

    if (!isDefault) {
        return null;
    }

    return (
        <PopoverWithHoverWrapper
            placement="left"
            message={
                <ul>
                    {name === documentRevisionsConfigNames.defaultConflicts ? (
                        <>
                            <li>
                                <StudioTrans
                                    ns="documentRevisions"
                                    i18nKey="panel.conflictsInfo.intro"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                            <li>{t("panel.conflictsInfo.createdItem")}</li>
                            <li>{t("panel.conflictsInfo.resolutionItem")}</li>
                            <li>
                                <StudioTrans
                                    ns="documentRevisions"
                                    i18nKey="panel.conflictsInfo.overrideItem"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                        </>
                    ) : (
                        <>
                            <li>
                                <StudioTrans
                                    ns="documentRevisions"
                                    i18nKey="panel.documentInfo.intro"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                            <li>{t("panel.documentInfo.createdItem")}</li>
                            <li>
                                <StudioTrans
                                    ns="documentRevisions"
                                    i18nKey="panel.documentInfo.overrideItem"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                        </>
                    )}
                </ul>
            }
        >
            <Icon icon="info" />
        </PopoverWithHoverWrapper>
    );
}
