import { useEffect, useState } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import EditRevision, {
    EditRevisionConfigType,
    EditRevisionTaskType,
} from "components/pages/database/settings/documentRevisions/EditRevision";
import EnforceConfiguration from "components/pages/database/settings/documentRevisions/EnforceConfiguration";
import { LoadingView } from "components/common/LoadingView";
import { documentRevisionsActions, DocumentRevisionsConfig } from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { LoadError } from "components/common/LoadError";
import DocumentRevisionsConfigPanel from "./DocumentRevisionsConfigPanel";
import useBoolean from "components/hooks/useBoolean";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import notificationCenter from "common/notifications/notificationCenter";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import DocumentRevisionsSelectActions from "./DocumentRevisionsSelectActions";
import { StickyHeader } from "components/common/StickyHeader";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAppUrls } from "components/hooks/useAppUrls";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import Button from "react-bootstrap/Button";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EditRevisionData {
    onConfirm: (config: DocumentRevisionsConfig) => void;
    configType: EditRevisionConfigType;
    taskType: EditRevisionTaskType;
    toggle: () => void;
    config?: DocumentRevisionsConfig;
}

export default function DocumentRevisions() {
    const t = useStudioTranslation("documentRevisions");
    const commonT = useStudioTranslation("common");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { value: isEnforceConfigurationModalOpen, toggle: toggleEnforceConfigurationModal } = useBoolean(false);
    const [editRevisionData, setEditRevisionData] = useState<EditRevisionData>(null);

    const loadStatus = useAppSelector(documentRevisionsSelectors.loadStatus);
    const defaultDocumentsConfig = useAppSelector(documentRevisionsSelectors.defaultDocumentsConfig);
    const defaultConflictsConfig = useAppSelector(documentRevisionsSelectors.defaultConflictsConfig);
    const collectionConfigs = useAppSelector(documentRevisionsSelectors.collectionConfigs);
    const isAnyModified = useAppSelector(documentRevisionsSelectors.isAnyModified);

    const canSetupDefaultRevisionsConfiguration = useAppSelector(
        licenseSelectors.statusValue("CanSetupDefaultRevisionsConfiguration")
    );
    const maxNumberOfRevisionsToKeep = useAppSelector(licenseSelectors.statusValue("MaxNumberOfRevisionsToKeep"));
    const maxNumberOfRevisionAgeToKeepInDays = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfRevisionAgeToKeepInDays")
    );

    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.defaultPolicy"),
            featureIcon: "default",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.maxRevisionsToKeep"),
            featureIcon: "revisions",
            community: { value: 2 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
        {
            featureName: t("featureAvailability.maxRetentionTime"),
            featureIcon: "clock",
            community: { value: 45 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: canSetupDefaultRevisionsConfiguration,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: maxNumberOfRevisionsToKeep,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: maxNumberOfRevisionAgeToKeepInDays,
            },
        ],
    });

    useDirtyFlag(isAnyModified);
    const dispatch = useAppDispatch();
    const { forCurrentDatabase: urls } = useAppUrls();

    const documentRevisionsDocsLink = useRavenLink({ hash: "OFVLX8" });

    useEffect(() => {
        dispatch(documentRevisionsActions.fetchConfigs(databaseName));

        return () => {
            dispatch(documentRevisionsActions.reset());
        };
    }, []);

    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    const asyncSaveConfigs = useAsyncCallback(async () => {
        reportEvent("revisions", "save");

        const promises = [
            databasesService.saveRevisionsConfiguration(databaseName, {
                Default: mapToDto(defaultDocumentsConfig),
                Collections: Object.fromEntries(collectionConfigs.map((x) => [x.Name, mapToDto(x)])),
            }),
            databasesService.saveRevisionsForConflictsConfiguration(databaseName, mapToDto(defaultConflictsConfig)),
        ];

        await Promise.all(promises);
        messagePublisher.reportSuccess(t("saveSuccess"));

        dispatch(documentRevisionsActions.configsSaved());
    });

    const asyncEnforceRevisionsConfiguration = useAsyncCallback(
        async (includeForceCreated: boolean, collections: string[], maxOpsPerSecond: number | null) => {
            const dto = await databasesService.enforceRevisionsConfiguration(
                databaseName,
                includeForceCreated,
                collections,
                maxOpsPerSecond
            );

            notificationCenter.instance.openDetailsForOperationById(
                activeDatabaseTracker.default.database(),
                dto.OperationId
            );
        }
    );

    const onEditRevision = (editRevisionData: Omit<EditRevisionData, "toggle">) => {
        if (editRevisionData.taskType === "new") {
            reportEvent("revisions", "create");
        }

        setEditRevisionData({
            ...editRevisionData,
            toggle: () => setEditRevisionData(null),
        });
    };

    const isSaveDisabled = !isAnyModified || asyncSaveConfigs.status === "loading";

    if (loadStatus === "idle" || loadStatus === "loading") {
        return <LoadingView />;
    }

    if (loadStatus === "failure") {
        return <LoadError error={t("loadError")} refresh={() => documentRevisionsActions.fetchConfigs(databaseName)} />;
    }

    return (
        <div className="content-margin">
            {isEnforceConfigurationModalOpen && (
                <EnforceConfiguration
                    toggle={toggleEnforceConfigurationModal}
                    onConfirm={asyncEnforceRevisionsConfiguration.execute}
                />
            )}
            {editRevisionData && <EditRevision {...editRevisionData} />}

            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title={t("title")} icon="revisions" marginBottom={2} />

                        {hasDatabaseAdminAccess && (
                            <StickyHeader>
                                <Row>
                                    <div className="d-flex flex-wrap gap-2">
                                        <ButtonWithSpinner
                                            variant="primary"
                                            icon="save"
                                            disabled={isSaveDisabled}
                                            onClick={asyncSaveConfigs.execute}
                                            isSpinning={asyncSaveConfigs.status === "loading"}
                                        >
                                            {commonT("save")}
                                        </ButtonWithSpinner>
                                        <FlexGrow />
                                        <PopoverWithHoverWrapper message={t("revertRevisionsPopover")}>
                                            <a className="btn btn-secondary" href={urls.revertRevisions()}>
                                                <Icon icon="revert-revisions" />
                                                {t("revertRevisions")}
                                            </a>
                                        </PopoverWithHoverWrapper>
                                        <PopoverWithHoverWrapper
                                            message={
                                                isSaveDisabled ? t("enforcePopover") : t("enforceSaveFirstPopover")
                                            }
                                        >
                                            <ButtonWithSpinner
                                                variant="secondary"
                                                onClick={toggleEnforceConfigurationModal}
                                                disabled={isAnyModified}
                                                isSpinning={asyncEnforceRevisionsConfiguration.status === "loading"}
                                            >
                                                <Icon icon="rocket" />
                                                {t("enforceButton")}
                                            </ButtonWithSpinner>
                                        </PopoverWithHoverWrapper>
                                    </div>
                                    <div className="mt-3">
                                        <DocumentRevisionsSelectActions />
                                    </div>
                                </Row>
                            </StickyHeader>
                        )}

                        <div className="mt-5">
                            <HrHeader
                                right={
                                    hasDatabaseAdminAccess && !defaultDocumentsConfig ? (
                                        <ConditionalPopover
                                            conditions={{
                                                isActive: !canSetupDefaultRevisionsConfiguration,
                                                message: t("defaultPolicyLicensePopover"),
                                            }}
                                        >
                                            <div id="add-default-config-button">
                                                <Button
                                                    variant="info"
                                                    size="sm"
                                                    className="rounded-pill"
                                                    title={t("addDefaultTitle")}
                                                    onClick={() =>
                                                        onEditRevision({
                                                            taskType: "new",
                                                            configType: "defaultDocument",
                                                            onConfirm: (config) =>
                                                                dispatch(documentRevisionsActions.configAdded(config)),
                                                        })
                                                    }
                                                    disabled={!canSetupDefaultRevisionsConfiguration}
                                                >
                                                    <Icon icon="plus" />
                                                    {t("addNew")}
                                                </Button>
                                            </div>
                                        </ConditionalPopover>
                                    ) : null
                                }
                            >
                                <Icon icon="default" />
                                {t("defaultsHeading")}
                            </HrHeader>
                            {defaultDocumentsConfig && (
                                <DocumentRevisionsConfigPanel
                                    config={defaultDocumentsConfig}
                                    onToggle={() =>
                                        dispatch(
                                            documentRevisionsActions.configStateToggled(defaultDocumentsConfig.Name)
                                        )
                                    }
                                    onDelete={() =>
                                        dispatch(documentRevisionsActions.configDeleted(defaultDocumentsConfig.Name))
                                    }
                                    onEdit={() =>
                                        onEditRevision({
                                            taskType: "edit",
                                            configType: "defaultDocument",
                                            onConfirm: (config) =>
                                                dispatch(documentRevisionsActions.configEdited(config)),
                                            config: defaultDocumentsConfig,
                                        })
                                    }
                                />
                            )}
                            <DocumentRevisionsConfigPanel
                                config={defaultConflictsConfig}
                                onToggle={() =>
                                    dispatch(documentRevisionsActions.configStateToggled(defaultConflictsConfig.Name))
                                }
                                onEdit={() =>
                                    onEditRevision({
                                        taskType: "edit",
                                        configType: "defaultConflicts",
                                        onConfirm: (config) => dispatch(documentRevisionsActions.configEdited(config)),
                                        config: defaultConflictsConfig,
                                    })
                                }
                            />
                        </div>
                        <div className="mt-5">
                            <HrHeader
                                right={
                                    hasDatabaseAdminAccess ? (
                                        <Button
                                            variant="info"
                                            size="sm"
                                            className="rounded-pill"
                                            title={t("addCollectionTitle")}
                                            onClick={() =>
                                                onEditRevision({
                                                    taskType: "new",
                                                    configType: "collectionSpecific",
                                                    onConfirm: (config) =>
                                                        dispatch(documentRevisionsActions.configAdded(config)),
                                                })
                                            }
                                        >
                                            <Icon icon="plus" />
                                            {t("addNew")}
                                        </Button>
                                    ) : null
                                }
                            >
                                <Icon icon="documents" />
                                {t("collectionsHeading")}
                            </HrHeader>
                            {collectionConfigs.length > 0 ? (
                                collectionConfigs.map((config) => (
                                    <DocumentRevisionsConfigPanel
                                        key={config.Name}
                                        config={config}
                                        onToggle={() =>
                                            dispatch(documentRevisionsActions.configStateToggled(config.Name))
                                        }
                                        onDelete={() => dispatch(documentRevisionsActions.configDeleted(config.Name))}
                                        onEdit={() =>
                                            onEditRevision({
                                                taskType: "edit",
                                                configType: "collectionSpecific",
                                                onConfirm: (config) =>
                                                    dispatch(documentRevisionsActions.configEdited(config)),
                                                config,
                                            })
                                        }
                                    />
                                ))
                            ) : (
                                <EmptySet>{t("collectionsEmpty")}</EmptySet>
                            )}
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper targetId="1" icon="about" color="info">
                                <p>
                                    <StudioTrans
                                        ns="documentRevisions"
                                        i18nKey="about.intro"
                                        components={{ strong: <strong /> }}
                                    />
                                </p>
                                <div>
                                    {t("about.createdWhen")}
                                    <ul>
                                        <li>{t("about.configDefinedItem")}</li>
                                        <li>{t("about.modifiedItem")}</li>
                                    </ul>
                                </div>
                                <div>
                                    {t("about.defineInView")}
                                    <ul>
                                        <li>
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="about.defaultsItem"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                        <li>
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="about.collectionsItem"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                    </ul>
                                </div>
                                <div>
                                    {t("about.optionsIntro")}
                                    <ul>
                                        <li>
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="about.revertItem"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                        <li>
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="about.enforceItem"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                    </ul>
                                </div>
                                <div>
                                    <StudioTrans
                                        ns="documentRevisions"
                                        i18nKey="about.allRevisions"
                                        components={{
                                            a: <a href={appUrl.forAllRevisions(activeDatabaseName)} target="_blank" />,
                                        }}
                                    />
                                </div>
                                <hr />
                                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                                <a href={documentRevisionsDocsLink} target="_blank">
                                    <Icon icon="newtab" /> {t("about.docsLink")}
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={
                                    canSetupDefaultRevisionsConfiguration &&
                                    !maxNumberOfRevisionsToKeep &&
                                    !maxNumberOfRevisionAgeToKeepInDays
                                }
                                data={featureAvailability}
                                isOpenedByDefault={false}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

function mapToDto(
    config: DocumentRevisionsConfig
): Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration {
    return config ? _.omit(config, "Name") : null;
}
