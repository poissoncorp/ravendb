import React, { useEffect } from "react";
import Card from "react-bootstrap/Card";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { HrHeader } from "components/common/HrHeader";
import ConflictResolutionConfigPanel from "components/pages/database/settings/conflictResolution/ConflictResolutionConfigPanel";
import { Switch } from "components/common/Checkbox";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { LoadError } from "components/common/LoadError";
import { LazyLoad } from "components/common/LazyLoad";
import {
    conflictResolutionSelectors,
    conflictResolutionActions,
    ConflictResolutionCollectionConfig,
} from "./store/conflictResolutionSlice";
import { EmptySet } from "components/common/EmptySet";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import ConflictResolutionAboutView from "./ConflictResolutionAboutView";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function ConflictResolution() {
    const dispatch = useAppDispatch();
    const t = useStudioTranslation("conflictResolution");
    const commonT = useStudioTranslation("common");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const loadStatus = useAppSelector(conflictResolutionSelectors.loadStatus);
    const isResolveToLatest = useAppSelector(conflictResolutionSelectors.isResolveToLatest);
    const collectionConfigs = useAppSelector(conflictResolutionSelectors.collectionConfigs);
    const isDirty = useAppSelector(conflictResolutionSelectors.isDirty);
    const isSomeInEditMode = useAppSelector(conflictResolutionSelectors.isSomeInEditMode);

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    useEffect(() => {
        dispatch(conflictResolutionActions.fetchConfig(databaseName));

        return () => {
            dispatch(conflictResolutionActions.reset());
        };
        // Changing the database causes re-mount
    }, []);

    const asyncSave = useAsyncCallback(async () => {
        reportEvent("conflict-resolution", "save");

        const response = await databasesService.saveConflictSolverConfiguration(
            databaseName,
            mapToDto(isResolveToLatest, collectionConfigs)
        );
        dispatch(conflictResolutionActions.allSaved(response.ConflictSolverConfig));
    });

    if (loadStatus === "failure") {
        return (
            <LoadError
                error={t("loadError")}
                refresh={() => dispatch(conflictResolutionActions.fetchConfig(databaseName))}
            />
        );
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title={t("title")} icon="conflicts-resolution" />
                    <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"}>
                        {hasDatabaseAdminAccess && (
                            <>
                                <ConditionalPopover
                                    conditions={{
                                        isActive: isSomeInEditMode,
                                        message: t("finishEditingHint"),
                                    }}
                                >
                                    <div id="saveConflictResolutionScript" className="d-flex w-fit-content gap-3 mb-3">
                                        <ButtonWithSpinner
                                            variant="primary"
                                            icon="save"
                                            isSpinning={asyncSave.loading}
                                            onClick={asyncSave.execute}
                                            disabled={!isDirty || isSomeInEditMode}
                                        >
                                            {commonT("save")}
                                        </ButtonWithSpinner>
                                    </div>
                                </ConditionalPopover>
                            </>
                        )}
                        <div className="mb-3">
                            <HrHeader
                                right={
                                    hasDatabaseAdminAccess && (
                                        <div id="addNewScriptButton">
                                            <Button
                                                variant="info"
                                                size="sm"
                                                className="rounded-pill"
                                                title={t("addScriptTitle")}
                                                onClick={() => dispatch(conflictResolutionActions.added())}
                                            >
                                                <Icon icon="plus" />
                                                {t("addScriptButton")}
                                            </Button>
                                        </div>
                                    )
                                }
                                count={collectionConfigs.length}
                            >
                                <Icon icon="documents" />
                                {t("scriptsHeading")}
                            </HrHeader>
                            {collectionConfigs.length > 0 ? (
                                collectionConfigs.map((config) => (
                                    <ConflictResolutionConfigPanel key={config.id} initialConfig={config} />
                                ))
                            ) : (
                                <EmptySet>{t("noScripts")}</EmptySet>
                            )}
                        </div>
                        <Card>
                            <Card.Body>
                                <Switch
                                    color="primary"
                                    selected={isResolveToLatest}
                                    toggleSelection={() =>
                                        dispatch(conflictResolutionActions.isResolveToLatestToggled())
                                    }
                                    disabled={!hasDatabaseAdminAccess}
                                >
                                    {t("resolveToLatestSwitch")}
                                </Switch>
                            </Card.Body>
                        </Card>
                    </LazyLoad>
                </Col>
                <Col sm={12} lg={4}>
                    <ConflictResolutionAboutView />
                </Col>
            </Row>
        </div>
    );
}

function mapToDto(
    isResolveToLatest: boolean,
    collectionConfigs: ConflictResolutionCollectionConfig[]
): Raven.Client.ServerWide.ConflictSolver {
    return {
        ResolveToLatest: isResolveToLatest,
        ResolveByCollection: Object.fromEntries(
            collectionConfigs.map((config): [string, Raven.Client.ServerWide.ScriptResolver] => [
                config.name,
                {
                    Script: config.script,
                    LastModifiedTime: null,
                },
            ])
        ),
    };
}
