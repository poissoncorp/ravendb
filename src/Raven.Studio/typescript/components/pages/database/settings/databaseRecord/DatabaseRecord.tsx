import React, { useCallback, useEffect, useRef, useState } from "react";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import useConfirm from "components/common/ConfirmDialog";
import classNames from "classnames";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { Switch } from "components/common/Checkbox";
import AceEditor from "components/common/ace/AceEditor";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import genUtils from "common/generalUtils";
import DatabaseRecordAboutView from "./DatabaseRecordAboutView";
import ReactAce from "react-ace/lib/ace";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface VisibleDocument {
    text: string;
    isFromServer: boolean;
}

export default function DatabaseRecord() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const t = useStudioTranslation("databaseRecord");
    const commonT = useStudioTranslation("common");

    const { databasesService } = useServices();
    const confirm = useConfirm();
    const { reportEvent } = useEventsCollector();
    const aceRef = useRef<ReactAce>(null);

    const { value: isEditMode, toggle: toggleIsEditMode } = useBoolean(false);
    const { value: isHideEmptyValues, toggle: toggleIsHideEmptyValues } = useBoolean(false);
    const { value: isCollapsed, setValue: setIsCollapsed } = useBoolean(false);
    const [visibleDocument, setVisibleDocument] = useState<VisibleDocument>(null);

    const asyncGetDatabaseRecord = useAsyncCallback(
        async (reportRefreshProgress: boolean) =>
            databasesService.getDatabaseRecord(databaseName, reportRefreshProgress),
        {
            onSuccess: () => {
                setIsCollapsed(false);
            },
        }
    );

    const asyncSaveDatabaseRecord = useAsyncCallback(async () => {
        const dto: documentDto = JSON.parse(visibleDocument?.text);
        dto.Settings = genUtils.flattenObj(dto.Settings, "");

        await databasesService.saveDatabaseRecord(databaseName, dto, dto.Etag);
        await asyncGetDatabaseRecord.execute(false);
    });

    const collapse = useCallback(() => {
        if (aceRef.current) {
            aceRef.current.editor.session.foldAll(1);
            setIsCollapsed(true);
        }
    }, [setIsCollapsed]);

    const expand = () => {
        if (aceRef.current) {
            aceRef.current.editor.session.unfold(null);
            setIsCollapsed(false);
        }
    };

    const updateVisibleDocumentText = useCallback(() => {
        const docText = genUtils.stringify(asyncGetDatabaseRecord.result.toDto(), isHideEmptyValues);

        setVisibleDocument({ text: docText, isFromServer: true });
    }, [asyncGetDatabaseRecord.result, isHideEmptyValues]);

    useEffect(() => {
        const getData = async () => {
            if (asyncGetDatabaseRecord.status === "not-requested" && isOperatorOrAbove) {
                try {
                    await asyncGetDatabaseRecord.execute(false);
                } catch (error) {
                    messagePublisher.reportError(t("fetchError"), error.message, error.name);
                }
            }
        };
        getData();
    }, [asyncGetDatabaseRecord, isOperatorOrAbove, t]);

    useEffect(() => {
        if (asyncGetDatabaseRecord.result) {
            updateVisibleDocumentText();
        }
    }, [asyncGetDatabaseRecord.result, isHideEmptyValues, updateVisibleDocumentText]);

    useEffect(() => {
        if (visibleDocument?.isFromServer) {
            collapse();
        }
    }, [collapse, visibleDocument]);

    const toggleEditMode = async () => {
        const confirmed = await confirm({
            icon: "database-record",
            actionColor: "warning",
            title: t("editConfirm.title"),
            message: <EditModeRiskAlert />,
            confirmText: t("editConfirm.confirmButton"),
        });

        if (confirmed) {
            toggleIsEditMode();
        }
    };

    const saveDatabaseRecord = async () => {
        if (aceRef.current.editor?.session?.getAnnotations()?.some((x) => x.type === "error")) {
            return;
        }

        const confirmed = await confirm({
            icon: "save",
            actionColor: "warning",
            title: t("saveConfirm.title"),
            confirmText: t("saveConfirm.confirmButton"),
        });

        if (confirmed) {
            await asyncSaveDatabaseRecord.execute();
            toggleIsEditMode();
        }
    };

    const discardDatabaseRecord = async () => {
        const confirmed = await confirm({
            icon: "database-record",
            actionColor: "primary",
            title: t("discardConfirm.title"),
            confirmText: t("discardConfirm.confirmButton"),
        });

        if (confirmed) {
            updateVisibleDocumentText();
            toggleIsEditMode();
        }
    };

    const refresh = () => {
        reportEvent("database-record", "refresh");
        asyncGetDatabaseRecord.execute(true);
    };

    if (asyncGetDatabaseRecord.status === "error") {
        return <LoadError error={t("loadError")} refresh={() => asyncGetDatabaseRecord.execute(true)} />;
    }

    return (
        <div className="content-margin">
            {isOperatorOrAbove ? (
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title={t("title")} icon="database-record" />
                        <div
                            className={classNames("d-flex gap-3 flex-wrap mb-3", {
                                "justify-content-between": !isEditMode,
                            })}
                        >
                            {isEditMode ? (
                                <>
                                    <ButtonWithSpinner
                                        variant="primary"
                                        icon="save"
                                        onClick={saveDatabaseRecord}
                                        isSpinning={asyncSaveDatabaseRecord.loading}
                                    >
                                        {commonT("save")}
                                    </ButtonWithSpinner>
                                    <Button variant="secondary" onClick={discardDatabaseRecord}>
                                        {commonT("cancel")}
                                    </Button>
                                </>
                            ) : (
                                <>
                                    <ButtonWithSpinner
                                        variant="primary"
                                        onClick={refresh}
                                        isSpinning={asyncGetDatabaseRecord.loading}
                                        icon="refresh"
                                    >
                                        {commonT("refresh")}
                                    </ButtonWithSpinner>
                                    <Button
                                        variant="secondary"
                                        onClick={toggleEditMode}
                                        disabled={asyncGetDatabaseRecord.loading}
                                    >
                                        <Icon icon="edit" />
                                        {t("editButton")}
                                    </Button>
                                </>
                            )}
                        </div>

                        <Card>
                            <Card.Body className="d-flex flex-center flex-column flex-wrap gap-4">
                                <InputGroup className="gap-1 flex-wrap flex-column">
                                    <div
                                        className={classNames(
                                            "d-flex flex-wrap",
                                            isEditMode ? "justify-content-end" : "justify-content-between"
                                        )}
                                    >
                                        {!isEditMode && (
                                            <Switch
                                                type="switch"
                                                color="primary"
                                                selected={isHideEmptyValues}
                                                toggleSelection={toggleIsHideEmptyValues}
                                            >
                                                {t("hideEmptyValuesSwitch")}
                                            </Switch>
                                        )}
                                        <Button
                                            variant="link"
                                            size="xs"
                                            className="p-0"
                                            onClick={isCollapsed ? expand : collapse}
                                            disabled={asyncGetDatabaseRecord.loading}
                                        >
                                            {isCollapsed ? (
                                                <>
                                                    <Icon icon="expand-vertical" /> {t("expandButton")}
                                                </>
                                            ) : (
                                                <>
                                                    <Icon icon="collapse-vertical" /> {t("collapseButton")}
                                                </>
                                            )}
                                        </Button>
                                    </div>
                                    <AceEditor
                                        aceRef={aceRef}
                                        mode="json"
                                        height="600px"
                                        value={visibleDocument?.text}
                                        onChange={(x) => setVisibleDocument({ text: x, isFromServer: false })}
                                        readOnly={!isEditMode}
                                    />
                                </InputGroup>
                            </Card.Body>
                        </Card>
                    </Col>
                    <Col sm={12} lg={4}>
                        <DatabaseRecordAboutView />
                    </Col>
                </Row>
            ) : (
                <FeatureNotAvailable badgeText={t("noAccess.badge")}>{t("noAccess.message")}</FeatureNotAvailable>
            )}
        </div>
    );
}

function EditModeRiskAlert() {
    const t = useStudioTranslation("databaseRecord");

    return <RichAlert variant="warning">{t("editConfirm.riskWarning")}</RichAlert>;
}
