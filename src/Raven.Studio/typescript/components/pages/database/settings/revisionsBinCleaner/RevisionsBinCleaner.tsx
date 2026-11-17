import React from "react";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    RevisionsBinCleanerFormData,
    revisionsBinCleanerYupResolver,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import { useAsyncCallback } from "react-async-hook";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useEventsCollector } from "hooks/useEventsCollector";
import { tryHandleSubmit } from "components/utils/common";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormDurationPicker, FormGroup, FormInput, FormSwitch } from "components/common/Form";
import { RevisionsBinCleanerInfoHub } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerInfoHub";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useRevisionsBinCleanerFormSideEffects from "components/pages/database/settings/revisionsBinCleaner/useRevisionsBinCleanerFormSideEffects";
import { revisionsBinCleanerUtils } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerUtils";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Form from "react-bootstrap/Form";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function RevisionsBinCleaner() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const t = useStudioTranslation("revisionsBinCleaner");
    const commonT = useStudioTranslation("common");

    const asyncGetRevisionsBinCleanerConfiguration = useAsyncCallback<RevisionsBinCleanerFormData>(async () =>
        revisionsBinCleanerUtils.mapToFormData(await databasesService.getRevisionsBinCleanerConfiguration(databaseName))
    );

    const { handleSubmit, control, formState, reset, setValue, watch } = useForm<Partial<RevisionsBinCleanerFormData>>({
        resolver: revisionsBinCleanerYupResolver,
        mode: "all",
        defaultValues: asyncGetRevisionsBinCleanerConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control });

    const { reportEvent } = useEventsCollector();

    useRevisionsBinCleanerFormSideEffects(watch, setValue);

    const handleSave: SubmitHandler<RevisionsBinCleanerFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("revisions-bin-configuration", "save");
            await databasesService.saveRevisionsBinCleanerConfiguration(
                databaseName,
                revisionsBinCleanerUtils.mapToDto(formData)
            );

            reset(formData);
        });
    };

    if (
        asyncGetRevisionsBinCleanerConfiguration.status === "not-requested" ||
        asyncGetRevisionsBinCleanerConfiguration.status === "loading"
    ) {
        return <LoadingView />;
    }

    if (asyncGetRevisionsBinCleanerConfiguration.status === "error") {
        return <LoadError error={t("loadError")} refresh={asyncGetRevisionsBinCleanerConfiguration.execute} />;
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(handleSave)} autoComplete="off">
                            <AboutViewHeading title={t("title")} icon="revisions-bin" />
                            {hasDatabaseAdminAccess && (
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty}
                                    isSpinning={formState.isSubmitting}
                                >
                                    {commonT("save")}
                                </ButtonWithSpinner>
                            )}
                            <Col>
                                <Card>
                                    <Card.Body>
                                        <FormGroup>
                                            <FormSwitch
                                                name="isRevisionsBinCleanerEnabled"
                                                disabled={!hasDatabaseAdminAccess}
                                                control={control}
                                            >
                                                {t("enableSwitch")}
                                            </FormSwitch>
                                        </FormGroup>
                                        <FormGroup className="d-flex gap-2 align-items-center">
                                            <FormSwitch
                                                name="isMinimumEntriesAgeToKeepEnabled"
                                                control={control}
                                                color="primary"
                                                disabled={
                                                    !hasDatabaseAdminAccess ||
                                                    formState.isSubmitting ||
                                                    !formValues.isRevisionsBinCleanerEnabled
                                                }
                                            >
                                                {t("minimumAgeSwitch")}
                                            </FormSwitch>
                                            {formValues.isRevisionsBinCleanerEnabled &&
                                                !formValues.isMinimumEntriesAgeToKeepEnabled && (
                                                    <PopoverWithHoverWrapper message={t("minimumAgeOffWarning")}>
                                                        <Icon margin="m-0" icon="warning" color="warning" />
                                                    </PopoverWithHoverWrapper>
                                                )}
                                        </FormGroup>
                                        <Collapse
                                            data-testid="collapse"
                                            appear
                                            in={
                                                formValues.isMinimumEntriesAgeToKeepEnabled &&
                                                formValues.isRevisionsBinCleanerEnabled
                                            }
                                        >
                                            <div>
                                                <FormGroup data-testid="durationPicker">
                                                    <FormDurationPicker
                                                        control={control}
                                                        disabled={
                                                            !hasDatabaseAdminAccess ||
                                                            formState.isSubmitting ||
                                                            !formValues.isMinimumEntriesAgeToKeepEnabled
                                                        }
                                                        placeholder={{
                                                            days: t("defaultPlaceholder", { value: 30 }),
                                                            hours: t("defaultPlaceholder", { value: 0 }),
                                                            minutes: t("defaultPlaceholder", { value: 0 }),
                                                        }}
                                                        name="minimumEntriesAgeToKeep"
                                                        showDays
                                                    />
                                                </FormGroup>
                                            </div>
                                        </Collapse>
                                        <FormGroup>
                                            <FormSwitch
                                                name="isCleanerFrequencyInSecEnabled"
                                                control={control}
                                                color="primary"
                                                className="mb-3"
                                                disabled={
                                                    !hasDatabaseAdminAccess ||
                                                    formState.isSubmitting ||
                                                    !formValues.isRevisionsBinCleanerEnabled
                                                }
                                            >
                                                {t("customFrequencySwitch")}
                                            </FormSwitch>
                                            <FormInput
                                                name="cleanerFrequencyInSec"
                                                control={control}
                                                type="number"
                                                disabled={
                                                    !hasDatabaseAdminAccess ||
                                                    formState.isSubmitting ||
                                                    !formValues.isRevisionsBinCleanerEnabled ||
                                                    !formValues.isCleanerFrequencyInSecEnabled
                                                }
                                                placeholder={t("defaultPlaceholder", { value: 300 })}
                                                addon={t("secondsAddon")}
                                            />
                                        </FormGroup>
                                    </Card.Body>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <RevisionsBinCleanerInfoHub />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
