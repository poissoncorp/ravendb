import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import React from "react";
import { useForm, useWatch } from "react-hook-form";
import { RevertRevisionsFormData, revertRevisionsYupResolver } from "./RevertRevisionsValidation";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "components/hooks/useAppUrls";
import { Icon } from "components/common/Icon";
import { FormDatePicker, FormGroup, FormInput, FormLabel, FormSelect } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import useConfirm from "components/common/ConfirmDialog";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import notificationCenter from "common/notifications/notificationCenter";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";
import { useAsyncCallback } from "react-async-hook";
import RevertRevisionsRequest = Raven.Server.Documents.Revisions.RevertRevisionsRequest;
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function RevertRevisions() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const t = useStudioTranslation("revertRevisions");
    const commonT = useStudioTranslation("common");

    const revertCollectionsDocsLink = useRavenLink({ hash: "E752O6" });

    const { control, formState, handleSubmit, setValue } = useForm<RevertRevisionsFormData>({
        resolver: revertRevisionsYupResolver,
        defaultValues: {
            pointInTime: null,
            timeWindow: null,
            timeMagnitude: "hours",
            isRevertAllCollections: true,
            collections: [],
        },
    });

    const { isRevertAllCollections, collections, pointInTime } = useWatch({ control });

    const { forCurrentDatabase } = useAppUrls();
    const confirm = useConfirm();

    const formattedPointInTimeUtc = moment(pointInTime).utc().format(defaultDateFormat) + " UTC";

    const { databasesService } = useServices();

    const asyncRevertRevisions = useAsyncCallback((dto: RevertRevisionsRequest) =>
        databasesService.revertRevisions(databaseName, dto)
    );

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );

    const tooltipMessage = t("revertTooltip", { context: isRevertAllCollections ? "all" : "selected" });

    const timeWindowOptions: SelectOption<timeMagnitude>[] = [
        { value: "minutes", label: t("timeWindow.units.minutes") },
        { value: "hours", label: t("timeWindow.units.hours") },
        { value: "days", label: t("timeWindow.units.days") },
    ];

    const onRevert = async (formData: RevertRevisionsFormData) => {
        const isConfirmed = await confirm({
            title: t("confirmTitle", { pointInTime: formattedPointInTimeUtc }),
            icon: "revert-revisions",
            actionColor: "primary",
            confirmText: t("revertButton"),
        });

        if (isConfirmed) {
            return tryHandleSubmit(async () => {
                const result = await asyncRevertRevisions.execute(toDto(formData));
                notificationCenter.instance.openDetailsForOperationById(
                    activeDatabaseTracker.default.database(),
                    result.OperationId
                );
            });
        }
    };

    return (
        <Row className="content-margin gy-sm">
            <Col>
                <AboutViewHeading title={t("title")} icon="revert-revisions" />
                <Form onSubmit={handleSubmit(onRevert)} autoComplete="off">
                    <div className="d-flex justify-content-between align-items-end">
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            icon="revert-revisions"
                            disabled={!formState.isDirty}
                            isSpinning={asyncRevertRevisions.status === "loading"}
                            title={tooltipMessage}
                        >
                            {t("revertButton")}
                        </ButtonWithSpinner>
                        <small>
                            <a href={forCurrentDatabase.revisions()} title={t("backLinkTitle")}>
                                <Icon icon="link" />
                                {t("backLink")}
                            </a>
                        </small>
                    </div>
                    <Card className="mt-3">
                        <Card.Body className="gap-4">
                            <FormGroup className="mb-3">
                                <FormLabel>{t("pointInTime.label")}</FormLabel>
                                <FormDatePicker
                                    name="pointInTime"
                                    control={control}
                                    showTimeSelect
                                    timeIntervals={15}
                                    filterDate={filterPointInTime}
                                    filterTime={filterPointInTime}
                                    showYearDropdown
                                    showMonthDropdown
                                    placeholderText={t("pointInTime.placeholder")}
                                    addon={t("pointInTime.localAddon")}
                                />
                            </FormGroup>
                            <FormGroup>
                                <FormLabel>{t("timeWindow.label")}</FormLabel>
                                <InputGroup>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="timeWindow"
                                        placeholder={t("timeWindow.placeholder", { value: defaultWindowValue })}
                                        addon={
                                            <FormSelect
                                                control={control}
                                                name="timeMagnitude"
                                                options={timeWindowOptions}
                                                isSearchable={false}
                                                className="w-25"
                                                classNames={{ menu: () => "time-window-select-menu" }}
                                            />
                                        }
                                    />
                                </InputGroup>
                            </FormGroup>
                        </Card.Body>
                    </Card>
                    {hasDatabaseAdminAccess && (
                        <Card className="mt-3">
                            <Card.Body>
                                <FormCollectionsSelect
                                    control={control}
                                    collectionsFormName="collections"
                                    collections={collections}
                                    isAllCollectionsFormName="isRevertAllCollections"
                                    isAllCollections={isRevertAllCollections}
                                    allCollectionNames={allCollectionNames}
                                    setValue={setValue}
                                />
                            </Card.Body>
                        </Card>
                    )}
                </Form>
            </Col>
            <Col sm={12} lg={4}>
                <AboutViewAnchored defaultOpen="about-view">
                    <AccordionItemWrapper targetId="about-view" icon="about" color="info">
                        <div className="margin-bottom-sm">
                            <StudioTrans
                                ns="revertRevisions"
                                i18nKey="about.intro"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </div>
                        <div className="margin-bottom-sm">
                            <StudioTrans
                                ns="revertRevisions"
                                i18nKey="about.timeWindow"
                                components={{ strong: <strong /> }}
                            />
                        </div>
                        {pointInTime && (
                            <div>
                                <StudioTrans
                                    ns="revertRevisions"
                                    i18nKey="about.effects"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                                <ul>
                                    <li>
                                        <StudioTrans
                                            ns="revertRevisions"
                                            i18nKey="about.modifiedItem"
                                            components={{ strong: <strong />, em: <em />, code: <code /> }}
                                            options={{ pointInTime: formattedPointInTimeUtc }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="revertRevisions"
                                            i18nKey="about.limitItem"
                                            components={{ strong: <strong />, em: <em />, code: <code /> }}
                                            options={{ pointInTime: formattedPointInTimeUtc }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="revertRevisions"
                                            i18nKey="about.createdItem"
                                            components={{ strong: <strong />, em: <em />, code: <code /> }}
                                            options={{ pointInTime: formattedPointInTimeUtc }}
                                        />
                                    </li>
                                    <li>{t("about.remainingItem")}</li>
                                </ul>
                            </div>
                        )}
                        <hr />
                        <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                        <a href={revertCollectionsDocsLink} target="_blank">
                            <Icon icon="newtab" /> {t("about.docsLink")}
                        </a>
                    </AccordionItemWrapper>
                </AboutViewAnchored>
            </Col>
        </Row>
    );
}

const defaultDateFormat = "DD/MM/YYYY HH:mm";
const defaultWindowValue = 96;

const filterPointInTime = (date: Date) => {
    const currentDate = moment().add(10, "minutes");
    const selectedDate = moment(date);

    return currentDate > selectedDate;
};

function toDto(formData: RevertRevisionsFormData): RevertRevisionsRequest {
    let WindowInSec = formData.timeWindow ?? defaultWindowValue;

    switch (formData.timeMagnitude) {
        case "minutes":
            WindowInSec *= 60;
            break;
        case "hours":
            WindowInSec *= 3600;
            break;
        case "days":
            WindowInSec *= 24 * 3600;
            break;
        default:
            assertUnreachable(formData.timeMagnitude);
    }

    return {
        Time: moment(formData.pointInTime).utc().toISOString(),
        WindowInSec,
        Collections: formData.isRevertAllCollections ? [] : formData.collections,
        ContinuationParameters: undefined,
    };
}
