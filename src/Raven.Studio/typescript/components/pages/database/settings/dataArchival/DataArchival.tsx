import { useEffect } from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { useServices } from "hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import {
    DataArchivalFormData,
    dataArchivalYupResolver,
} from "components/pages/database/settings/dataArchival/DataArchivalValidation";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import { useEventsCollector } from "hooks/useEventsCollector";
import DataArchivalConfiguration = Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
import { tryHandleSubmit } from "components/utils/common";
import messagePublisher from "common/messagePublisher";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch } from "components/common/Form";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useRavenLink } from "hooks/useRavenLink";
import moment from "moment";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const defaultItemsToProcess = 65536;

export default function DataArchival() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDataArchival = useAppSelector(licenseSelectors.statusValue("HasDataArchival"));

    const documentArchiveLinkToOverviewDocs = useRavenLink({ hash: "1FI4YS" });

    const { databasesService } = useServices();
    const t = useStudioTranslation("dataArchival");
    const commonT = useStudioTranslation("common");
    const asyncGetDataArchivalConfiguration = useAsyncCallback<DataArchivalFormData>(async () =>
        mapToFormData(await databasesService.getDataArchivalConfiguration(databaseName))
    );
    const { handleSubmit, control, formState, reset, setValue, watch } = useForm<DataArchivalFormData>({
        resolver: dataArchivalYupResolver,
        mode: "all",
        defaultValues: asyncGetDataArchivalConfiguration.execute,
    });
    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });
    const { reportEvent } = useEventsCollector();

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("title"),
            featureIcon: "data-archival",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasDataArchival,
            },
        ],
    });

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            switch (name) {
                case "isDataArchivalEnabled": {
                    if (values.isDataArchivalEnabled) {
                        setValue("isLimitMaxItemsToProcessEnabled", true, { shouldValidate: true });
                    } else {
                        setValue("isLimitMaxItemsToProcessEnabled", false, { shouldValidate: true });
                        setValue("isArchiveFrequencyEnabled", false, { shouldValidate: true });
                    }
                    break;
                }
                case "isLimitMaxItemsToProcessEnabled": {
                    if (values.isLimitMaxItemsToProcessEnabled) {
                        setValue("maxItemsToProcess", defaultItemsToProcess, { shouldValidate: true });
                    } else {
                        setValue("maxItemsToProcess", null, { shouldValidate: true });
                    }
                    break;
                }
                case "isArchiveFrequencyEnabled": {
                    if (!values.isArchiveFrequencyEnabled) {
                        setValue("archiveFrequency", null, { shouldValidate: true });
                    }
                    break;
                }
            }
        });
        return () => unsubscribe();
    }, [setValue, watch]);

    const onSave: SubmitHandler<DataArchivalFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("data-archival-configuration", "save");

            await databasesService.saveDataArchivalConfiguration(databaseName, {
                Disabled: !formData.isDataArchivalEnabled,
                ArchiveFrequencyInSec: formData.isArchiveFrequencyEnabled ? formData.archiveFrequency : null,
                MaxItemsToProcess: formData.isLimitMaxItemsToProcessEnabled ? formData.maxItemsToProcess : null,
            });

            messagePublisher.reportSuccess(t("saveSuccess"));
            activeDatabaseTracker.default.database().hasArchivalConfiguration(formData.isDataArchivalEnabled);
            reset(formData);
        });
    };

    if (
        asyncGetDataArchivalConfiguration.status === "not-requested" ||
        asyncGetDataArchivalConfiguration.status === "loading"
    ) {
        return <LoadingView />;
    }

    if (asyncGetDataArchivalConfiguration.status === "error") {
        return <LoadError error={t("loadError")} refresh={asyncGetDataArchivalConfiguration.execute} />;
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading
                                title={t("title")}
                                icon="data-archival"
                                licenseBadgeText={hasDataArchival ? null : "Enterprise"}
                            />
                            <ConditionalPopover
                                conditions={{
                                    isActive: !hasDataArchival,
                                    message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                }}
                            >
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="primary"
                                    className="mb-3"
                                    icon="save"
                                    disabled={!formState.isDirty || !hasDatabaseAdminAccess}
                                    isSpinning={formState.isSubmitting}
                                >
                                    {commonT("save")}
                                </ButtonWithSpinner>
                            </ConditionalPopover>
                            <Col className={hasDataArchival ? "" : "item-disabled pe-none"}>
                                <Card>
                                    <Card.Body>
                                        <div className="vstack gap-2">
                                            <FormSwitch
                                                name="isDataArchivalEnabled"
                                                control={control}
                                                disabled={formState.isSubmitting}
                                            >
                                                {t("enableSwitch")}
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isArchiveFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDataArchivalEnabled
                                                    }
                                                >
                                                    {t("customFrequencySwitch")}
                                                </FormSwitch>
                                                <FormInput
                                                    name="archiveFrequency"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isArchiveFrequencyEnabled
                                                    }
                                                    placeholder={t("frequencyPlaceholder", { seconds: 60 })}
                                                    addon={t("secondsAddon")}
                                                />
                                            </div>
                                            <div>
                                                <FormSwitch
                                                    name="isLimitMaxItemsToProcessEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDataArchivalEnabled
                                                    }
                                                >
                                                    {t("maxItemsSwitch")}
                                                </FormSwitch>
                                                <FormInput
                                                    name="maxItemsToProcess"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isLimitMaxItemsToProcessEnabled
                                                    }
                                                    addon={t("itemsAddon")}
                                                />
                                            </div>
                                        </div>
                                    </Card.Body>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored defaultOpen={hasDataArchival ? null : "licensing"}>
                            <AccordionItemWrapper targetId="about" icon="about" color="info">
                                <p>
                                    <StudioTrans
                                        ns="dataArchival"
                                        i18nKey="about.intro"
                                        components={{ strong: <strong /> }}
                                    />
                                </p>
                                <ul>
                                    <li>
                                        <StudioTrans
                                            ns="dataArchival"
                                            i18nKey="about.scanItem"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="dataArchival"
                                            i18nKey="about.metadataItem"
                                            components={{ code: <code /> }}
                                        />
                                        <ul>
                                            <li>{t("about.compressedItem")}</li>
                                            <li>
                                                <StudioTrans
                                                    ns="dataArchival"
                                                    i18nKey="about.replacedItem"
                                                    components={{ code: <code /> }}
                                                />
                                            </li>
                                            <li>{t("about.indexingItem")}</li>
                                        </ul>
                                    </li>
                                </ul>
                                <p>{t("about.exampleLabel")}</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                                <a href={documentArchiveLinkToOverviewDocs} target="_blank">
                                    <Icon icon="newtab" /> {t("about.docsLink")}
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={hasDataArchival}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

const codeExample = `{
  "Example": "Set a timestamp in the @archive-at metadata property",
  "@metadata": {
    "@collection": "Foo",
    "@archive-at": "${moment().add(1, "year").toISOString()}"
  }
}`;

function mapToFormData(dto: DataArchivalConfiguration): DataArchivalFormData {
    if (!dto) {
        return {
            isDataArchivalEnabled: false,
            isArchiveFrequencyEnabled: false,
            archiveFrequency: null,
            isLimitMaxItemsToProcessEnabled: false,
            maxItemsToProcess: null,
        };
    }

    return {
        isDataArchivalEnabled: !dto.Disabled,
        isArchiveFrequencyEnabled: dto.ArchiveFrequencyInSec != null,
        archiveFrequency: dto.ArchiveFrequencyInSec,
        isLimitMaxItemsToProcessEnabled: dto.MaxItemsToProcess != null,
        maxItemsToProcess: dto.MaxItemsToProcess,
    };
}
