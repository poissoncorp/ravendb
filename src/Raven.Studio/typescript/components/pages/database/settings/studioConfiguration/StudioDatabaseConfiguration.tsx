import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

import { SubmitHandler, useForm } from "react-hook-form";
import { FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    StudioDatabaseConfigurationFormData,
    studioDatabaseConfigurationYupResolver,
} from "./StudioDatabaseConfigurationValidation";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { studioEnvironmentOptions } from "components/common/studioConfiguration/StudioConfigurationUtils";
import { useServices } from "components/hooks/useServices";
import appUrl from "common/appUrl";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function StudioDatabaseConfiguration() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const t = useStudioTranslation("studioDatabaseConfiguration");
    const commonT = useStudioTranslation("common");

    const asyncDatabaseSettings = useAsyncCallback<StudioDatabaseConfigurationFormData>(async () => {
        const settings = await databasesService.getDatabaseStudioConfiguration(databaseName);

        return {
            Environment: settings ? settings.Environment : "None",
            DisableAutoIndexCreation: settings ? settings.DisableAutoIndexCreation : false,
            Disabled: settings ? settings.Disabled : false,
        };
    });

    const { handleSubmit, control, formState, reset } = useForm<StudioDatabaseConfigurationFormData>({
        resolver: studioDatabaseConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncDatabaseSettings.execute,
    });

    useDirtyFlag(formState.isDirty);

    const studioConfigurationDocsLink = useRavenLink({ hash: "HIR1VP" });
    const { reportEvent } = useEventsCollector();

    const hasStudioConfiguration = useAppSelector(licenseSelectors.statusValue("HasStudioConfiguration"));
    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("title"),
            featureIcon: "studio-configuration",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasStudioConfiguration,
            },
        ],
    });

    const onSave: SubmitHandler<StudioDatabaseConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("studio-configuration-database", "save");
            databasesService.saveDatabaseStudioConfiguration(formData, databaseName);
            reset(formData);
        });
    };

    const onRefresh = async () => {
        reset(await asyncDatabaseSettings.execute());
    };

    if (asyncDatabaseSettings.status === "not-requested" || asyncDatabaseSettings.status === "loading") {
        return <LoadingView />;
    }

    if (asyncDatabaseSettings.status === "error") {
        return <LoadError error={t("loadError")} refresh={onRefresh} />;
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading
                        icon="database-studio-configuration"
                        title={t("title")}
                        licenseBadgeText={hasStudioConfiguration ? null : "Professional +"}
                    />
                    <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                        <div className="d-flex align-items-center justify-content-between">
                            <ConditionalPopover
                                conditions={{
                                    isActive: !hasStudioConfiguration,
                                    message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                }}
                            >
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
                            </ConditionalPopover>
                            <small title={t("serverWideLinkTitle")}>
                                <a target="_blank" href={appUrl.forGlobalStudioConfiguration()}>
                                    <Icon icon="link" />
                                    {t("serverWideLink")}
                                </a>
                            </small>
                        </div>
                        <div className={hasStudioConfiguration ? "" : "item-disabled pe-none"}>
                            <Card>
                                <Card.Body className="d-flex flex-center flex-column flex-wrap gap-4">
                                    <InputGroup className="gap-1 flex-wrap flex-column">
                                        <FormLabel className="mb-0 md-label">
                                            {t("environment.label")}{" "}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <ul>
                                                        <li className="margin-bottom-xs">
                                                            <StudioTrans
                                                                ns="studioDatabaseConfiguration"
                                                                i18nKey="environment.tagInfo"
                                                                components={{ strong: <strong /> }}
                                                            />
                                                        </li>
                                                        <li>{t("environment.noEffectInfo")}</li>
                                                    </ul>
                                                }
                                                placement="right"
                                            >
                                                <Icon icon="info" color="info" id="environmentInfo" />
                                            </PopoverWithHoverWrapper>
                                        </FormLabel>
                                        <FormSelect
                                            control={control}
                                            name="Environment"
                                            options={studioEnvironmentOptions}
                                            isSearchable={false}
                                        ></FormSelect>
                                    </InputGroup>
                                </Card.Body>
                            </Card>
                            <Card className="mt-3" id="disableAutoIndexesContainer">
                                <Card.Body>
                                    <div className="d-flex flex-column">
                                        <FormSwitch control={control} name="DisableAutoIndexCreation">
                                            {t("autoIndexes.disableSwitch")}{" "}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <ul className="mb-0">
                                                        <li className="margin-bottom-xs">
                                                            <StudioTrans
                                                                ns="studioDatabaseConfiguration"
                                                                i18nKey="autoIndexes.dynamicQueryInfo"
                                                                components={{ strong: <strong /> }}
                                                            />
                                                        </li>
                                                        <li>{t("autoIndexes.existingIndexInfo")}</li>
                                                    </ul>
                                                }
                                                placement="right"
                                            >
                                                <Icon icon="info" color="info" id="disableAutoIndexesInfo" />
                                            </PopoverWithHoverWrapper>
                                        </FormSwitch>
                                    </div>
                                </Card.Body>
                            </Card>
                        </div>
                    </Form>
                </Col>
                <Col sm={12} md={4}>
                    <AboutViewAnchored defaultOpen={hasStudioConfiguration ? null : "licensing"}>
                        <AccordionItemWrapper icon="about" color="info" targetId="1">
                            <p>
                                <StudioTrans
                                    ns="studioDatabaseConfiguration"
                                    i18nKey="about.scope"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </p>
                            <hr />
                            <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                            <a href={studioConfigurationDocsLink} target="_blank">
                                <Icon icon="newtab" /> {t("about.docsLink")}
                            </a>
                        </AccordionItemWrapper>
                        <FeatureAvailabilitySummaryWrapper
                            isUnlimited={hasStudioConfiguration}
                            data={featureAvailability}
                        />
                    </AboutViewAnchored>
                </Col>
            </Row>
        </div>
    );
}
