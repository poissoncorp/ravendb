import { useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormCheckbox, FormInput, FormSelect, FormSwitch } from "components/common/Form";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "../../../../common/clientConfiguration/ClientConfigurationValidation";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormSideEffects from "components/common/clientConfiguration/useClientConfigurationFormSideEffects";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ClientGlobalConfiguration() {
    const t = useStudioTranslation("serverClientConfiguration");
    const commonT = useStudioTranslation("common");
    const { manageServerService } = useServices();
    const asyncGetGlobalClientConfiguration = useAsyncCallback(manageServerService.getGlobalClientConfiguration);

    const { handleSubmit, control, formState, setValue, reset, watch } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: async () =>
            ClientConfigurationUtils.mapToFormData(await asyncGetGlobalClientConfiguration.execute()),
    });

    const loadBalancingDocsLink = useRavenLink({ hash: "GYJ8JA" });
    const clientConfigurationDocsLink = useRavenLink({ hash: "TS7SGF" });

    const formValues = useWatch({ control });

    useClientConfigurationFormSideEffects(watch, setValue);

    useEffect(() => {
        if (formState.isSubmitSuccessful) {
            reset(formValues);
        }
    }, [formState.isSubmitSuccessful, reset, formValues]);

    useDirtyFlag(formState.isDirty);

    const hasClientConfiguration = useAppSelector(licenseSelectors.statusValue("HasClientConfiguration"));
    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.clientConfiguration"),
            featureIcon: "client-configuration",
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
                value: hasClientConfiguration,
            },
        ],
    });

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveGlobalClientConfiguration(ClientConfigurationUtils.mapToDto(formData, true));
        });
    };

    const onRefresh = async () => {
        reset(ClientConfigurationUtils.mapToFormData(await asyncGetGlobalClientConfiguration.execute()));
    };

    if (asyncGetGlobalClientConfiguration.loading) {
        return <LoadingView />;
    }

    if (asyncGetGlobalClientConfiguration.error) {
        return <LoadError error={t("loadError")} refresh={onRefresh} />;
    }

    return (
        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
            <div className="content-margin">
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            icon="database-client-configuration"
                            title={t("title")}
                            licenseBadgeText={hasClientConfiguration ? null : "Professional +"}
                        />
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasClientConfiguration,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <Button
                                type="submit"
                                variant="primary"
                                disabled={formState.isSubmitting || !formState.isDirty}
                                className="mb-3"
                            >
                                {formState.isSubmitting ? <Spinner size="sm" className="me-1" /> : <Icon icon="save" />}
                                {commonT("save")}
                            </Button>
                        </ConditionalPopover>
                        <div className={hasClientConfiguration ? "" : "item-disabled pe-none"}>
                            <Card className="flex-column p-4">
                                <div>
                                    <div className="d-flex flex-grow-1">
                                        <div className="md-label">
                                            {t("identityPartsSeparator.label")}{" "}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <StudioTrans
                                                        ns="serverClientConfiguration"
                                                        i18nKey="identityPartsSeparator.info"
                                                        components={{ i: <i />, br: <br />, code: <code /> }}
                                                    />
                                                }
                                                placement="right"
                                            >
                                                <Icon icon="info" color="info" />
                                            </PopoverWithHoverWrapper>
                                        </div>
                                    </div>
                                    <Row>
                                        <Col>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="identityPartsSeparatorEnabled"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="text"
                                                    control={control}
                                                    name="identityPartsSeparatorValue"
                                                    placeholder={t("identityPartsSeparator.placeholder")}
                                                    disabled={!formValues.identityPartsSeparatorEnabled}
                                                    className="d-flex"
                                                />
                                            </InputGroup>
                                        </Col>
                                    </Row>
                                </div>
                                <div className="mt-4">
                                    <div className="d-flex flex-grow-1">
                                        <div className="md-label">
                                            {t("maximumNumberOfRequests.label")}{" "}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <StudioTrans
                                                        ns="serverClientConfiguration"
                                                        i18nKey="maximumNumberOfRequests.info"
                                                        components={{ code: <code /> }}
                                                    />
                                                }
                                                placement="right"
                                            >
                                                <Icon icon="info" color="info" />
                                            </PopoverWithHoverWrapper>
                                        </div>
                                    </div>
                                    <Row>
                                        <Col>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="maximumNumberOfRequestsEnabled"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="number"
                                                    control={control}
                                                    name="maximumNumberOfRequestsValue"
                                                    placeholder={t("maximumNumberOfRequests.placeholder")}
                                                    disabled={!formValues.maximumNumberOfRequestsEnabled}
                                                />
                                            </InputGroup>
                                        </Col>
                                    </Row>
                                </div>
                            </Card>
                            <div className="d-flex justify-content-between mt-4 position-relative">
                                <h5>{t("loadBalancing.heading")}</h5>
                                <small title={t("loadBalancing.tutorialTitle")} className="position-absolute end-0">
                                    <a href={loadBalancingDocsLink} target="_blank">
                                        <Icon icon="link" /> {t("loadBalancing.tutorialLink")}
                                    </a>
                                </small>
                            </div>
                            <Card className="flex-column p-4">
                                <div className="d-flex flex-grow-1">
                                    <div className="md-label">
                                        {t("loadBalanceBehavior.label")}{" "}
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    <span className="d-inline-block mb-1">
                                                        <StudioTrans
                                                            ns="serverClientConfiguration"
                                                            i18nKey="loadBalanceBehavior.info"
                                                            components={{ strong: <strong /> }}
                                                        />
                                                    </span>
                                                    <ul>
                                                        <li className="mb-1">
                                                            <StudioTrans
                                                                ns="serverClientConfiguration"
                                                                i18nKey="loadBalanceBehavior.noneItem"
                                                                components={{
                                                                    code: <code />,
                                                                    br: <br />,
                                                                    strong: <strong />,
                                                                }}
                                                            />
                                                        </li>
                                                        <li className="mb-1">
                                                            <StudioTrans
                                                                ns="serverClientConfiguration"
                                                                i18nKey="loadBalanceBehavior.sessionContextItem"
                                                                components={{
                                                                    code: <code />,
                                                                    br: <br />,
                                                                    strong: <strong />,
                                                                }}
                                                            />
                                                        </li>
                                                    </ul>
                                                </>
                                            }
                                            placement="right"
                                        >
                                            <Icon icon="info" color="info" />
                                        </PopoverWithHoverWrapper>
                                    </div>
                                </div>
                                <Row>
                                    <Col>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox control={control} name="loadBalancerEnabled" />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="loadBalancerValue"
                                                isDisabled={!formValues.loadBalancerEnabled}
                                                options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                    </Col>
                                </Row>
                                {formValues.loadBalancerValue === "UseSessionContext" && (
                                    <>
                                        <div className="md-label mt-4">
                                            {t("seed.label")}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <>
                                                        {" "}
                                                        <StudioTrans
                                                            ns="serverClientConfiguration"
                                                            i18nKey="seed.info"
                                                            components={{ br: <br /> }}
                                                        />
                                                    </>
                                                }
                                                placement="right"
                                            >
                                                <Icon icon="info" color="info" />
                                            </PopoverWithHoverWrapper>
                                        </div>
                                        <Row>
                                            <Col className="hstack gap-3">
                                                <FormSwitch
                                                    control={control}
                                                    name="loadBalancerSeedEnabled"
                                                    color="primary"
                                                    className="small"
                                                />
                                                <InputGroup>
                                                    <FormInput
                                                        type="number"
                                                        control={control}
                                                        name="loadBalancerSeedValue"
                                                        placeholder={t("seed.placeholder")}
                                                        disabled={!formValues.loadBalancerSeedEnabled}
                                                    />
                                                </InputGroup>
                                            </Col>
                                        </Row>
                                    </>
                                )}
                                <div className="d-flex flex-grow-1">
                                    <div className="md-label mt-4">
                                        {t("readBalanceBehavior.label")}{" "}
                                        <PopoverWithHoverWrapper
                                            message={
                                                <StudioTrans
                                                    ns="serverClientConfiguration"
                                                    i18nKey="readBalanceBehavior.info"
                                                    components={{ code: <code />, br: <br /> }}
                                                />
                                            }
                                            placement="right"
                                        >
                                            <Icon id="SetReadBalanceBehavior" icon="info" color="info" />
                                        </PopoverWithHoverWrapper>
                                    </div>
                                </div>
                                <Row>
                                    <Col>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox control={control} name="readBalanceBehaviorEnabled" />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="readBalanceBehaviorValue"
                                                isDisabled={!formValues.readBalanceBehaviorEnabled}
                                                options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                    </Col>
                                </Row>
                            </Card>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored defaultOpen={hasClientConfiguration ? null : "licensing"}>
                            <AccordionItemWrapper icon="about" color="info" targetId="1">
                                <ul>
                                    <li className="margin-bottom-xs">
                                        <StudioTrans
                                            ns="serverClientConfiguration"
                                            i18nKey="about.thisView"
                                            components={{ strong: <strong />, br: <br /> }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="serverClientConfiguration"
                                            i18nKey="about.perDatabase"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                </ul>
                                <hr />
                                <ul>
                                    <li className="margin-bottom-xs">
                                        <StudioTrans
                                            ns="serverClientConfiguration"
                                            i18nKey="about.override"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                    <li className="margin-bottom-xs">{t("about.nextRequest")}</li>
                                    <li>
                                        <StudioTrans
                                            ns="serverClientConfiguration"
                                            i18nKey="about.dynamicControl"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                </ul>
                                <hr />
                                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                                <a href={clientConfigurationDocsLink} target="_blank">
                                    <Icon icon="newtab" /> {t("about.docsLink")}
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={hasClientConfiguration}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </div>
        </Form>
    );
}
