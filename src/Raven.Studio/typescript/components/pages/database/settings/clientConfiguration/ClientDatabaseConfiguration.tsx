import { useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormCheckbox, FormInput, FormRadioToggleWithIcon, FormSelect, FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import {
    ClientConfigurationFormData,
    clientConfigurationYupResolver,
} from "components/common/clientConfiguration/ClientConfigurationValidation";
import { Icon } from "components/common/Icon";
import appUrl = require("common/appUrl");
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormSideEffects from "components/common/clientConfiguration/useClientConfigurationFormSideEffects";
import { tryHandleSubmit } from "components/utils/common";
import classNames from "classnames";
import { RadioToggleWithIconInputItem } from "components/common/toggles/RadioToggle";
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
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ClientDatabaseConfiguration() {
    const t = useStudioTranslation("clientConfiguration");
    const commonT = useStudioTranslation("common");
    const { manageServerService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncGetClientGlobalConfiguration = useAsyncCallback(async () => {
        const globalConfigResult = await manageServerService.getGlobalClientConfiguration();
        if (!globalConfigResult) {
            return null;
        }

        return ClientConfigurationUtils.mapToFormData(globalConfigResult);
    });

    const asyncGetDefaultValues = useAsyncCallback(async () => {
        try {
            const globalConfiguration = await asyncGetClientGlobalConfiguration.execute();
            const clientConfiguration = await manageServerService.getClientConfiguration(databaseName);

            const overrideConfig = !globalConfiguration || (clientConfiguration && !clientConfiguration.Disabled);

            return ClientConfigurationUtils.mapToFormData(clientConfiguration, overrideConfig);
        } catch {
            return ClientConfigurationUtils.mapToFormData(null);
        }
    });

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const { handleSubmit, control, formState, watch, reset, setValue } = useForm<ClientConfigurationFormData>({
        resolver: clientConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncGetDefaultValues.execute,
    });

    useDirtyFlag(formState.isDirty);

    const loadBalancingLink = useRavenLink({ hash: "GYJ8JA" });
    const clientConfigurationLink = useRavenLink({ hash: "XYJ3B3" });

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

    const formValues = useWatch({ control: control });

    useClientConfigurationFormSideEffects(watch, setValue);

    useEffect(() => {
        if (formState.isSubmitSuccessful) {
            reset(formValues);
        }
    }, [formState.isSubmitSuccessful, reset, formValues]);

    const onSave: SubmitHandler<ClientConfigurationFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            await manageServerService.saveClientConfiguration(
                ClientConfigurationUtils.mapToDto(formData, false),
                databaseName
            );
        });
    };

    const onRefresh = async () => {
        reset(await asyncGetDefaultValues.execute());
    };

    const globalConfig = asyncGetClientGlobalConfiguration.result;

    if (asyncGetDefaultValues.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error) {
        return <LoadError error={t("loadError")} refresh={onRefresh} />;
    }

    const canEditDatabaseConfig = formValues.overrideConfig || !globalConfig;

    const leftRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
        label: t("useServerConfig"),
        value: false,
        iconName: "server",
    };

    const rightRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
        label: t("useDatabaseConfig"),
        value: true,
        iconName: "database",
    };

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
                        <div className="d-flex align-items-center justify-content-between flex-wrap gap-3 mb-3">
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
                                >
                                    {formState.isSubmitting ? (
                                        <Spinner size="sm" className="me-1" />
                                    ) : (
                                        <i className="icon-save me-1" />
                                    )}
                                    {commonT("save")}
                                </Button>
                            </ConditionalPopover>

                            {isClusterAdminOrClusterNode && (
                                <small title={t("serverWideLinkTitle")}>
                                    <a target="_blank" href={appUrl.forGlobalClientConfiguration()}>
                                        <Icon icon="link" />
                                        {t("serverWideLink")}
                                    </a>
                                </small>
                            )}
                        </div>
                        <div className={hasClientConfiguration ? "" : "item-disabled pe-none"}>
                            {globalConfig && (
                                <div className="mt-4 mb-3">
                                    <div className="w-fit-content mx-auto">
                                        <FormRadioToggleWithIcon
                                            name="overrideConfig"
                                            control={control}
                                            leftItem={leftRadioToggleItem}
                                            rightItem={rightRadioToggleItem}
                                        />
                                    </div>
                                </div>
                            )}

                            <Row>
                                {globalConfig && (
                                    <Col>
                                        <h4 className="mb-3">
                                            <Icon icon="server" />
                                            {t("serverConfigHeading")}
                                            {isClusterAdminOrClusterNode && (
                                                <a
                                                    target="_blank"
                                                    href={appUrl.forGlobalClientConfiguration()}
                                                    className="ms-1 no-decor"
                                                    title={t("serverSettingsTitle")}
                                                >
                                                    <Icon icon="link" />
                                                </a>
                                            )}
                                        </h4>
                                        <Card className={canEditDatabaseConfig && "item-disabled"}>
                                            <div className="p-4">
                                                <div className="md-label">
                                                    {t("identityPartsSeparator.label")}{" "}
                                                    <PopoverWithHoverWrapper
                                                        placement="right"
                                                        message={
                                                            <StudioTrans
                                                                ns="clientConfiguration"
                                                                i18nKey="identityPartsSeparator.info"
                                                                components={{ i: <i />, br: <br />, code: <code /> }}
                                                            />
                                                        }
                                                    >
                                                        <Icon icon="info" color="info" />
                                                    </PopoverWithHoverWrapper>
                                                </div>
                                                <Form.Control
                                                    defaultValue={globalConfig.identityPartsSeparatorValue}
                                                    disabled
                                                    placeholder={
                                                        globalConfig.identityPartsSeparatorValue ||
                                                        t("identityPartsSeparator.placeholder")
                                                    }
                                                />
                                                <div className="md-label mt-4">
                                                    {t("maxRequests.label")}{" "}
                                                    <PopoverWithHoverWrapper
                                                        placement="right"
                                                        message={
                                                            <StudioTrans
                                                                ns="clientConfiguration"
                                                                i18nKey="maxRequests.serverInfo"
                                                                components={{ code: <code /> }}
                                                            />
                                                        }
                                                    >
                                                        <Icon icon="info" color="info" />
                                                    </PopoverWithHoverWrapper>
                                                </div>
                                                <Form.Control
                                                    defaultValue={globalConfig.maximumNumberOfRequestsValue}
                                                    disabled
                                                    placeholder={
                                                        globalConfig.maximumNumberOfRequestsValue
                                                            ? globalConfig.maximumNumberOfRequestsValue.toLocaleString()
                                                            : t("maxRequests.placeholder")
                                                    }
                                                />
                                            </div>
                                        </Card>
                                    </Col>
                                )}
                                <Col>
                                    <h4 className="mb-3">
                                        <Icon icon="database" />
                                        {t("databaseConfigHeading")}
                                    </h4>
                                    <Card className={classNames({ "item-disabled": !canEditDatabaseConfig })}>
                                        <div className="p-4">
                                            <div className="md-label">
                                                {t("identityPartsSeparator.label")}{" "}
                                                <PopoverWithHoverWrapper
                                                    placement="right"
                                                    message={
                                                        <StudioTrans
                                                            ns="clientConfiguration"
                                                            i18nKey="identityPartsSeparator.info"
                                                            components={{ i: <i />, br: <br />, code: <code /> }}
                                                        />
                                                    }
                                                >
                                                    <Icon icon="info" color="info" />
                                                </PopoverWithHoverWrapper>
                                            </div>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="identityPartsSeparatorEnabled"
                                                        disabled={!canEditDatabaseConfig}
                                                        color="primary"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="text"
                                                    control={control}
                                                    name="identityPartsSeparatorValue"
                                                    placeholder={t("identityPartsSeparator.placeholder")}
                                                    disabled={
                                                        !formValues.identityPartsSeparatorEnabled ||
                                                        !canEditDatabaseConfig
                                                    }
                                                    className="d-flex"
                                                />
                                            </InputGroup>
                                            <div className="md-label mt-4">
                                                {t("maxRequests.label")}{" "}
                                                <PopoverWithHoverWrapper
                                                    placement="right"
                                                    message={
                                                        <StudioTrans
                                                            ns="clientConfiguration"
                                                            i18nKey="maxRequests.databaseInfo"
                                                            components={{ br: <br />, code: <code /> }}
                                                        />
                                                    }
                                                >
                                                    <Icon icon="info" color="info" />
                                                </PopoverWithHoverWrapper>
                                            </div>
                                            <InputGroup>
                                                <div className="toggle-field-checkbox">
                                                    <FormCheckbox
                                                        control={control}
                                                        name="maximumNumberOfRequestsEnabled"
                                                        disabled={!canEditDatabaseConfig}
                                                        color="primary"
                                                    />
                                                </div>
                                                <FormInput
                                                    type="number"
                                                    control={control}
                                                    name="maximumNumberOfRequestsValue"
                                                    placeholder={t("maxRequests.placeholder")}
                                                    disabled={
                                                        !formValues.maximumNumberOfRequestsEnabled ||
                                                        !canEditDatabaseConfig
                                                    }
                                                />
                                            </InputGroup>
                                        </div>
                                    </Card>
                                </Col>
                            </Row>

                            <div
                                className={classNames(
                                    "d-flex mt-3 position-relative",
                                    { "justify-content-center": globalConfig },
                                    { "justify-content-between": !globalConfig }
                                )}
                            >
                                <h5 className={globalConfig && "text-center"}>{t("loadBalancing.heading")}</h5>
                                <small title={t("loadBalancing.tutorialLinkTitle")} className="position-absolute end-0">
                                    <a href={loadBalancingLink} target="_blank">
                                        <Icon icon="link" /> {t("loadBalancing.tutorialLink")}
                                    </a>
                                </small>
                            </div>

                            <Row>
                                {globalConfig && (
                                    <Col>
                                        <Card className={classNames("p-4", { "item-disabled": canEditDatabaseConfig })}>
                                            <div className="md-label">
                                                {t("loadBalancing.behaviorLabel")}{" "}
                                                <PopoverWithHoverWrapper
                                                    message={
                                                        <>
                                                            <span className="d-inline-block mb-1">
                                                                <StudioTrans
                                                                    ns="clientConfiguration"
                                                                    i18nKey="loadBalancing.behaviorInfo"
                                                                    components={{ strong: <strong /> }}
                                                                />
                                                            </span>
                                                            <ul>
                                                                <li className="mb-1">
                                                                    <StudioTrans
                                                                        ns="clientConfiguration"
                                                                        i18nKey="loadBalancing.behaviorInfoNone"
                                                                        components={{
                                                                            code: <code />,
                                                                            br: <br />,
                                                                            strong: <strong />,
                                                                        }}
                                                                    />
                                                                </li>
                                                                <li className="mb-1">
                                                                    <StudioTrans
                                                                        ns="clientConfiguration"
                                                                        i18nKey="loadBalancing.behaviorInfoSessionContext"
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
                                                >
                                                    <Icon icon="info" color="info" />
                                                </PopoverWithHoverWrapper>
                                            </div>
                                            <Form.Control
                                                defaultValue={globalConfig.loadBalancerValue}
                                                disabled
                                                placeholder={t("loadBalancing.nonePlaceholder")}
                                            />
                                            {(globalConfig?.loadBalancerSeedValue ||
                                                formValues.loadBalancerValue === "UseSessionContext") && (
                                                <>
                                                    <div className="md-label mt-4">
                                                        {" "}
                                                        {t("loadBalancing.seedLabel")}
                                                        <PopoverWithHoverWrapper
                                                            placement="right"
                                                            message={
                                                                <StudioTrans
                                                                    ns="clientConfiguration"
                                                                    i18nKey="loadBalancing.seedInfo"
                                                                    components={{ br: <br /> }}
                                                                />
                                                            }
                                                        >
                                                            <Icon icon="info" color="info" />
                                                        </PopoverWithHoverWrapper>
                                                    </div>
                                                    <Form.Control
                                                        defaultValue={globalConfig.loadBalancerSeedValue}
                                                        disabled
                                                        placeholder={t("loadBalancing.seedPlaceholder")}
                                                    />
                                                </>
                                            )}
                                            <div className="md-label mt-4">
                                                {t("loadBalancing.readBehaviorLabel")}{" "}
                                                <PopoverWithHoverWrapper
                                                    placement="right"
                                                    message={
                                                        <StudioTrans
                                                            ns="clientConfiguration"
                                                            i18nKey="loadBalancing.readBehaviorInfo"
                                                            components={{ code: <code />, br: <br /> }}
                                                        />
                                                    }
                                                >
                                                    <Icon icon="info" color="info" />
                                                </PopoverWithHoverWrapper>
                                            </div>
                                            <Form.Control
                                                defaultValue={globalConfig.readBalanceBehaviorValue}
                                                placeholder={t("loadBalancing.nonePlaceholder")}
                                                disabled
                                            />
                                        </Card>
                                    </Col>
                                )}
                                <Col>
                                    <Card className={classNames("p-4", { "item-disabled": !canEditDatabaseConfig })}>
                                        <div className="md-label">
                                            {t("loadBalancing.behaviorLabel")}{" "}
                                            <PopoverWithHoverWrapper
                                                placement="right"
                                                message={
                                                    <>
                                                        <span className="d-inline-block mb-1">
                                                            <StudioTrans
                                                                ns="clientConfiguration"
                                                                i18nKey="loadBalancing.behaviorInfo"
                                                                components={{ strong: <strong /> }}
                                                            />
                                                        </span>
                                                        <ul>
                                                            <li className="mb-1">
                                                                <StudioTrans
                                                                    ns="clientConfiguration"
                                                                    i18nKey="loadBalancing.behaviorInfoNone"
                                                                    components={{
                                                                        code: <code />,
                                                                        br: <br />,
                                                                        strong: <strong />,
                                                                    }}
                                                                />
                                                            </li>
                                                            <li className="mb-1">
                                                                <StudioTrans
                                                                    ns="clientConfiguration"
                                                                    i18nKey="loadBalancing.behaviorInfoSessionContext"
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
                                            >
                                                <Icon icon="info" color="info" />
                                            </PopoverWithHoverWrapper>
                                        </div>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox
                                                    control={control}
                                                    name="loadBalancerEnabled"
                                                    disabled={!canEditDatabaseConfig}
                                                    color="primary"
                                                />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="loadBalancerValue"
                                                isDisabled={!formValues.loadBalancerEnabled || !canEditDatabaseConfig}
                                                options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                        {(globalConfig?.loadBalancerSeedValue ||
                                            formValues.loadBalancerValue === "UseSessionContext") && (
                                            <>
                                                <div className="md-label mt-4">
                                                    {" "}
                                                    {t("loadBalancing.seedLabel")}
                                                    <PopoverWithHoverWrapper
                                                        placement="right"
                                                        message={
                                                            <StudioTrans
                                                                ns="clientConfiguration"
                                                                i18nKey="loadBalancing.seedInfo"
                                                                components={{ br: <br /> }}
                                                            />
                                                        }
                                                    >
                                                        <Icon icon="info" color="info" />
                                                    </PopoverWithHoverWrapper>
                                                </div>

                                                <div className="hstack gap-3">
                                                    <FormSwitch
                                                        control={control}
                                                        name="loadBalancerSeedEnabled"
                                                        color="primary"
                                                        disabled={
                                                            formValues.loadBalancerValue !== "UseSessionContext" ||
                                                            !canEditDatabaseConfig
                                                        }
                                                        className="small"
                                                    ></FormSwitch>
                                                    <InputGroup>
                                                        <FormInput
                                                            type="number"
                                                            control={control}
                                                            name="loadBalancerSeedValue"
                                                            placeholder={t("loadBalancing.seedPlaceholder")}
                                                            disabled={
                                                                !formValues.loadBalancerSeedEnabled ||
                                                                !canEditDatabaseConfig
                                                            }
                                                        />
                                                    </InputGroup>
                                                </div>
                                            </>
                                        )}
                                        <div className="md-label mt-4">
                                            {t("loadBalancing.readBehaviorLabel")}{" "}
                                            <PopoverWithHoverWrapper
                                                placement="right"
                                                message={
                                                    <StudioTrans
                                                        ns="clientConfiguration"
                                                        i18nKey="loadBalancing.readBehaviorInfo"
                                                        components={{ code: <code />, br: <br /> }}
                                                    />
                                                }
                                            >
                                                <Icon icon="info" color="info" />
                                            </PopoverWithHoverWrapper>
                                        </div>
                                        <InputGroup>
                                            <div className="toggle-field-checkbox">
                                                <FormCheckbox
                                                    control={control}
                                                    name="readBalanceBehaviorEnabled"
                                                    disabled={!canEditDatabaseConfig}
                                                    color="primary"
                                                />
                                            </div>
                                            <FormSelect
                                                control={control}
                                                name="readBalanceBehaviorValue"
                                                isDisabled={
                                                    !formValues.readBalanceBehaviorEnabled || !canEditDatabaseConfig
                                                }
                                                options={ClientConfigurationUtils.getReadBalanceBehaviorOptions()}
                                                isSearchable={false}
                                            />
                                        </InputGroup>
                                    </Card>
                                </Col>
                            </Row>
                        </div>
                    </Col>
                    <Col sm={12} md={4}>
                        <AboutViewAnchored defaultOpen={hasClientConfiguration ? null : "licensing"}>
                            <AccordionItemWrapper icon="about" color="info" targetId="1">
                                <ul>
                                    <li className="margin-bottom-xs">
                                        <StudioTrans
                                            ns="clientConfiguration"
                                            i18nKey="about.viewItem"
                                            components={{ strong: <strong />, br: <br /> }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="clientConfiguration"
                                            i18nKey="about.overrideServerWideItem"
                                            components={{ strong: <strong />, br: <br /> }}
                                        />
                                    </li>
                                </ul>
                                <hr />
                                <ul>
                                    <li className="margin-bottom-xs">
                                        <StudioTrans
                                            ns="clientConfiguration"
                                            i18nKey="about.overrideClientItem"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                    <li className="margin-bottom-xs">{t("about.updateItem")}</li>
                                    <li>
                                        <StudioTrans
                                            ns="clientConfiguration"
                                            i18nKey="about.dynamicControlItem"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                </ul>
                                <hr />
                                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                                <a href={clientConfigurationLink} target="_blank">
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
            <div id="PopoverContainer"></div>
        </Form>
    );
}
