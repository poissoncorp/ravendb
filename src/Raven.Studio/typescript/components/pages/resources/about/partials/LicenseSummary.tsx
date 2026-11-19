import Card from "react-bootstrap/Card";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { ConnectivityStatus, OverallInfoItem } from "components/pages/resources/about/partials/common";
import { useAppDispatch, useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import LicenseType = Raven.Server.Commercial.LicenseType;
import registration from "viewmodels/shell/registration";
import { AsyncState } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "hooks/useServices";
import forceLicenseUpdateCommand from "commands/licensing/forceLicenseUpdateCommand";
import licenseModel from "models/auth/licenseModel";
import useConfirm from "components/common/ConfirmDialog";
import useUniqueId from "components/hooks/useUniqueId";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import moment from "moment";
import genUtils = require("common/generalUtils");
import Button from "react-bootstrap/Button";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { AiAssistantEulaModal } from "components/common/aiAssistant/AiAssistantEulaModal";
import useBoolean from "components/hooks/useBoolean";
import { aiAssistantActions, aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";
import { loadableData } from "components/models/common";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import ProgressBar from "react-bootstrap/ProgressBar";
import { aiAssistantConstants } from "components/common/aiAssistant/aiAssistantConstants";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface LicenseSummaryProps {
    asyncCheckLicenseServerConnectivity: AsyncState<ConnectivityStatus>;
    asyncGetConfigurationSettings: AsyncState<Raven.Server.Config.Categories.LicenseConfiguration>;
    recheckConnectivity: () => Promise<void>;
}

function canRenewLicense(licenseType: LicenseType) {
    return licenseType === "Developer" || licenseType === "Community";
}

export function LicenseSummary(props: LicenseSummaryProps) {
    const t = useStudioTranslation("aboutPage");
    const { recheckConnectivity, asyncCheckLicenseServerConnectivity, asyncGetConfigurationSettings } = props;

    const licenseStatus = useAppSelector(licenseSelectors.status);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const licenseType = useAppSelector(licenseSelectors.statusValue("Type"));
    const aiAssistantConsentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const aiAssistantUsage = useAppSelector(aiAssistantSelectors.usage);
    const isAiAssistantDisabled = useAppSelector(aiAssistantSelectors.isDisabled);

    const [refreshing, setRefreshing] = useState<boolean>(false);

    const aiUsageResetFormattedDate = moment().add(1, "month").startOf("month").format("MMM D, YYYY");

    const refreshConnectivity = async () => {
        setRefreshing(true);
        try {
            await recheckConnectivity();
        } finally {
            setRefreshing(false);
        }
    };

    return (
        <Card>
            <Card.Body>
                <h4>{t("license.title")}</h4>
                <div className="vstack gap-4">
                    <Row>
                        <OverallInfoItem icon="license" label={t("license.licenseType")}>
                            <span
                                className={classNames({
                                    "text-cloud": isCloud,
                                    "ai-gradient": licenseType === "EnterpriseAi",
                                })}
                            >
                                {licenseModel.licenseTypeTextProvider(licenseStatus)}
                            </span>
                        </OverallInfoItem>
                    </Row>
                    <Row>
                        <LicenseExpiration />
                    </Row>
                    <Row>
                        <OverallInfoItem icon="raven" label={t("license.licenseServer")}>
                            <ConnectivityStatusComponent
                                refreshing={refreshing}
                                refresh={refreshConnectivity}
                                status={asyncCheckLicenseServerConnectivity}
                            />
                        </OverallInfoItem>
                        <LicenseActions asyncGetConfigurationSettings={asyncGetConfigurationSettings} />
                    </Row>
                </div>
                {!isAiAssistantDisabled && (
                    <>
                        <hr />
                        <h4>{t("aiAssistant.title")}</h4>
                        <div className="vstack gap-4">
                            <Row>
                                <OverallInfoItem
                                    icon="ai"
                                    isFullWidth={aiAssistantUsage.status !== "failure"}
                                    label={
                                        <div className="hstack justify-content-between">
                                            <div>
                                                {t("aiAssistant.tokensUsage")}
                                                <PopoverWithHoverWrapper message={t("aiAssistant.tokensUsageInfo")}>
                                                    <Icon icon="info-new" margin="ms-1" />
                                                </PopoverWithHoverWrapper>
                                            </div>
                                            {aiAssistantUsage.data?.Status === "Success" && (
                                                <div>
                                                    {t("aiAssistant.resets", { date: aiUsageResetFormattedDate })}
                                                </div>
                                            )}
                                        </div>
                                    }
                                >
                                    <TokensUsageItem usage={aiAssistantUsage} />
                                </OverallInfoItem>
                                {aiAssistantUsage.status === "failure" && <AiAssistantRetryCheckUsageButton />}
                            </Row>
                            <Row>
                                <OverallInfoItem
                                    icon="document"
                                    label={
                                        <>
                                            {t("aiAssistant.consent")}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <span>
                                                        {t("aiAssistant.consentInfo")}{" "}
                                                        {aiAssistantConsentStatus.data === "Success" && (
                                                            <span>{t("aiAssistant.withdrawConsentInfo")}</span>
                                                        )}
                                                    </span>
                                                }
                                            >
                                                <Icon icon="info-new" margin="ms-1" />
                                            </PopoverWithHoverWrapper>
                                        </>
                                    }
                                >
                                    <ConsentStatusItem consentStatus={aiAssistantConsentStatus} />
                                </OverallInfoItem>
                                {aiAssistantConsentStatus.data === "Success" && <AiAssistantReviewConsentButton />}
                                {aiAssistantConsentStatus.data === "ConsentRequired" && (
                                    <AiAssistantGiveConsentButton />
                                )}
                                {aiAssistantConsentStatus.status === "failure" && (
                                    <AiAssistantRetryCheckConsentButton />
                                )}
                            </Row>
                        </div>
                    </>
                )}
            </Card.Body>
        </Card>
    );
}

function ConnectivityStatusComponent(props: {
    status: AsyncState<ConnectivityStatus>;
    refreshing: boolean;
    refresh: () => void;
}) {
    const t = useStudioTranslation("aboutPage");
    const { status, refresh, refreshing } = props;
    const uniqueId = useUniqueId("licenseConnectivityException");

    if (status.loading) {
        return (
            <LazyLoad active>
                <div>{t("loadingPlaceholder")}</div>
            </LazyLoad>
        );
    }

    if (status.status === "error") {
        return (
            <div className="m-3">
                <LoadError />
            </div>
        );
    }

    if (status.result.connected) {
        return (
            <span className="text-success">
                <Icon icon="check" />
                {t("license.connected")}
            </span>
        );
    }

    return (
        <div>
            <OverlayTrigger
                overlay={
                    <Tooltip id={uniqueId}>
                        {t("license.connectionException", { exception: status.result.exception })}
                    </Tooltip>
                }
            >
                <span className="text-warning">
                    <Icon icon="warning" />
                    <small>
                        <StudioTrans
                            ns="aboutPage"
                            i18nKey="licenseServerUnreachable"
                            components={{ code: <code /> }}
                        />
                    </small>
                </span>
            </OverlayTrigger>
            <ButtonWithSpinner
                isSpinning={refreshing}
                variant="outline-secondary"
                className="mt-2 rounded-pill"
                onClick={refresh}
            >
                <Icon icon="refresh" title={t("license.checkConnectionTitle")} /> {t("license.testAgain")}
            </ButtonWithSpinner>
        </div>
    );
}

interface LicenseActionsProps {
    asyncGetConfigurationSettings: AsyncState<Raven.Server.Config.Categories.LicenseConfiguration>;
}

function LicenseActions(props: LicenseActionsProps) {
    const t = useStudioTranslation("aboutPage");
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const licenseRegistered = useAppSelector(licenseSelectors.licenseRegistered);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const confirm = useConfirm();
    const { licenseService } = useServices();
    const [forcingUpdate, setForcingUpdate] = useState<boolean>(false);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const { asyncGetConfigurationSettings } = props;

    if (asyncGetConfigurationSettings.status !== "success") {
        return null;
    }

    const registerLicense = () => registration.showRegistrationDialog(licenseStatus, false, true);
    const renewLicense = () => registration.showRegistrationDialog(licenseStatus, false, true, true);

    const licenseConfiguration = asyncGetConfigurationSettings.result;

    if (licenseRegistered) {
        const isReplaceLicenseEnabled = licenseConfiguration.CanActivate && isClusterAdminOrClusterNode;
        const isForceUpdateEnabled = licenseConfiguration.CanForceUpdate && isClusterAdminOrClusterNode;
        const isRenewLicenseEnabled = licenseConfiguration.CanRenew && isClusterAdminOrClusterNode;

        const forceUpdate = async () => {
            const confirmed = await confirm({
                icon: "force",
                title: t("license.forceUpdateConfirm.title"),
                message: <p className="text-center">{t("license.forceUpdateConfirm.message")}</p>,
                actionColor: "primary",
                confirmText: t("license.forceUpdateConfirm.confirm"),
            });

            if (!confirmed) {
                return;
            }

            setForcingUpdate(true);
            try {
                const updateResult = await licenseService.forceUpdate();
                await licenseModel.fetchLicenseStatus();

                if (updateResult.Status === "NotModified") {
                    forceLicenseUpdateCommand.handleNotModifiedStatus(licenseStatus.Expired);
                }
                await licenseModel.fetchSupportCoverage();
            } finally {
                setForcingUpdate(false);
            }
        };

        return (
            <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                {canRenewLicense(licenseStatus.Type) && (
                    <React.Fragment key="renew-container">
                        <LicenseTooltip
                            target="renew-license-btn"
                            operationEnabledInConfiguration={licenseConfiguration.CanRenew}
                            hasPrivileges={isClusterAdminOrClusterNode}
                            operationAction={t("license.renewAction")}
                            operationTitle={t("license.renewTitle")}
                        >
                            <span>
                                <Button
                                    variant="outline-secondary"
                                    className="rounded-pill"
                                    onClick={renewLicense}
                                    disabled={!isRenewLicenseEnabled}
                                >
                                    <Icon icon="reset" /> {t("license.renew")}
                                </Button>
                            </span>
                        </LicenseTooltip>
                    </React.Fragment>
                )}
                {!isCloud && (
                    <React.Fragment key="replace-container">
                        <LicenseTooltip
                            target="replace-license-btn"
                            operationEnabledInConfiguration={licenseConfiguration.CanActivate}
                            hasPrivileges={isClusterAdminOrClusterNode}
                            operationAction={t("license.replaceAction")}
                            operationTitle={t("license.replaceTitle")}
                        >
                            <span>
                                <Button
                                    variant="outline-secondary"
                                    className="rounded-pill"
                                    onClick={registerLicense}
                                    disabled={!isReplaceLicenseEnabled}
                                >
                                    <Icon icon="replace" /> {t("license.replace")}
                                </Button>
                            </span>
                        </LicenseTooltip>
                    </React.Fragment>
                )}

                <LicenseTooltip
                    target="force-update-license-btn"
                    operationEnabledInConfiguration={licenseConfiguration.CanForceUpdate}
                    hasPrivileges={isClusterAdminOrClusterNode}
                    operationAction={t("license.forceUpdateAction")}
                    operationTitle={t("license.forceUpdateTitle")}
                >
                    <span>
                        <ButtonWithSpinner
                            isSpinning={forcingUpdate}
                            disabled={!isForceUpdateEnabled}
                            className="rounded-pill"
                            variant="outline-secondary"
                            onClick={forceUpdate}
                        >
                            <Icon icon="force" /> {t("license.forceUpdate")}
                        </ButtonWithSpinner>
                    </span>
                </LicenseTooltip>
            </Col>
        );
    }

    const isRegisterLicenseEnabled = licenseConfiguration.CanActivate && isClusterAdminOrClusterNode;

    return (
        <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
            <LicenseTooltip
                target="replace-license-btn"
                operationEnabledInConfiguration={licenseConfiguration.CanActivate}
                hasPrivileges={isClusterAdminOrClusterNode}
                operationAction={t("license.registerAction")}
                operationTitle={t("license.registerTitle")}
            >
                <div className="d-inline-block">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={registerLicense}
                        disabled={!isRegisterLicenseEnabled}
                    >
                        <Icon icon="replace" /> {t("license.register")}
                    </Button>
                </div>
            </LicenseTooltip>
        </Col>
    );
}

export function LicenseTooltip(props: {
    children: React.ReactNode;
    target: string;
    operationEnabledInConfiguration: boolean;
    hasPrivileges: boolean;
    operationAction: string;
    operationTitle: string;
}) {
    const t = useStudioTranslation("aboutPage");
    const { children, operationEnabledInConfiguration, operationTitle, operationAction, hasPrivileges } = props;

    const getMessage = () => {
        if (!operationEnabledInConfiguration) {
            return t("license.tooltip.disabledInConfiguration", {
                operation: operationTitle,
                context: hasPrivileges ? undefined : "noPrivileges",
            });
        }

        if (!hasPrivileges) {
            return t("license.tooltip.noPrivileges");
        }

        return operationAction;
    };

    const msg = getMessage();

    if (!msg) {
        return null;
    }

    return (
        <PopoverWithHoverWrapper message={msg}>
            <span>{children}</span>
        </PopoverWithHoverWrapper>
    );
}

function LicenseExpiration() {
    const t = useStudioTranslation("aboutPage");
    const subscriptionExpiration = useAppSelector(licenseSelectors.statusValue("SubscriptionExpiration"));
    const isIsv = useAppSelector(licenseSelectors.statusValue("IsIsv"));

    if (!subscriptionExpiration) {
        return null;
    }

    return (
        <OverallInfoItem icon="calendar" label={isIsv ? t("license.updatesExpiration") : t("license.expires")}>
            <LicenseExpirationDetails />
        </OverallInfoItem>
    );
}

function LicenseExpirationDetails() {
    const t = useStudioTranslation("aboutPage");
    const isExpired = useAppSelector(licenseSelectors.statusValue("Expired"));
    const subscriptionExpiration = useAppSelector(licenseSelectors.statusValue("SubscriptionExpiration"));
    const subscriptionExpirationUtc = moment.utc(subscriptionExpiration);

    const dateFormat = "YYYY MMMM Do";
    const nextMonth = moment.utc().add(1, "month");
    const duration = genUtils.formatDurationByDate(subscriptionExpirationUtc, true);

    return (
        <div>
            {t("license.expirationDate", { date: subscriptionExpirationUtc.format(dateFormat) })}{" "}
            <LicenseExpirationInfoPopover date={subscriptionExpirationUtc}>
                <Icon icon="info" color="info" id="utc-info" />
            </LicenseExpirationInfoPopover>
            <br />
            <small
                className={classNames({
                    "text-warning": !isExpired && subscriptionExpirationUtc.isBefore(nextMonth),
                    "text-danger": isExpired,
                })}
            >
                {duration}
            </small>
        </div>
    );
}

function LicenseExpirationInfoPopover({ date, children }: { date: moment.Moment; children: React.ReactNode }) {
    const t = useStudioTranslation("aboutPage");
    const isExpired = useAppSelector(licenseSelectors.statusValue("Expired"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    {t("license.expirationInfo", {
                        date: date.format("YYYY-MM-DD"),
                        localDate: date.local().format("YYYY-MM-DD HH:mm:ss"),
                        context: isExpired ? "expired" : "active",
                    })}
                    {isCloud && (
                        <div>
                            <br />
                            <Icon icon="cloud" /> {t("license.cloudAutoRenewal")}
                        </div>
                    )}
                </>
            }
        >
            {children}
        </PopoverWithHoverWrapper>
    );
}

function AiAssistantReviewConsentButton() {
    const t = useStudioTranslation("aboutPage");

    return (
        <Col className="d-flex align-items-center justify-content-end">
            <a
                href="https://ravendb.net/legal/ravendb/ai-assistant-terms-of-use"
                target="_blank"
                className="btn btn-outline-secondary rounded-pill"
            >
                {t("aiAssistant.reviewConsent")}
                <Icon icon="newtab" margin="ms-1" />
            </a>
        </Col>
    );
}

function AiAssistantGiveConsentButton() {
    const t = useStudioTranslation("aboutPage");
    const { value: isEulaOpen, toggle: toggleEulaOpen } = useBoolean(false);

    return (
        <Col className="d-flex align-items-center justify-content-end">
            <Button variant="outline-secondary" className="rounded-pill" onClick={toggleEulaOpen}>
                {t("aiAssistant.reviewConsent")}
                <Icon icon="open-modal" margin="ms-1" />
            </Button>
            {isEulaOpen && <AiAssistantEulaModal close={toggleEulaOpen} />}
        </Col>
    );
}

function AiAssistantRetryCheckConsentButton() {
    const t = useStudioTranslation("aboutPage");
    const dispatch = useAppDispatch();

    return (
        <Col className="d-flex align-items-center justify-content-end">
            <Button
                variant="outline-secondary"
                className="rounded-pill"
                onClick={() => dispatch(aiAssistantActions.checkConsent())}
            >
                <Icon icon="refresh" /> {t("aiAssistant.retry")}
            </Button>
        </Col>
    );
}

function AiAssistantRetryCheckUsageButton() {
    const t = useStudioTranslation("aboutPage");
    const dispatch = useAppDispatch();

    return (
        <Col className="d-flex align-items-center justify-content-end">
            <Button
                variant="outline-secondary"
                className="rounded-pill"
                onClick={() => dispatch(aiAssistantActions.checkUsage())}
            >
                <Icon icon="refresh" /> {t("aiAssistant.retry")}
            </Button>
        </Col>
    );
}

function TokensUsageItem({ usage }: { usage: loadableData<CheckUsageAiAssistantResultDto> }) {
    const t = useStudioTranslation("aboutPage");

    if (usage.status === "loading") {
        return (
            <LazyLoad active>
                <strong className="fs-4">{t("aiAssistant.checkingUsage")}</strong>
            </LazyLoad>
        );
    }

    if (usage.status === "failure") {
        return (
            <strong className="text-danger fs-4">
                <Icon icon="cancel" />
                {t("aiAssistant.checkUsageFailed")}
            </strong>
        );
    }

    if (usage.data?.Status === "InvalidCredentials") {
        return (
            <strong className="text-warning fs-4">
                <Icon icon="cancel" />
                {aiAssistantConstants.invalidCredentials}
            </strong>
        );
    }

    if (usage.data?.Status === "Success") {
        const remaining = Math.round(100 - usage.data.UsagePercentage);

        return (
            <div>
                <div className="fs-4">
                    <StudioTrans
                        ns="aboutPage"
                        i18nKey="aiAssistant.remaining"
                        components={{ strong: <strong /> }}
                        options={{ remaining }}
                    />
                </div>
                <ProgressBar now={remaining} className="tokens-usage-progress-bar" />
            </div>
        );
    }

    return null;
}

function ConsentStatusItem({
    consentStatus,
}: {
    consentStatus: loadableData<CheckConsentAiAssistantResultDto["Status"]>;
}) {
    const t = useStudioTranslation("aboutPage");

    if (consentStatus.status === "loading") {
        return (
            <LazyLoad active>
                <strong className="fs-4">{t("aiAssistant.checkingConsent")}</strong>
            </LazyLoad>
        );
    }

    if (consentStatus.status === "failure") {
        return (
            <strong className="text-danger fs-4">
                <Icon icon="cancel" />
                {aiAssistantConstants.failedToCheckConsent}
            </strong>
        );
    }

    if (consentStatus.data === "ConsentRequired") {
        return (
            <strong className="text-warning fs-4">
                <Icon icon="cancel" />
                {t("aiAssistant.notAccepted")}
            </strong>
        );
    }

    if (consentStatus.data === "InvalidCredentials") {
        return (
            <strong className="text-warning fs-4">
                <Icon icon="cancel" />
                {aiAssistantConstants.invalidCredentials}
            </strong>
        );
    }

    if (consentStatus.data === "Success") {
        return (
            <strong className="text-success fs-4">
                <Icon icon="check" />
                {t("aiAssistant.accepted")}
            </strong>
        );
    }

    return null;
}
