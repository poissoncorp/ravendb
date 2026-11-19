import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { licenseKeySchema, LicenseTypeToGenerate, SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import {
    FormCheckbox,
    FormGroup,
    FormInput,
    FormLabel,
    FormMultiRadioToggle,
    FormSelect,
    FormVerificationCodeInput,
    OptionalLabel,
} from "components/common/Form";
import { HStack } from "components/common/utilities/HStack";
import { setupWizardConstants } from "../utils/setupWizardConstants";
import Row from "react-bootstrap/Row";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import Badge from "react-bootstrap/Badge";
import messagePublisher from "common/messagePublisher";
import Modal from "components/common/Modal";
import useBoolean from "components/hooks/useBoolean";
import { useAsyncCallback, UseAsyncReturn } from "react-async-hook";
import { useEffect, useState } from "react";
import { get } from "lodash";
import { LazyLoad } from "components/common/LazyLoad";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";
import classNames from "classnames";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";
import LicenseType = Raven.Server.Commercial.LicenseType;
import License = Raven.Server.Commercial.License;

function scrollSetupWizardToTop() {
    const container = document.querySelector<HTMLElement>(".setup-wizard-main");
    container.scrollTo({ top: 0, left: 0 });
}

export function SetupWizardLicenseKeyStep() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        licenseKeyStep: { licenseTypeToGenerate },
    } = useWatch({ control });

    return (
        <div className="setup-wizard-license-key-step">
            {licenseTypeToGenerate == null && <NoLicenseToGenerate />}
            {licenseTypeToGenerate === "community" && <GenerateCommunity />}
            {licenseTypeToGenerate === "developer" && <GenerateDeveloper />}
        </div>
    );
}

function NoLicenseToGenerate() {
    const t = useStudioTranslation("setupWizard");
    const { reportEvent } = useEventsCollector();
    const { control, setValue } = useFormContext<SetupWizardFormData>();

    return (
        <div>
            <h2 className="mb-1">{t("licenseKeyStep.title")}</h2>
            <p className="mb-4 text-muted">{t("licenseKeyStep.description")}</p>

            <FormGroup>
                <FormLabel>{t("licenseKeyStep.keyLabel")}</FormLabel>
                <div className="position-relative">
                    <FormInput
                        type="textarea"
                        as="textarea"
                        control={control}
                        name="licenseKeyStep.key"
                        className="rounded-2 font-monospace"
                        placeholder={t("licenseKeyStep.keyPlaceholder", { example: KEY_PLACEHOLDER_EXAMPLE })}
                        rows={16}
                        data-testid="license-key-input"
                    />
                    <LicenseKeyBadge />
                </div>
            </FormGroup>
            <div className="free-license rounded-2 p-3 mb-3 panel-bg-1 border border-secondary">
                <div className="free-license-content">
                    <h4 className="mb-0">{t("licenseKeyStep.freeLicense.title")}</h4>
                    <p className="text-muted">{t("licenseKeyStep.freeLicense.description")}</p>
                    <Button
                        variant="outline-success"
                        className="rounded-1"
                        onClick={() => {
                            setValue("licenseKeyStep.licenseTypeToGenerate", "developer");
                            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "start-generate", "developer");
                            scrollSetupWizardToTop();
                        }}
                    >
                        {t("licenseKeyStep.freeLicense.getLicense")} <Icon icon="arrow-thin-right" margin="m-0" />
                    </Button>
                </div>
                <svg width="219" height="129" viewBox="0 0 219 129" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <g clipPath="url(#clip0_3218_7886)">
                        <path
                            d="M147.148 31.7465L181.336 82.5625L159.907 192.113L39.3529 168.531L69.0964 16.4785L147.148 31.7465ZM55.8859 157.666L148.197 175.723L148.293 175.233L148.486 175.271L165.265 89.4968L127.229 82.0565L134.727 43.7244L80.2586 33.0696L55.8859 157.666ZM110.638 144.312L109.086 152.245L71.4382 144.881L72.9901 136.947L110.638 144.312ZM144.83 127.244L143.278 135.177L75.914 122L77.4659 114.066L144.83 127.244ZM149.364 104.066L147.812 111.999L80.4479 98.8221L81.9998 90.8885L149.364 104.066ZM143.82 70.8953L158.57 73.7805L157.896 72.779L147.379 57.146L146.705 56.1445L143.82 70.8953Z"
                            stroke="var(--border-color-light)"
                        />
                    </g>
                    <defs>
                        <clipPath id="clip0_3218_7886">
                            <rect
                                width="193.783"
                                height="193.783"
                                fill="white"
                                transform="translate(37.8359 -9.27441) rotate(11.068)"
                            />
                        </clipPath>
                    </defs>
                </svg>
            </div>
        </div>
    );
}

const KEY_PLACEHOLDER_EXAMPLE = `{
    "Id": "0cb35dc0-598b-4416-b291-088439082dc1",
    "Name": "RavenDB",
    "Keys": [
        "A1B2C3D4E5F6G7H8I9J0",
        "K1L2M3N4O5P6Q7R8S9T0",
        "U1V2W3X4Y5Z6A7B8C9D0",
        "E1F2G3H4I5J6K7L8M9N0",
        "O1P2Q3R4S5T6U7V8W9X0",
        "Y1Z2A3B4C5D6E7F8G9H0",
        "I1J2K3L4M5N6O7P8Q9R0",
        "S1T2U3V4W5X6Y7Z8A9B0",
        "C1D2E3F4G5H6I7J8K9L0"
    ]
}
`;

function getLicenseTypeBadgeClass(licenseType: LicenseType) {
    switch (licenseType) {
        case "Community":
        case "Essential":
            return "bg-info";
        case "Developer":
            return "bg-developer";
        case "Professional":
            return "bg-professional";
        case "Enterprise":
            return "bg-primary";
        case "EnterpriseAi":
            return "bg-enterprise-ai";
        default:
            return "bg-secondary";
    }
}

function getLicenseTypeDisplayName(licenseType: LicenseType) {
    switch (licenseType) {
        case "Professional":
            return "Professional +";
        case "EnterpriseAi":
            return "RavenDB AI";
        default:
            return licenseType;
    }
}

function LicenseKeyBadge() {
    const t = useStudioTranslation("setupWizard");
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        licenseKeyStep: { licenseInfo, licenseStatus },
    } = useWatch({ control });

    if (licenseStatus === "loading") {
        return (
            <LazyLoad active>
                <Badge bg="secondary" pill className="position-absolute bottom-0 end-0 mb-3 me-3" style={{ zIndex: 5 }}>
                    {t("licenseKeyStep.keyStatus.loading")}
                </Badge>
            </LazyLoad>
        );
    }

    if (licenseStatus === "invalid") {
        return (
            <PopoverWithHoverWrapper
                wrapperClassName="position-absolute bottom-0 end-0 mb-3 me-3"
                targetStyle={{ zIndex: 5 }}
                message={t("licenseKeyStep.keyStatus.invalidDescription")}
            >
                <Badge bg="danger" pill>
                    {t("licenseKeyStep.keyStatus.invalid")}
                    <Icon icon="warning" margin="ms-2" />
                </Badge>
            </PopoverWithHoverWrapper>
        );
    }

    if (licenseStatus === "connection-error") {
        return (
            <PopoverWithHoverWrapper
                wrapperClassName="position-absolute bottom-0 end-0 mb-3 me-3"
                targetStyle={{ zIndex: 5 }}
                message={t("licenseKeyStep.keyStatus.connectionErrorDescription")}
            >
                <Badge bg="danger" pill>
                    {t("licenseKeyStep.keyStatus.connectionError")}
                    <Icon icon="warning" margin="ms-2" />
                </Badge>
            </PopoverWithHoverWrapper>
        );
    }

    if (licenseStatus !== "valid" || licenseInfo == null || licenseInfo.licenseStatus?.Type == null) {
        return null;
    }

    const badgeClass = getLicenseTypeBadgeClass(licenseInfo.licenseStatus.Type);
    const displayName = getLicenseTypeDisplayName(licenseInfo.licenseStatus.Type);

    return (
        <Badge
            pill
            className={classNames("position-absolute bottom-0 end-0 mb-3 me-3", badgeClass)}
            style={{ zIndex: 5 }}
        >
            {displayName}
        </Badge>
    );
}

function GenerateCommunity() {
    const t = useStudioTranslation("setupWizard");

    return (
        <div>
            <HStack className="justify-content-between flex-wrap align-items-start mb-1">
                <h2 className="m-0">
                    <StudioTrans
                        ns="setupWizard"
                        i18nKey="licenseKeyStep.community.title"
                        components={{ span: <span className="text-community" /> }}
                    />
                </h2>
                <LicenseTypeRadio />
            </HStack>
            <p className="text-muted">
                {t("licenseKeyStep.community.description")}
                <br />
                <br />
                <Icon icon="check" color="community" /> {t("licenseKeyStep.community.commercialUse")}
                <br />
                <Icon icon="check" color="community" /> {t("licenseKeyStep.community.features")}
                <br />
                <Icon icon="check" color="community" /> {t("licenseKeyStep.community.limits")}
            </p>
            <SeeAllPlansButton />
            <GenerateLicenseFields />
        </div>
    );
}

function GenerateDeveloper() {
    const t = useStudioTranslation("setupWizard");

    return (
        <div>
            <HStack className="justify-content-between flex-wrap align-items-start mb-1">
                <h2 className="m-0">
                    <StudioTrans
                        ns="setupWizard"
                        i18nKey="licenseKeyStep.developer.title"
                        components={{ span: <span className="text-developer" /> }}
                    />
                </h2>
                <LicenseTypeRadio />
            </HStack>
            <p className="text-muted">
                {t("licenseKeyStep.developer.description")}
                <br />
                <br />
                <Icon icon="cancel" color="developer" /> {t("licenseKeyStep.developer.commercialUse")}
                <br />
                <Icon icon="check" color="developer" /> {t("licenseKeyStep.developer.features")}
                <br />
                <Icon icon="check" color="developer" /> {t("licenseKeyStep.developer.limits")}
            </p>
            <SeeAllPlansButton />
            <GenerateLicenseFields />
        </div>
    );
}

function GenerateLicenseFields() {
    const t = useStudioTranslation("setupWizard");
    const { control } = useFormContext<SetupWizardFormData>();

    return (
        <Row className="w-100 mt-4">
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.firstName")}</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name="licenseKeyStep.firstName"
                    placeholder={t("licenseKeyStep.form.firstNamePlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.lastName")}</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name="licenseKeyStep.lastName"
                    placeholder={t("licenseKeyStep.form.lastNamePlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.email")}</FormLabel>
                <FormInput
                    type="email"
                    control={control}
                    name="licenseKeyStep.email"
                    placeholder={t("licenseKeyStep.form.emailPlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>
                    {t("licenseKeyStep.form.phone")} <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name="licenseKeyStep.phone"
                    placeholder={t("licenseKeyStep.form.phonePlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.country")}</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.countryOptions}
                    name="licenseKeyStep.country"
                    placeholder={t("licenseKeyStep.form.countryPlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.jobTitle")}</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.jobTitleOptions}
                    name="licenseKeyStep.jobTitle"
                    placeholder={t("licenseKeyStep.form.jobTitlePlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.industry")}</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.industryOptions}
                    name="licenseKeyStep.industry"
                    placeholder={t("licenseKeyStep.form.industryPlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.company")}</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name="licenseKeyStep.company"
                    placeholder={t("licenseKeyStep.form.companyPlaceholder")}
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>{t("licenseKeyStep.form.usagePlan")}</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.howYouPlanToUseRavenDBOptions}
                    name="licenseKeyStep.howYouPlanToUseRavenDB"
                    placeholder={t("licenseKeyStep.form.usagePlanPlaceholder")}
                />
            </FormGroup>
            <FormGroup marginClass="mb-0 mt-2">
                <FormCheckbox control={control} name="licenseKeyStep.isAcceptTerms">
                    {t("licenseKeyStep.form.acceptTerms")}
                </FormCheckbox>
            </FormGroup>
            <FormGroup>
                <FormCheckbox control={control} name="licenseKeyStep.isAcceptEmails">
                    {t("licenseKeyStep.form.acceptEmails")} <OptionalLabel />
                </FormCheckbox>
            </FormGroup>
        </Row>
    );
}

function SeeAllPlansButton() {
    const t = useStudioTranslation("setupWizard");

    return (
        <Button href="https://ravendb.net/buy" target="_blank" variant="link" className="p-0">
            {t("licenseKeyStep.seeAllPlans")} <Icon icon="newtab" />
        </Button>
    );
}

function LicenseTypeRadio() {
    const { control } = useFormContext<SetupWizardFormData>();

    return (
        <div>
            <FormMultiRadioToggle
                control={control}
                name="licenseKeyStep.licenseTypeToGenerate"
                inputItems={[
                    {
                        label: "Community",
                        value: "community",
                        badgeColor: "secondary",
                        icon: <Icon icon="community" />,
                    },
                    {
                        label: "Developer",
                        value: "developer",
                        badgeColor: "secondary",
                        icon: <Icon icon="console" />,
                    },
                ]}
                className="d-flex justify-content-center"
            />
        </div>
    );
}

function SkipLicenseVerificationConfirmModal(props: { close: () => void }) {
    const t = useStudioTranslation("setupWizard");
    const tCommon = useStudioTranslation("common");
    const { reportEvent } = useEventsCollector();
    const { close } = props;
    const { setValue } = useFormContext<SetupWizardFormData>();

    const handleConfirm = () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "skip-verification", "confirmed");
        setValue("licenseKeyStep.key", null);
        setValue("licenseKeyStep.licenseStatus", null);
        setValue("licenseKeyStep.licenseInfo", null);
        setValue("currentStep", "Security");
    };

    const handleLicenseTypeChange = (licenseType: LicenseTypeToGenerate) => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "choose-generate", licenseType);
        setValue("licenseKeyStep.licenseTypeToGenerate", licenseType);
        close();
    };

    return (
        <Modal show onHide={close} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Header closeButton onCloseClick={close} className="pb-0">
                <h3 className="mb-0">
                    <Icon icon="license" color="warning" />
                    {t("licenseKeyStep.skipVerification.title")}
                </h3>
            </Modal.Header>

            <Modal.Body className="pt-0">
                <p className="my-3">{t("licenseKeyStep.skipVerification.limitationsIntro")}</p>
                <Icon icon="check" color="success" />{" "}
                <StudioTrans
                    ns="setupWizard"
                    i18nKey="licenseKeyStep.skipVerification.agplRestrictions"
                    components={{ a: <a href="https://www.gnu.org/licenses/agpl-3.0.en.html" target="_blank" /> }}
                />
                <br />
                <Icon icon="check" color="success" /> {t("licenseKeyStep.skipVerification.limitedFeatures")}
                <br />
                <Icon icon="check" color="success" /> {t("licenseKeyStep.skipVerification.limits")}
                <p className="mt-3 mb-0">
                    <StudioTrans
                        ns="setupWizard"
                        i18nKey="licenseKeyStep.skipVerification.generateInstead"
                        components={{
                            community: (
                                <Button
                                    variant="link"
                                    className="text-info p-0 text-decoration-underline"
                                    onClick={() => handleLicenseTypeChange("community")}
                                    title={t("licenseKeyStep.skipVerification.generateCommunityTitle")}
                                />
                            ),
                            developer: (
                                <Button
                                    variant="link"
                                    className="text-developer p-0 text-decoration-underline"
                                    onClick={() => handleLicenseTypeChange("developer")}
                                    title={t("licenseKeyStep.skipVerification.generateDeveloperTitle")}
                                />
                            ),
                        }}
                    />
                </p>
            </Modal.Body>
            <Modal.Footer>
                <Button
                    variant="link"
                    onClick={() => {
                        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "skip-verification", "cancel");
                        close();
                    }}
                    className="link-muted"
                >
                    {tCommon("cancel")}
                </Button>
                <Button variant="warning" onClick={handleConfirm} className="rounded-pill d-flex align-items-center">
                    {t("licenseKeyStep.skipVerification.skip")} &nbsp;
                    <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

interface SetupWizardLicenseKeyVerifyCodeModalProps {
    close: () => void;
    sendLicenseVerificationCode: UseAsyncReturn<void, []>;
}

export function SetupWizardLicenseKeyVerifyCodeModal({
    close,
    sendLicenseVerificationCode,
}: SetupWizardLicenseKeyVerifyCodeModalProps) {
    const t = useStudioTranslation("setupWizard");
    const tCommon = useStudioTranslation("common");
    const { reportEvent } = useEventsCollector();
    const licenseKeyStepData: SetupWizardFormData["licenseKeyStep"] = useWatch<SetupWizardFormData>({
        name: "licenseKeyStep",
    });

    const { licenseService } = useServices();
    const {
        control,
        setValue,
        setError,
        clearErrors,
        setFocus,
        formState: { errors },
    } = useFormContext<SetupWizardFormData>();
    const { countdown, isCountdownActive, startCountdown } = useResendCountdown();

    useEffect(() => {
        // clear errors on remounting component
        setFocus("licenseKeyStep.verificationCode");
        if (get(errors, "licenseKeyStep.verificationCode")) {
            clearErrors("licenseKeyStep.verificationCode");
        }
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "open-verify-modal");
    }, []);

    const onSubmitVerifiedCode = useAsyncCallback(
        async (inputCode: string) => {
            const license = await licenseService.verifyLicense({
                Email: licenseKeyStepData.email,
                VerificationCode: inputCode,
            });

            if (license.LicenseDownloadStatus !== "Success") {
                throw new Error(license.LicenseDownloadStatus);
            }
            return license;
        },
        {
            onSuccess: async (license) => {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "verify-code", "success");
                setValue("licenseKeyStep.key", JSON.stringify(license.License, null, 2), {
                    shouldDirty: true,
                });
                close();
                setValue("licenseKeyStep.licenseTypeToGenerate", null);
            },
            onError: async (error) => {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "verify-code", error.message);
                setError("licenseKeyStep.verificationCode", {
                    message: convertVerificationCodeErrorMessage(t, error.message as FreeLicenseDownloadStatus),
                });
            },
        }
    );

    const handleResendClick = async () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "resend-code");
        await sendLicenseVerificationCode.execute().then(() => {
            startCountdown();
        });
    };

    return (
        <Modal
            show
            onHide={() => {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "close-verify-modal");
                close();
            }}
            contentClassName="modal-border bulge-primary"
            size="lg"
        >
            <Modal.Header
                closeButton
                onCloseClick={() => {
                    reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "close-verify-modal");
                    close();
                }}
                className="pb-0"
            >
                <h3 className="d-flex justify-content-center align-items-center w-100">
                    <Icon icon="paperplane" color="primary" />
                    {t("licenseKeyStep.verifyCode.title")}
                </h3>
            </Modal.Header>
            <Modal.Body className="d-flex align-items-center flex-column justify-items-center">
                <p className="text-center mb-1">{t("licenseKeyStep.verifyCode.confirmEmail")}</p>
                <p>
                    <StudioTrans
                        ns="setupWizard"
                        i18nKey="licenseKeyStep.verifyCode.checkInbox"
                        options={{ email: licenseKeyStepData?.email ?? "" }}
                        components={{ b: <b /> }}
                    />
                </p>
                <form>
                    <FormVerificationCodeInput
                        onLastDigitInsertSubmit={onSubmitVerifiedCode.execute}
                        name="licenseKeyStep.verificationCode"
                        control={control}
                    />
                </form>
                <p className="text-center mt-3">
                    <StudioTrans
                        ns="setupWizard"
                        i18nKey="licenseKeyStep.verifyCode.resend"
                        options={{
                            context: isCountdownActive
                                ? "countdown"
                                : sendLicenseVerificationCode.loading
                                  ? "sending"
                                  : "ready",
                            countdown,
                        }}
                        components={{
                            resend: (
                                <Button
                                    variant="link"
                                    className="m-0 p-0"
                                    onClick={handleResendClick}
                                    disabled={sendLicenseVerificationCode.loading || isCountdownActive}
                                />
                            ),
                        }}
                    />
                </p>
            </Modal.Body>
            <Modal.Footer>
                <Button
                    variant="link"
                    onClick={() => {
                        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "close-verify-modal");
                        close();
                    }}
                    className="link-muted"
                >
                    {tCommon("cancel")}
                </Button>
                <ButtonWithSpinner
                    isSpinning={onSubmitVerifiedCode.loading}
                    variant="primary"
                    disabled={licenseKeyStepData.verificationCode?.length !== 6}
                    className="rounded-pill"
                    onClick={() => onSubmitVerifiedCode.execute(licenseKeyStepData.verificationCode)}
                >
                    {t("licenseKeyStep.verifyCode.verifyEmail")}
                    <Icon icon="arrow-right" margin="ms-1" />
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}

function useResendCountdown(initialDelay: number = 30) {
    const [countdown, setCountdown] = useState(0);
    const [isActive, setIsActive] = useState(false);

    useEffect(() => {
        let interval: NodeJS.Timeout;
        if (isActive && countdown > 0) {
            interval = setInterval(() => {
                setCountdown((prev) => prev - 1);
            }, 1000);
        } else if (countdown === 0) {
            setIsActive(false);
        }
        return () => clearInterval(interval);
    }, [isActive, countdown]);

    const startCountdown = () => {
        setCountdown(initialDelay);
        setIsActive(true);
    };

    return { countdown, isCountdownActive: isActive, startCountdown };
}

function convertVerificationCodeErrorMessage(t: StudioTranslate<"setupWizard">, error: FreeLicenseDownloadStatus) {
    switch (error) {
        case "CodeAlreadyUsed":
            return t("licenseKeyStep.verifyCode.codeAlreadyUsed");
        case "CodeExpired":
            return t("licenseKeyStep.verifyCode.codeExpired");
        case "InvalidCredentials":
            return t("licenseKeyStep.verifyCode.invalidCredentials");
        default:
            return error;
    }
}

export function SetupWizardLicenseKeyStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { reportEvent } = useEventsCollector();
    const { control, setValue, trigger } = useFormContext<SetupWizardFormData>();
    const { setupWizardService } = useServices();

    const { value: isLicenseSkipModalOpen, toggle: toggleIsLicenseSkipModalOpen } = useBoolean(false);
    const { value: isLicenseActivationCodeModalOpen, toggle: toggleIsLicenseActivationCodeModalOpen } =
        useBoolean(false);

    const { licenseKeyStep } = useWatch({ control });

    const { key, licenseTypeToGenerate, licenseStatus } = licenseKeyStep;

    const toDto = (licenseStepData: SetupWizardFormData["licenseKeyStep"]): SendFreeLicenseVerificationRequest => {
        if (!licenseStepData) {
            return;
        }

        return {
            Type: licenseStepData.licenseTypeToGenerate === "developer" ? "Developer" : "Community",
            LicenseType: licenseStepData.licenseTypeToGenerate === "developer" ? "Developer" : "Community",
            FirstName: licenseStepData.firstName,
            LastName: licenseStepData.lastName,
            Email: licenseStepData.email,
            Company: licenseStepData.company,
            Country: licenseStepData.country,
            JobTitle: licenseStepData.jobTitle,
            Industry: licenseStepData.industry,
            HowDoYouPlanToUseRavenDb: licenseStepData.howYouPlanToUseRavenDB,
            AcceptTheTermsAndConditions: !!licenseStepData.isAcceptTerms,
            MarketingConsent: !!licenseStepData.isAcceptEmails,
        };
    };
    const asyncRegistrationInfo = useAsyncDebounce(
        async () => {
            setValue("licenseKeyStep.licenseStatus", "loading");
            setValue("licenseKeyStep.licenseInfo", null);

            if (!key) {
                setValue("licenseKeyStep.licenseStatus", null);
                return;
            }

            let parsedKey: License = null;

            try {
                parsedKey = JSON.parse(key);
                await licenseKeySchema.validate(parsedKey);
            } catch {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "key-parse", "invalid");
                setValue("licenseKeyStep.licenseStatus", "invalid");
                return;
            }

            try {
                const info = await setupWizardService.registrationInfo(parsedKey);

                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "key-parse", "valid");
                setValue(
                    "licenseKeyStep.licenseInfo",
                    {
                        licenseStatus: info.LicenseStatus,
                        userDomainsWithIps: {
                            email: info.UserDomainsWithIps.Emails,
                            rootDomains: info.UserDomainsWithIps.RootDomains,
                            domains: info.UserDomainsWithIps.Domains,
                        },
                    },
                    {
                        shouldDirty: true,
                    }
                );
                setValue("licenseKeyStep.licenseStatus", "valid");
            } catch (err) {
                const message: string = (err as JQueryXHR)?.responseJSON?.Message;
                const isConnError = message?.includes("Failed to contact");

                reportEvent(
                    setupWizardGA4Prefixes.licenseKeyStep,
                    "key-parse",
                    isConnError ? "connection-error" : "invalid"
                );

                setValue("licenseKeyStep.licenseStatus", isConnError ? "connection-error" : "invalid");
                setValue("licenseKeyStep.licenseInfo", null);
            }
        },
        [key],
        300
    );

    const { licenseService } = useServices();

    const asyncSendLicenseVerificationCode = useAsyncCallback(() =>
        licenseService.sendVerificationCode(toDto(licenseKeyStep))
    );

    const handleAlreadyHaveLicense = () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "already-have-license");
        setValue("licenseKeyStep.licenseTypeToGenerate", null);
        scrollSetupWizardToTop();
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "back");
        setValue("currentStep", "Setup method");
        scrollSetupWizardToTop();
        setValue("licenseKeyStep", setupWizardFormDefaultValues["licenseKeyStep"]);
    };

    const handleGenerateLicense = async () => {
        const fields: FieldPath<SetupWizardFormData>[] = [
            "licenseKeyStep.firstName",
            "licenseKeyStep.lastName",
            "licenseKeyStep.email",
            "licenseKeyStep.company",
            "licenseKeyStep.country",
            "licenseKeyStep.jobTitle",
            "licenseKeyStep.industry",
            "licenseKeyStep.howYouPlanToUseRavenDB",
            "licenseKeyStep.isAcceptTerms",
        ];
        const isValid = await trigger(fields);

        if (isValid) {
            reportEvent(
                setupWizardGA4Prefixes.licenseKeyStep,
                "send-verification-code",
                licenseTypeToGenerate ?? "unknown"
            );
            await asyncSendLicenseVerificationCode.execute();
            messagePublisher.reportSuccess(t("licenseKeyStep.codeSent"));
            toggleIsLicenseActivationCodeModalOpen();
        } else {
            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "send-verification-code", "validation-failed");
        }
    };

    const handleContinue = async () => {
        if (key && licenseStatus === "valid") {
            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "continue", "with-key");
            setValue("currentStep", "Security");
        } else {
            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "open-skip-modal");
            toggleIsLicenseSkipModalOpen();
        }
    };

    return (
        <div className="d-flex justify-content-between">
            {licenseTypeToGenerate != null ? (
                <Button variant="outline-secondary" className="rounded-pill" onClick={handleAlreadyHaveLicense}>
                    <Icon icon="license" /> {t("licenseKeyStep.alreadyHaveLicense")}
                </Button>
            ) : (
                <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                    <Icon icon="arrow-left" /> {t("actions.back")}
                </Button>
            )}

            {licenseTypeToGenerate != null ? (
                <Button variant="primary" className="rounded-pill" onClick={handleGenerateLicense}>
                    {t("licenseKeyStep.generateLicense")} <Icon icon="arrow-right" margin="m-0" />
                </Button>
            ) : (
                <ButtonWithSpinner
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                    isSpinning={asyncRegistrationInfo.loading}
                >
                    {key && licenseStatus === "valid" ? t("actions.continue") : t("licenseKeyStep.skipLicense")}
                    <Icon icon="arrow-right" margin="ms-1" />
                </ButtonWithSpinner>
            )}
            {isLicenseSkipModalOpen && <SkipLicenseVerificationConfirmModal close={toggleIsLicenseSkipModalOpen} />}
            {isLicenseActivationCodeModalOpen && (
                <SetupWizardLicenseKeyVerifyCodeModal
                    sendLicenseVerificationCode={asyncSendLicenseVerificationCode}
                    close={toggleIsLicenseActivationCodeModalOpen}
                />
            )}
        </div>
    );
}
