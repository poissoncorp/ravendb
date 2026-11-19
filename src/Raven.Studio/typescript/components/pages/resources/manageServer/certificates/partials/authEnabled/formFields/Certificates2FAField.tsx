import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import { FormSwitch, FormValidationMessage } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { CertificateItem } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { QRCode } from "qrcodejs";
import { useRef, ElementRef, useState, useEffect } from "react";
import { flushSync } from "react-dom";
import { useAsync } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function Certificates2FAField({ editingCert }: { editingCert?: CertificateItem }) {
    const t = useStudioTranslation("certificates");
    const { manageServerService } = useServices();

    const { control, setValue, formState } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesEditFormData
    >();

    const formValues = useWatch({ control });

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const authenticationKeyInputRef = useRef<HTMLInputElement>(null);
    const generateNewKeyButtonRef = useRef<HTMLButtonElement>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    const hasCurrentKey = editingCert?.HasTwoFactor ?? false;
    const isRegenerate2FAKey = "isRegenerate2FAKey" in formValues && formValues.isRegenerate2FAKey;
    const isNewKeyNeeded = certificatesUtils.isNew2FAKeyNeeded(formValues, hasCurrentKey);
    const isCurrentKeyRemoved = certificatesUtils.is2FARemoved(formValues, hasCurrentKey);
    const authenticationKeyError = formState.errors.authenticationKey;

    const pendingChange = (() => {
        if (isCurrentKeyRemoved) {
            return t("twoFactor.pending", { context: "disabled" });
        }
        if (hasCurrentKey && isRegenerate2FAKey) {
            return t("twoFactor.pending", { context: "newKey" });
        }
        if (editingCert && !hasCurrentKey && formValues.isRequire2FA) {
            return t("twoFactor.pending", { context: "enabled" });
        }
        return null;
    })();

    useAsync(
        async () => {
            if (!isNewKeyNeeded) {
                return null;
            }
            const result = await manageServerService.generateTwoFactorSecret();
            return result.Secret;
        },
        [isNewKeyNeeded],
        {
            onSuccess: (authenticationKey, { isCurrent }) => {
                if (isCurrent()) {
                    setValue("authenticationKey", authenticationKey, { shouldValidate: true });
                }
            },
        }
    );

    const toggleRegenerate2FAKey = (isRegenerate: boolean) => {
        flushSync(() => setValue("isRegenerate2FAKey", isRegenerate));
        (isRegenerate ? authenticationKeyInputRef : generateNewKeyButtonRef).current?.focus();
    };

    const resetRegenerate2FAKey = () => {
        if (hasCurrentKey) {
            setValue("isRegenerate2FAKey", false);
        }
    };

    const certName = "name" in formValues ? formValues.name : editingCert?.Name;

    useEffect(() => {
        const generateQrCode = async () => {
            if (!formValues.authenticationKey) {
                qrCode?.clear();
                return;
            }

            const encodedIssuer = encodeURIComponent(location.hostname);
            const encodedName = encodeURIComponent(certName ?? "Client Certificate");

            const uri = `otpauth://totp/${encodedIssuer}:${encodedName}?secret=${formValues.authenticationKey}&issuer=${encodedIssuer}`;

            if (!qrCode) {
                setQrCode(
                    new QRCode(qrContainerRef.current, {
                        text: uri,
                        width: 128,
                        height: 128,
                        colorDark: "#000000",
                        colorLight: "#ffffff",
                        correctLevel: QRCode.CorrectLevel.Q,
                    })
                );
            } else {
                qrCode.clear();
                qrCode.makeCode(uri);
            }
        };

        generateQrCode();

        return () => {
            qrCode?.clear();
        };
    }, [formValues.authenticationKey, certName, qrCode]);

    return (
        <div
            className={classNames("well rounded hstack gap-3 align-items-start p-3 mb-3", {
                "border border-warning": isCurrentKeyRemoved,
            })}
        >
            <div
                className={classNames("rounded p-2 lh-1 fs-5", {
                    "bg-faded-2fa": formValues.isRequire2FA,
                    "bg-faded-warning": isCurrentKeyRemoved,
                    "bg-faded-secondary": !formValues.isRequire2FA && !isCurrentKeyRemoved,
                })}
            >
                <Icon icon="shield" margin="m-0" />
            </div>
            <div className="vstack gap-3 flex-grow-1 min-w-0">
                <div className="hstack gap-3 align-items-start">
                    <div className="flex-grow-1">
                        <div className="hstack gap-1">
                            <span className="fw-semibold text-emphasis">{t("twoFactor.title")}</span>
                            {pendingChange && (
                                <Badge bg="faded-warning" pill>
                                    {pendingChange}
                                </Badge>
                            )}
                        </div>
                        <div className="small text-muted">{t("twoFactor.description")}</div>
                    </div>
                    <FormSwitch
                        control={control}
                        name="isRequire2FA"
                        aria-label={t("twoFactor.switchLabel")}
                        afterChange={resetRegenerate2FAKey}
                    />
                </div>
                {hasCurrentKey && formValues.isRequire2FA && !isRegenerate2FAKey && (
                    <>
                        <hr className="m-0" />
                        <div className="hstack gap-3">
                            <div className="flex-grow-1">
                                <div className="text-emphasis">{t("twoFactor.keyLabel")}</div>
                                <div className="small text-muted">{t("twoFactor.currentKeyInUse")}</div>
                            </div>
                            <Button
                                ref={generateNewKeyButtonRef}
                                variant="secondary"
                                onClick={() => toggleRegenerate2FAKey(true)}
                            >
                                <Icon icon="refresh" />
                                {t("twoFactor.generateNewKey")}
                            </Button>
                        </div>
                    </>
                )}
                <Collapse in={isNewKeyNeeded}>
                    <div>
                        <hr className="mt-0 mb-3" />
                        <div className="hstack gap-3 align-items-start">
                            <div ref={qrContainerRef} className="qrcode rounded-2 overflow-hidden flex-shrink-0" />
                            <div className="vstack gap-2 min-w-0">
                                <div>
                                    <div className="text-emphasis">{t("twoFactor.scan")}</div>
                                    <div className="small text-muted">{t("twoFactor.enterManually")}</div>
                                </div>
                                <InputGroup>
                                    <Form.Control
                                        ref={authenticationKeyInputRef}
                                        readOnly
                                        value={formValues.authenticationKey ?? ""}
                                        placeholder={t("twoFactor.generatingKey")}
                                        aria-label={t("twoFactor.keyLabel")}
                                        className="font-monospace"
                                        isInvalid={!!authenticationKeyError}
                                    />
                                    <Button
                                        onClick={() =>
                                            copyToClipboard.copy(formValues.authenticationKey, t("twoFactor.keyCopied"))
                                        }
                                        variant="secondary"
                                        title={t("twoFactor.copyKeyTitle")}
                                    >
                                        <Icon icon="copy" margin="m-0" />
                                    </Button>
                                </InputGroup>
                                {authenticationKeyError && (
                                    <FormValidationMessage>{authenticationKeyError.message}</FormValidationMessage>
                                )}
                                {hasCurrentKey && (
                                    <div className="hstack gap-1 flex-wrap">
                                        <span className="text-warning">
                                            <Icon icon="warning" />
                                            {t("twoFactor.currentKeyStops")}
                                        </span>
                                        <Button
                                            variant="link"
                                            size="sm"
                                            className="p-0"
                                            onClick={() => toggleRegenerate2FAKey(false)}
                                        >
                                            {t("twoFactor.keepCurrentKey")}
                                        </Button>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </Collapse>
            </div>
        </div>
    );
}
