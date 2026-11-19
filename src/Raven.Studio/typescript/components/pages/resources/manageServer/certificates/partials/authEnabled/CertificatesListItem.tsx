import copyToClipboard from "common/copyToClipboard";
import genUtils from "common/generalUtils";
import { sortBy } from "common/typeUtils";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import useConfirm from "components/common/ConfirmDialog";
import { Icon } from "components/common/Icon";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
} from "components/common/RichPanel";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { ThemeColor } from "components/models/common";
import CertificatesItemStatus from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesItemStatus";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import {
    CertificateItem,
    UpdateCertificateDto,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useAppDispatch, useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import { useAsyncCallback } from "react-async-hook";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface CertificatesListItemProps {
    certificate: CertificateItem;
}

export default function CertificatesListItem({ certificate }: CertificatesListItemProps) {
    const t = useStudioTranslation("certificates");
    const { manageServerService } = useServices();
    const dispatch = useAppDispatch();
    const confirm = useConfirm();
    const { reportEvent } = useEventsCollector();

    const serverCertificateThumbprint = useAppSelector(certificatesSelectors.serverCertificateThumbprint);
    const serverCertificateForCommunicationThumbprint = useAppSelector(
        certificatesSelectors.serverCertificateForCommunicationThumbprint
    );
    const serverCertificateSetupMode = useAppSelector(certificatesSelectors.serverCertificateSetupMode);
    const serverCertificateRenewalDate = useAppSelector(certificatesSelectors.serverCertificateRenewalDate);
    const clientCertificateThumbprint = useAppSelector(accessManagerSelectors.clientCertificateThumbprint);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const ssoServerCertificates = useAppSelector(certificatesSelectors.ssoServerCertificates);
    const ssoUserCertificates = useAppSelector(certificatesSelectors.ssoUserCertificates);

    const state = certificatesUtils.getState(certificate);
    const clearance = certificatesUtils.getClearance(certificate.SecurityClearance);
    const isServerCert = certificate.Thumbprints.includes(serverCertificateThumbprint);
    const isServerCertForCommunication = certificate.Thumbprints.includes(serverCertificateForCommunicationThumbprint);
    const isCurrentBrowserCert = certificate.Thumbprints.includes(clientCertificateThumbprint);
    const has2fa = certificate.HasTwoFactor ?? false;
    const certDisplayName = certificate.Name ?? t("item.emptyName");

    const isDisabled = certificate.Disabled ?? false;
    const toggleContext = isDisabled ? "enable" : "disable";
    const canBeAutomaticallyRenewed = isServerCert && serverCertificateSetupMode === "LetsEncrypt";
    const authorizedSsoUsers =
        certificate.Usage === "SsoServer"
            ? ssoUserCertificates.filter(
                  (u) =>
                      u.AllowAnySsoServer ||
                      (u.SsoServerPublicKeyPinningHashes ?? []).includes(certificate.PublicKeyPinningHash)
              )
            : [];
    const canEdit =
        !isServerCert && !isServerCertForCommunication && certificate.Usage !== "SsoServer" && state !== "Expired";
    const canClone = !isServerCert && !isServerCertForCommunication && certificate.Usage !== "SsoServer";
    const canToggleDisable =
        !isServerCert &&
        !isServerCertForCommunication &&
        !isCurrentBrowserCert &&
        certificate.SecurityClearance !== "ClusterNode" &&
        (isClusterAdminOrClusterNode || clearance !== "Admin");
    const canDelete = (() => {
        if (isServerCert || isServerCertForCommunication) {
            return false;
        }
        if (!isClusterAdminOrClusterNode && clearance === "Admin") {
            return false;
        }
        return true;
    })();

    const asyncRenewServerCertificate = useAsyncCallback(async () => {
        reportEvent("certificates", "renew");
        await manageServerService.forceRenewServerCertificate();
        await dispatch(certificatesActions.fetchData());
    });

    const handleRenewServerCertificate = async () => {
        const isConfirmed = await confirm({
            icon: "refresh",
            title: t("item.renewConfirmTitle"),
            actionColor: "primary",
            confirmText: t("item.renewConfirm"),
        });

        if (isConfirmed) {
            asyncRenewServerCertificate.execute();
        }
    };

    const asyncToggleDisable = useAsyncCallback(async () => {
        const newDisabledState = !isDisabled;
        reportEvent("certificates", newDisabledState ? "disable" : "enable");

        const dto: UpdateCertificateDto = {
            Name: certificate.Name,
            Thumbprint: certificate.Thumbprint,
            SecurityClearance: certificate.SecurityClearance,
            Permissions: certificate.Permissions,
            TwoFactorAuthenticationKey: null,
            Disabled: newDisabledState,
        };

        await manageServerService.updateCertificate(dto, false);
        await dispatch(certificatesActions.fetchData());
    });

    const handleToggleDisable = async () => {
        const isConfirmed = await confirm({
            icon: isDisabled ? "play" : "stop",
            title: t("item.toggleConfirmTitle", { context: toggleContext }),
            message: (
                <span>
                    <StudioTrans
                        ns="certificates"
                        i18nKey="item.toggleConfirmMessage"
                        components={{ strong: <strong />, br: <br />, code: <code /> }}
                        options={{ name: certificate.Name, thumbprint: certificate.Thumbprint }}
                    />
                </span>
            ),
            actionColor: isDisabled ? "success" : "warning",
            confirmText: t("item.toggle", { context: toggleContext }),
        });

        if (!isConfirmed) {
            return;
        }

        asyncToggleDisable.execute();
    };

    const asyncDeleteCertificate = useAsyncCallback(async () => {
        reportEvent("certificates", "delete");
        await manageServerService.deleteCertificate(certificate.Thumbprint);
        await dispatch(certificatesActions.fetchData());
    });

    const handleDeleteCertificate = async () => {
        const isConfirmed = await confirm({
            icon: "trash",
            title: t("item.deleteConfirmTitle"),
            message: (
                <span>
                    <StudioTrans
                        ns="certificates"
                        i18nKey="item.deleteConfirmMessage"
                        components={{ code: <code /> }}
                        options={{ thumbprint: certificate.Thumbprint }}
                    />
                </span>
            ),
            actionColor: "danger",
            confirmText: t("item.delete"),
        });

        if (isConfirmed) {
            asyncDeleteCertificate.execute();
        }
    };

    return (
        <RichPanel className="flex-row with-status" hover>
            <CertificatesItemStatus state={state} />
            <div className="flex-grow">
                <RichPanelHeader>
                    <div className="flex-grow">
                        <div className="d-flex align-items-center justify-content-start flex-wrap w-100">
                            <span className="fs-4 text-truncate" title={certDisplayName} style={{ maxWidth: "400px" }}>
                                {certDisplayName}
                            </span>
                            {state === "About to expire" && (
                                <Badge bg="warning" className="ms-1 fs-6" pill title={t("item.aboutToExpireTitle")}>
                                    <Icon icon="clock" margin="m-0" /> {t("state.aboutToExpire")}
                                </Badge>
                            )}
                            {isCurrentBrowserCert && (
                                <Badge bg="success" className="ms-1 fs-6" pill title={t("item.currentBrowserTitle")}>
                                    {t("item.currentBrowser")}
                                </Badge>
                            )}
                            {has2fa && (
                                <Badge bg="2fa" className="ms-1 fs-6" pill title={t("item.twoFactorTitle")}>
                                    {t("item.twoFactor")}
                                </Badge>
                            )}
                            {certificate.Usage === "SsoServer" && (
                                <Badge bg="info" className="ms-1 fs-6" pill title={t("item.ssoServerTitle")}>
                                    {t("item.ssoServer")}
                                </Badge>
                            )}
                            {certificate.Usage === "SsoClient" && (
                                <Badge bg="primary" className="ms-1 fs-6" pill>
                                    {t("item.ssoUser")}
                                </Badge>
                            )}
                        </div>
                        {certificate.Thumbprints.join(", ")}
                        <Button
                            variant="link"
                            size="xs"
                            onClick={() =>
                                copyToClipboard.copy(certificate.Thumbprints.join(", "), t("item.thumbprintsCopied"))
                            }
                        >
                            <Icon icon="copy" margin="m-0" />
                        </Button>
                    </div>
                    <RichPanelActions>
                        {canClone && (
                            <Button
                                title={t("item.clone")}
                                onClick={() =>
                                    certificate.Usage === "SsoClient"
                                        ? dispatch(certificatesActions.ssoUserCloneModalOpen(certificate))
                                        : dispatch(certificatesActions.cloneModalOpen(certificate))
                                }
                                variant="secondary"
                            >
                                <Icon icon="copy" margin="m-0" />
                            </Button>
                        )}
                        {canEdit && (
                            <Button
                                title={t("item.edit")}
                                onClick={() =>
                                    certificate.Usage === "SsoClient"
                                        ? dispatch(certificatesActions.ssoUserEditModalOpen(certificate))
                                        : dispatch(certificatesActions.editModalOpen(certificate))
                                }
                                variant="secondary"
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                        )}
                        {canToggleDisable && (
                            <ButtonWithSpinner
                                title={t("item.toggle", { context: toggleContext })}
                                variant={isDisabled ? "success" : "warning"}
                                onClick={handleToggleDisable}
                                icon={isDisabled ? "play" : "stop"}
                                isSpinning={asyncToggleDisable.loading}
                            />
                        )}
                        {canDelete && (
                            <ButtonWithSpinner
                                title={t("item.delete")}
                                variant="danger"
                                onClick={handleDeleteCertificate}
                                icon="trash"
                                isSpinning={asyncDeleteCertificate.loading}
                            />
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    {certificate.Usage !== "SsoServer" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="user" />
                                    {t("item.securityClearance")}
                                </>
                            }
                        >
                            {getFormattedSecurityClearance(certificate.SecurityClearance, t)}
                        </RichPanelDetailItem>
                    )}
                    {certificate.NotBefore && certificate.Usage !== "SsoClient" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="check" />
                                    {t("item.validFrom")}
                                </>
                            }
                        >
                            {moment.utc(certificate.NotBefore).format(genUtils.basicDateFormat)}
                        </RichPanelDetailItem>
                    )}
                    {certificate.NotAfter && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="expiration" />
                                    {t("item.expiration", { context: state === "Expired" ? "expired" : undefined })}
                                </>
                            }
                        >
                            <span className={`text-${certificatesUtils.getStateDateColor(state)}`}>
                                {moment.utc(certificate.NotAfter).format(genUtils.basicDateFormat)}
                            </span>
                        </RichPanelDetailItem>
                    )}
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="user-info" />
                                {t("item.lastUsed")}
                            </>
                        }
                    >
                        {certificate.LastUsedDate
                            ? moment.utc(certificate.LastUsedDate).format(genUtils.basicDateFormat)
                            : t("item.notUsed")}
                    </RichPanelDetailItem>
                    {certificate.Usage === "SsoServer" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="user" />
                                    {t("item.entriesAuthorized")}
                                </>
                            }
                        >
                            {authorizedSsoUsers.length}
                            {authorizedSsoUsers.length > 0 && (
                                <PopoverWithHoverWrapper
                                    message={
                                        <ul className="mb-0 ps-3">
                                            {authorizedSsoUsers.map((u) => (
                                                <li key={u.Thumbprint}>{u.Name ?? u.Thumbprint}</li>
                                            ))}
                                        </ul>
                                    }
                                >
                                    <Icon icon="info" color="info" margin="ms-1" className="small" />
                                </PopoverWithHoverWrapper>
                            )}
                        </RichPanelDetailItem>
                    )}
                    {certificate.Usage === "SsoClient" && certificate.SsoIdentifiers?.length > 0 && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="user" />
                                    {t("item.identifiers")}
                                </>
                            }
                        >
                            {certificate.SsoIdentifiers.map((id) =>
                                id.Provider === "Windows" && id.Domain
                                    ? `Windows\\${id.Domain}: ${id.Identifier}`
                                    : `${id.Provider}: ${id.Identifier}`
                            ).join(" · ")}
                        </RichPanelDetailItem>
                    )}
                    {certificate.Usage === "SsoClient" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="key" />
                                    {t("item.authorizingSso")}
                                </>
                            }
                        >
                            {certificate.AllowAnySsoServer
                                ? t("item.allowAnySso")
                                : ssoServerCertificates
                                      .filter((s) =>
                                          (certificate.SsoServerPublicKeyPinningHashes ?? []).includes(
                                              s.PublicKeyPinningHash
                                          )
                                      )
                                      .map((s) => s.Name)
                                      .join(", ") || t("item.none")}
                        </RichPanelDetailItem>
                    )}
                    {canBeAutomaticallyRenewed && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="refresh" />
                                    {t("item.autoRenewalDate")}
                                </>
                            }
                        >
                            {moment.utc(serverCertificateRenewalDate).format(genUtils.basicDateFormat)}
                            <ButtonWithSpinner
                                variant="link"
                                size="xs"
                                title={t("item.renewTitle")}
                                onClick={handleRenewServerCertificate}
                                icon="refresh"
                                isSpinning={asyncRenewServerCertificate.loading}
                            >
                                {t("item.renewNow")}
                            </ButtonWithSpinner>
                        </RichPanelDetailItem>
                    )}
                    {certificate.Usage !== "SsoServer" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="database" />
                                    {t("item.databasePermissions")}
                                </>
                            }
                        >
                            <PermissionsBadge certificate={certificate} />
                        </RichPanelDetailItem>
                    )}
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}

function PermissionsBadge({ certificate }: { certificate: CertificateItem }) {
    const t = useStudioTranslation("certificates");
    const { SecurityClearance, Permissions } = certificate;

    if (
        SecurityClearance === "ClusterNode" ||
        SecurityClearance === "ClusterAdmin" ||
        SecurityClearance === "Operator"
    ) {
        return (
            <Badge bg="faded-success" title={t("item.clusterAdminAccess")} pill>
                <Icon icon="cluster" />
                {t("item.allDatabases")}
            </Badge>
        );
    }

    const dbAccessArray = sortBy(
        Object.entries(Permissions ?? []).map(([databaseName, accessLevel]) => ({
            databaseName,
            accessLevel,
        })),
        (x) => x.databaseName.toLowerCase()
    );

    if (dbAccessArray.length === 0) {
        return <span className="text-danger">{t("item.none")}</span>;
    }

    return (
        <div className="d-flex flex-wrap gap-1">
            {dbAccessArray.map(({ databaseName, accessLevel }) => (
                <Badge key={databaseName} bg={getAccessColor(accessLevel)} title={getAccessTitle(accessLevel, t)} pill>
                    <Icon icon={getAccessIcon(accessLevel)} />
                    {databaseName}
                </Badge>
            ))}
        </div>
    );
}

function getAccessTitle(
    access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess,
    t: StudioTranslate<"certificates">
) {
    switch (access) {
        case "Admin":
            return t("item.accessTitle", { context: "admin" });
        case "Read":
            return t("item.accessTitle", { context: "read" });
        case "ReadWrite":
            return t("item.accessTitle", { context: "readWrite" });
        default:
            assertUnreachable(access);
    }
}

function getAccessIcon(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess): IconName {
    switch (access) {
        case "Admin":
            return "hammer-driver";
        case "Read":
            return "access-read";
        case "ReadWrite":
            return "access-read-write";
        default:
            assertUnreachable(access);
    }
}

function getAccessColor(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess): `faded-${ThemeColor}` {
    switch (access) {
        case "Admin":
            return "faded-danger";
        case "Read":
            return "faded-info";
        case "ReadWrite":
            return "faded-warning";
        default:
            assertUnreachable(access);
    }
}

function getFormattedSecurityClearance(
    securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance,
    t: StudioTranslate<"certificates">
): string {
    switch (securityClearance) {
        case "ClusterAdmin":
            return t("clearance.clusterAdmin");
        case "ClusterNode":
            return t("clearance.clusterNode");
        case "Operator":
            return t("clearance.operator");
        case "ValidUser":
            return t("clearance.user");
        default:
            return t("clearance.unknown");
    }
}
