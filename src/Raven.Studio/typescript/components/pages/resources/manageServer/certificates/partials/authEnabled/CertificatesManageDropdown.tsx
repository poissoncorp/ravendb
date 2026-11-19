import { Icon } from "components/common/Icon";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef } from "react";
import Dropdown from "react-bootstrap/Dropdown";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import endpoints from "endpoints";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function CertificatesManageDropdown() {
    const t = useStudioTranslation("certificates");
    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();
    const exportServerCertFormRef = useRef<HTMLFormElement>(null);
    const hasClusterNodeCertificate = useAppSelector(certificatesSelectors.hasClusterNodeCertificate);

    return (
        <>
            <Dropdown>
                <Dropdown.Toggle title={t("manage.toggle")} variant="primary" className="rounded-pill">
                    {t("manage.toggle")}
                </Dropdown.Toggle>
                <Dropdown.Menu>
                    <Dropdown.Header className="small-label">{t("managementType.sso")}</Dropdown.Header>
                    <Dropdown.Item onClick={() => dispatch(certificatesActions.isRegisterSsoServerModalOpenToggled())}>
                        <Icon icon="certificate" addon="upload" />
                        {t("manage.registerSsoServer")}
                    </Dropdown.Item>
                    <Dropdown.Item onClick={() => dispatch(certificatesActions.isRegisterSsoUserModalOpenToggled())}>
                        <Icon icon="user" addon="plus" />
                        {t("manage.addSsoUser")}
                    </Dropdown.Item>
                    <Dropdown.Divider />
                    <Dropdown.Header className="small-label">{t("managementType.server")}</Dropdown.Header>
                    <ConditionalPopover
                        conditions={[
                            {
                                isActive: !hasClusterNodeCertificate,
                                message: t("manage.exportNoServerCert"),
                            },
                            {
                                isActive: true,
                                message: <span>{t("manage.exportInfo")}</span>,
                            },
                        ]}
                        popoverPlacement="right"
                    >
                        <Dropdown.Item
                            onClick={() => {
                                reportEvent("certificates", "export-certs");
                                exportServerCertFormRef.current?.submit();
                            }}
                            disabled={!hasClusterNodeCertificate}
                        >
                            <Icon icon="download" />
                            {t("manage.exportServerCerts")}
                        </Dropdown.Item>
                    </ConditionalPopover>
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasClusterNodeCertificate,
                            message: t("manage.replaceNoServerCert"),
                        }}
                    >
                        <Dropdown.Item
                            onClick={() => dispatch(certificatesActions.isReplaceServerModalOpenToggled())}
                            disabled={!hasClusterNodeCertificate}
                        >
                            <Icon icon="refresh" />
                            {t("manage.replaceServerCerts")}
                        </Dropdown.Item>
                    </ConditionalPopover>
                    <Dropdown.Divider />
                    <Dropdown.Header className="small-label">{t("managementType.client")}</Dropdown.Header>
                    <Dropdown.Item onClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())}>
                        <Icon icon="certificate" addon="plus" />
                        {t("manage.generateClientCert")}
                    </Dropdown.Item>
                    <Dropdown.Item onClick={() => dispatch(certificatesActions.isUploadModalOpenToggled())}>
                        <Icon icon="upload" />
                        {t("manage.uploadClientCert")}
                    </Dropdown.Item>
                </Dropdown.Menu>
            </Dropdown>
            <form
                ref={exportServerCertFormRef}
                action={endpoints.global.adminCertificates.adminCertificatesExport}
                className="d-none"
            />
        </>
    );
}
