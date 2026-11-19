import { EmptySet } from "components/common/EmptySet";
import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import CertificatesListItem from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesListItem";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function CertificatesServerList() {
    const t = useStudioTranslation("certificates");
    const serverCertificateThumbprint = useAppSelector(certificatesSelectors.serverCertificateThumbprint);
    const serverCertificateForCommunicationThumbprint = useAppSelector(
        certificatesSelectors.serverCertificateForCommunicationThumbprint
    );
    const hasActiveFilter = useAppSelector(certificatesSelectors.hasActiveFilter);

    const filteredCertificates = useAppSelector(certificatesSelectors.filteredCertificates).filter(
        (cert) =>
            cert.Thumbprints.includes(serverCertificateThumbprint) ||
            cert.Thumbprints.includes(serverCertificateForCommunicationThumbprint)
    );

    if (hasActiveFilter && !filteredCertificates.length) {
        return null;
    }

    return (
        <div>
            <HrHeader>
                <Icon icon="server" />
                {t("managementType.server")}
            </HrHeader>
            {!filteredCertificates.length && <EmptySet compact>{t("noCertificates")}</EmptySet>}
            {filteredCertificates.map((cert) => (
                <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
            ))}
        </div>
    );
}
