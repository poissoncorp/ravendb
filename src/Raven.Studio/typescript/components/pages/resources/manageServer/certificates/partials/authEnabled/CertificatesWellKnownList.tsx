import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelDetails,
    RichPanelDetailItem,
    RichPanelStatus,
    RichPanelNameMultiLine,
} from "components/common/RichPanel";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppSelector } from "components/store";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function CertificatesWellKnownList() {
    const t = useStudioTranslation("certificates");
    const wellKnownAdminCerts = useAppSelector(certificatesSelectors.wellKnownAdminCerts);
    const wellKnownIssuers = useAppSelector(certificatesSelectors.wellKnownIssuers);

    return (
        <div className="vstack mt-3">
            <WellKnownItem title={t("wellKnown.adminTitle")} wellKnownThumbprints={wellKnownAdminCerts} />
            <WellKnownItem title={t("wellKnown.issuerTitle")} wellKnownThumbprints={wellKnownIssuers} />
        </div>
    );
}

interface WellKnownItemProps {
    wellKnownThumbprints: string[];
    title: string;
}

function WellKnownItem({ title, wellKnownThumbprints }: WellKnownItemProps) {
    const t = useStudioTranslation("certificates");
    const nameOrThumbprintFilter = useAppSelector(certificatesSelectors.nameOrThumbprintFilter);
    const clearanceFilter = useAppSelector(certificatesSelectors.clearanceFilter);

    if (wellKnownThumbprints.length === 0) {
        return null;
    }

    if (
        wellKnownThumbprints.some(
            (thumbprint) => !thumbprint.toLowerCase().includes(nameOrThumbprintFilter.toLowerCase())
        )
    ) {
        return null;
    }

    if (clearanceFilter.length > 0 && !clearanceFilter.includes("Admin")) {
        return null;
    }

    return (
        <RichPanel className="flex-row with-status" hover>
            <RichPanelStatus color="info">{t("wellKnown.status")}</RichPanelStatus>
            <div className="flex-grow">
                <RichPanelHeader>
                    <div>
                        <RichPanelNameMultiLine className="d-flex align-items-center w-100">
                            {title}
                        </RichPanelNameMultiLine>
                        {wellKnownThumbprints.join(", ")}
                        <Button
                            variant="link"
                            size="xs"
                            onClick={() =>
                                copyToClipboard.copy(wellKnownThumbprints.join(", "), t("item.thumbprintsCopied"))
                            }
                        >
                            <Icon icon="copy" margin="m-0" />
                        </Button>
                    </div>
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="user" />
                                {t("item.securityClearance")}
                            </>
                        }
                    >
                        {t("clearance.clusterAdmin")}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="database" />
                                {t("item.databasePermissions")}
                            </>
                        }
                    >
                        <Badge bg="faded-success" pill>
                            <Icon icon="user" />
                            {t("item.allDatabases")}
                        </Badge>
                    </RichPanelDetailItem>
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}
