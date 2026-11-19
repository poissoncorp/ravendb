import { useRavenLink } from "hooks/useRavenLink";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function FeatureNotAvailableInYourLicensePopoverBody() {
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <div className="text-center">
            <StudioTrans
                ns="commonComponents"
                i18nKey="featureNotAvailable.inYourLicense"
                components={{ br: <br />, a: <a href={upgradeLicenseLink} target="_blank" /> }}
            />
        </div>
    );
}
