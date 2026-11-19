import React, { PropsWithChildren } from "react";
import { Icon } from "./Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function LicenseRestrictedMessage({ children }: PropsWithChildren) {
    const t = useStudioTranslation("commonComponents");
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <>
            {children}
            <div className="mt-2">
                <a href={buyLink} target="_blank" className="btn btn-primary btn-sm rounded-pill">
                    {t("licenseRestrictedMessage.licensingOptions")} <Icon icon="newtab" margin="ms-1" />
                </a>
            </div>
        </>
    );
}
