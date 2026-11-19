import React from "react";
import { useRavenLink } from "components/hooks/useRavenLink";
import { Icon } from "components/common/Icon";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface PopoverMessageProps {
    description: string | React.ReactNode;
    alert?: React.ReactNode;
    ravenLinkHash?: string;
}

export function SetupWizardInfoPopover({ description, ravenLinkHash = "37GM2Z", alert }: PopoverMessageProps) {
    const docsLink = useRavenLink({ hash: ravenLinkHash });

    return (
        <>
            <p className="mb-0">{description}</p>
            {alert}
            <hr className="my-2" />
            <span className="md-label">
                <Icon icon="link" />
                <StudioTrans
                    ns="setupWizard"
                    i18nKey="infoPopover.readMore"
                    components={{
                        a: <a href={docsLink} target="_blank" className="text-primary fw-bold" />,
                        icon: <Icon icon="newtab" />,
                    }}
                />
            </span>
        </>
    );
}
