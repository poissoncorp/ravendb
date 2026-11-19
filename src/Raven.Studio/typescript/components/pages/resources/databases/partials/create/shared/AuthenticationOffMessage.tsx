import { Icon } from "components/common/Icon";
import { useAppUrls } from "components/hooks/useAppUrls";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AuthenticationOffMessage() {
    const { appUrl } = useAppUrls();
    const t = useStudioTranslation("databases");

    return (
        <>
            <p className="lead text-warning">
                <Icon icon="unsecure" margin="m-0" /> {t("createDatabase.authenticationOff.title")}
            </p>
            <p>
                <StudioTrans
                    ns="databases"
                    i18nKey="createDatabase.authenticationOff.encryptionRequirement"
                    components={{ strong: <strong /> }}
                />
            </p>
            <p>
                <StudioTrans
                    ns="databases"
                    i18nKey="createDatabase.authenticationOff.moreInfo"
                    components={{ a: <a href={appUrl.forCertificates()} /> }}
                />
            </p>
        </>
    );
}
