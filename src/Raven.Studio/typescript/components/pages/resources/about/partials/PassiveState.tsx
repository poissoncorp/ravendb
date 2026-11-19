import Card from "react-bootstrap/Card";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import appUrl from "common/appUrl";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import registration from "viewmodels/shell/registration";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { withPreventDefault } from "components/utils/common";
import CreateDatabase, {
    CreateDatabaseMode,
} from "components/pages/resources/databases/partials/create/CreateDatabase";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function PassiveState() {
    const t = useStudioTranslation("aboutPage");
    const isPassive = useAppSelector(clusterSelectors.isPassive);
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const licenseRegistered = useAppSelector(licenseSelectors.licenseRegistered);
    const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);

    const registerLicense = () => registration.showRegistrationDialog(licenseStatus, false, true);
    const newDatabase = () => {
        setCreateDatabaseMode("regular");
    };

    if (!isPassive) {
        return null;
    }

    return (
        <Card color="faded-primary">
            <Card.Body className="text-body">
                <h3>
                    <Icon icon="info" />
                    <StudioTrans
                        ns="aboutPage"
                        i18nKey="passiveState.heading"
                        components={{ strong: <span className="fw-bolder" /> }}
                    />
                </h3>
                <p>{t("passiveState.licenseInfo")}</p>
                <p>{t("passiveState.bootstrapIntro")}</p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="aboutPage"
                            i18nKey="passiveState.createDatabase"
                            components={{ a: <a href="#" onClick={withPreventDefault(newDatabase)} /> }}
                        />
                    </li>
                    {!licenseRegistered && (
                        <li>
                            <a href="#" onClick={withPreventDefault(registerLicense)}>
                                {t("passiveState.registerLicense")}
                            </a>
                        </li>
                    )}
                    <li>
                        <StudioTrans
                            ns="aboutPage"
                            i18nKey="passiveState.bootstrapCluster"
                            components={{ a: <a href={appUrl.forCluster()} />, br: <br /> }}
                        />
                    </li>
                </ul>
                {createDatabaseMode && (
                    <CreateDatabase closeModal={() => setCreateDatabaseMode(null)} initialMode={createDatabaseMode} />
                )}
            </Card.Body>
        </Card>
    );
}
