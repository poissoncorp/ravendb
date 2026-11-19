import React from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import IntegrationsUserList from "components/pages/database/settings/integrations/IntegrationsUserList";
import { useIntegrations } from "components/pages/database/settings/integrations/useIntegrations";
import IntegrationsAlerts from "components/pages/database/settings/integrations/IntegrationsAlerts";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import IntegrationsAddNewButton from "components/pages/database/settings/integrations/IntegrationsAddNewButton";
import { IntegrationsInfoHub } from "components/pages/database/settings/integrations/IntegrationsInfoHub";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function Integrations() {
    const t = useStudioTranslation("integrations");
    const {
        isSharded,
        isLicenseUpgradeRequired,
        isPostgreSqlSupportEnabled,
        asyncGetPostgreSqlUsers,
        users,
        addNewUser,
        removeUser,
    } = useIntegrations();

    if (isSharded) {
        return (
            <FeatureNotAvailable>
                <span>
                    <StudioTrans
                        ns="integrations"
                        i18nKey="notAvailableSharded"
                        components={{ icon: <Icon icon="sharding" color="shard" margin="m-0" /> }}
                    />
                </span>
            </FeatureNotAvailable>
        );
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title={t("title")} icon="integrations" />
                    <div className="mb-3">
                        <HrHeader
                            right={
                                <IntegrationsAddNewButton
                                    isLicenseUpgradeRequired={isLicenseUpgradeRequired}
                                    addNewUser={addNewUser}
                                />
                            }
                        >
                            {t("postgreSqlHeading")}
                        </HrHeader>
                        <IntegrationsUserList
                            fetchState={asyncGetPostgreSqlUsers.status}
                            reload={asyncGetPostgreSqlUsers.execute}
                            users={users}
                            removeUser={removeUser}
                        />
                    </div>
                    <IntegrationsAlerts
                        isLicenseUpgradeRequired={isLicenseUpgradeRequired}
                        isPostgreSqlSupportEnabled={isPostgreSqlSupportEnabled}
                    />
                </Col>
                <Col sm={12} lg={4}>
                    <IntegrationsInfoHub />
                </Col>
            </Row>
        </div>
    );
}
