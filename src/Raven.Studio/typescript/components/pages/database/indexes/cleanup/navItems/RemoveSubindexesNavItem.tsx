import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import Badge from "react-bootstrap/Badge";
import Card from "react-bootstrap/Card";
import Nav from "react-bootstrap/Nav";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const removeSubindexesImg = require("Content/img/pages/indexCleanup/remove-subindexes.svg");

interface RemoveSubindexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    surpassing: UseIndexCleanupResult["surpassing"];
}

export default function RemoveSubindexesNavItem({ carousel, surpassing }: RemoveSubindexesNavItemProps) {
    const t = useStudioTranslation("indexCleanup");
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Nav.Item>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 1 })}
                onClick={() => carousel.setActiveTab(1)}
            >
                <img src={removeSubindexesImg} alt={t("nav.removeSubindexesAlt")} />
                <Badge className="rounded-pill fs-5" bg={surpassing.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? surpassing.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    <StudioTrans ns="indexCleanup" i18nKey="nav.removeSubindexes" components={{ br: <br /> }} />
                </h4>
            </Card>
        </Nav.Item>
    );
}
