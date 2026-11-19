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

const removeUnusedImg = require("Content/img/pages/indexCleanup/remove-unused.svg");

interface RemoveUnusedIndexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    unused: UseIndexCleanupResult["unused"];
}

export default function RemoveUnusedIndexesNavItem({ carousel, unused }: RemoveUnusedIndexesNavItemProps) {
    const t = useStudioTranslation("indexCleanup");
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Nav.Item>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 2 })}
                onClick={() => carousel.setActiveTab(2)}
            >
                <img src={removeUnusedImg} alt={t("nav.removeUnusedAlt")} />
                <Badge className="rounded-pill fs-5" bg={unused.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? unused.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    <StudioTrans ns="indexCleanup" i18nKey="nav.removeUnused" components={{ br: <br /> }} />
                </h4>
            </Card>
        </Nav.Item>
    );
}
