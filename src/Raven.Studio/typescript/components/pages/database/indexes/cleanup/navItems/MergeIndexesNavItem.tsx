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

const mergeIndexesImg = require("Content/img/pages/indexCleanup/merge-indexes.svg");

interface MergeIndexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    mergable: UseIndexCleanupResult["mergable"];
}

export default function MergeIndexesNavItem({ carousel, mergable }: MergeIndexesNavItemProps) {
    const t = useStudioTranslation("indexCleanup");
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Nav.Item>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 0 })}
                onClick={() => carousel.setActiveTab(0)}
            >
                <img src={mergeIndexesImg} alt={t("nav.mergeIndexesAlt")} />
                <Badge className="rounded-pill fs-5" bg={mergable.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? mergable.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    <StudioTrans ns="indexCleanup" i18nKey="nav.mergeIndexes" components={{ br: <br /> }} />
                </h4>
            </Card>
        </Nav.Item>
    );
}
