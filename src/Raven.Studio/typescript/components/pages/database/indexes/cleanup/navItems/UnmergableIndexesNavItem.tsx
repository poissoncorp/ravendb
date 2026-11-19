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

const unmergableIndexesImg = require("Content/img/pages/indexCleanup/unmergable-indexes.svg");

interface UnmergableIndexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    unmergable: UseIndexCleanupResult["unmergable"];
}

export default function UnmergableIndexesNavItem({ carousel, unmergable }: UnmergableIndexesNavItemProps) {
    const t = useStudioTranslation("indexCleanup");
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Nav.Item>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 3 })}
                onClick={() => carousel.setActiveTab(3)}
            >
                <img src={unmergableIndexesImg} alt={t("nav.unmergeableAlt")} />
                <Badge className="rounded-pill fs-5" bg={unmergable.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? unmergable.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    <StudioTrans ns="indexCleanup" i18nKey="nav.unmergeable" components={{ br: <br /> }} />
                </h4>
            </Card>
        </Nav.Item>
    );
}
