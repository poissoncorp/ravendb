import React, { memo } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import appUrl from "common/appUrl";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface DatabaseIndexPerformanceLinkProps {
    packageId: string;
    database: string;
    node: string;
}

// Opens the selected node's indexing performance timeline from the package in the live Indexing Performance viewer
// (which auto-imports the package data). That viewer is database-scoped, so it can only open when the database
// exists on this server; otherwise the link is disabled with an explanation.
export default memo(function DatabaseIndexPerformanceLink({
    packageId,
    database,
    node,
}: DatabaseIndexPerformanceLinkProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const databaseExistsLocally = useAppSelector(databaseSelectors.allDatabases).some((db) => db.name === database);

    const href = appUrl.forIndexPerformance(database, null, packageId, node);

    return (
        <div className="panel-bg-1 rounded">
            <div className="p-4 hstack gap-3 flex-wrap align-items-center">
                <div className="vstack">
                    <h3 className="m-0">{t("indexingPerformance.title")}</h3>
                    <span className="text-muted">{t("indexingPerformance.description")}</span>
                </div>
                <div className="hstack gap-2 ms-auto align-items-center">
                    {databaseExistsLocally ? (
                        <Button href={href} target="_blank">
                            <Icon icon="indexing-performance" /> {t("indexingPerformance.open")}
                        </Button>
                    ) : (
                        <span title={t("indexingPerformance.databaseMissing", { database })}>
                            <Button disabled>
                                <Icon icon="indexing-performance" /> {t("indexingPerformance.open")}
                            </Button>
                        </span>
                    )}
                </div>
            </div>
        </div>
    );
});
