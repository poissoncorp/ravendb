import appUrl from "common/appUrl";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface UnmergableIndexesCardProps {
    unmergable: UseIndexCleanupResult["unmergable"];
}

export default function UnmergableIndexesCard({ unmergable }: UnmergableIndexesCardProps) {
    const t = useStudioTranslation("indexCleanup");

    return (
        <Card>
            <Card className="bg-faded-primary m-1 p-4">
                <div className="text-limit-width">
                    <h2>{t("unmergeable.title")}</h2>
                    {t("unmergeable.description")}
                </div>
            </Card>

            {unmergable.data.length === 0 ? (
                <EmptySet>{t("unmergeable.empty")}</EmptySet>
            ) : (
                <MainPanel unmergable={unmergable} />
            )}
        </Card>
    );
}

function MainPanel({ unmergable }: UnmergableIndexesCardProps) {
    const t = useStudioTranslation("indexCleanup");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <div className="p-2">
            <RichPanel hover>
                <RichPanelHeader className="px-3 py-2 d-block">
                    <Table responsive className="m-0 table-inner-border">
                        <thead>
                            <tr>
                                <td>
                                    <div className="small-label">{t("columns.indexName")}</div>
                                </td>

                                <td>
                                    <div className="small-label">{t("unmergeable.reason")}</div>
                                </td>
                            </tr>
                        </thead>
                        <tbody>
                            {unmergable.data.map((index, indexKey) => (
                                <tr key={"unmergable-" + indexKey}>
                                    <td>
                                        <div>
                                            <a href={appUrl.forEditIndex(index.name, databaseName)}>
                                                {index.name}
                                                <Icon icon="newtab" margin="ms-1" />
                                            </a>
                                        </div>
                                    </td>
                                    <td>
                                        <div>{index.reason}</div>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </RichPanelHeader>
            </RichPanel>
        </div>
    );
}
