import appUrl from "common/appUrl";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import {
    UseIndexCleanupResult,
    formatIndexCleanupDate,
} from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface MergeIndexesCardProps {
    mergable: UseIndexCleanupResult["mergable"];
}

export default function MergeIndexesCard({ mergable }: MergeIndexesCardProps) {
    const t = useStudioTranslation("indexCleanup");
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Card>
            <Card className="bg-faded-primary p-4 m-1 d-block">
                <div className="text-limit-width">
                    <h2>{t("mergeIndexes.title")}</h2>
                    <StudioTrans
                        ns="indexCleanup"
                        i18nKey="mergeIndexes.description"
                        components={{ br: <br />, strong: <strong /> }}
                    />
                </div>
            </Card>
            {hasIndexCleanup && (
                <div className="p-2">
                    {mergable.data.length === 0 ? (
                        <EmptySet>{t("mergeIndexes.empty")}</EmptySet>
                    ) : (
                        <MainPanel mergable={mergable} />
                    )}
                </div>
            )}
        </Card>
    );
}

function MainPanel({ mergable }: MergeIndexesCardProps) {
    const t = useStudioTranslation("indexCleanup");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <>
            <div className="mx-3">
                <Table className="mb-1 table-inner-border">
                    <tbody>
                        <tr>
                            <td></td>
                            <td width={300}>
                                <div className="small-label">{t("columns.lastQueryTime")}</div>
                            </td>
                            <td width={300}>
                                <div className="small-label">{t("columns.lastIndexingTime")}</div>
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </div>

            {mergable.data.map((mergableGroup, groupKey) => (
                <RichPanel key={"mergeGroup-" + groupKey} hover>
                    <RichPanelHeader className="px-3 py-2 flex-wrap flex-row gap-3">
                        <div className="mt-1">
                            <Button
                                variant="primary"
                                size="sm"
                                className="rounded-pill"
                                onClick={() => mergable.navigateToMergeSuggestion(mergableGroup)}
                            >
                                <Icon icon="merge" />
                                {t("mergeIndexes.reviewButton")}
                            </Button>
                        </div>
                        <div className="flex-grow-1">
                            <Table className="m-0 table-inner-border">
                                <tbody>
                                    {mergableGroup.toMerge.map((index, indexKey) => (
                                        <tr key={"index-" + groupKey + "-" + indexKey}>
                                            <td>
                                                <div>
                                                    <a href={appUrl.forEditIndex(index.name, databaseName)}>
                                                        {index.name} <Icon icon="newtab" margin="ms-1" />
                                                    </a>
                                                </div>
                                            </td>

                                            <td width={300}>
                                                <div>{formatIndexCleanupDate(index.lastQueryingTime)}</div>
                                            </td>
                                            <td width={300}>
                                                <div>{formatIndexCleanupDate(index.lastIndexingTime)}</div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        </div>
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </>
    );
}
