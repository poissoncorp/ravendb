import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import genUtils from "common/generalUtils";
import changeVectorUtils from "common/changeVectorUtils";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import { LazyLoad } from "components/common/LazyLoad";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { statisticsViewSelectors } from "components/pages/database/status/statistics/store/statisticsViewSlice";
import copyToClipboard = require("common/copyToClipboard");
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { JSX } from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DetailsBlockProps {
    children: (data: DetailedDatabaseStatistics, location: databaseLocationSpecifier) => JSX.Element;
}

export function DetailedDatabaseStats() {
    const t = useStudioTranslation("statistics");
    const perNodeStats = useAppSelector(statisticsViewSelectors.allDatabaseDetails);
    const copyChangeVector = (formattedChangeVector: changeVectorItem[]) => {
        copyToClipboard.copy(
            formattedChangeVector.map((cv) => cv.fullFormat).join(", "),
            t("detailed.changeVectorCopied")
        );
    };

    function DetailsBlock(props: DetailsBlockProps): JSX.Element {
        const { children } = props;

        return (
            <>
                {perNodeStats.map((perNode) => {
                    const { location, data: stat, status } = perNode;

                    if (status === "failure") {
                        return (
                            <td key={genUtils.formatLocation(location)} className="text-danger">
                                <Icon icon="cancel" title={t("loadError")} margin="m-0" />
                            </td>
                        );
                    }

                    return (
                        <td key={genUtils.formatLocation(location)}>
                            {status === "success" || (status === "loading" && stat) ? (
                                children(stat, location)
                            ) : (
                                <LazyLoad active>
                                    <div>{t("loading")}</div>
                                </LazyLoad>
                            )}
                        </td>
                    );
                })}
            </>
        );
    }

    return (
        <section className="mt-6">
            <h2 className="on-base-background">{t("detailed.title")}</h2>
            <Card className="panel mt-4">
                <Table responsive bordered striped className="mb-0">
                    <thead>
                        <tr>
                            <th>&nbsp;</th>
                            {perNodeStats.map(({ location }) => {
                                return (
                                    <th key={genUtils.formatLocation(location)}>{genUtils.formatLocation(location)}</th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>
                                <Icon icon="database-id" /> <span>{t("detailed.databaseId")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.DatabaseId}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="vector" /> <span>{t("detailed.databaseChangeVector")}</span>
                            </td>
                            <DetailsBlock>
                                {(data, location) => {
                                    const id = "js-cv-" + location.nodeTag + "-" + location.shardNumber;

                                    const formattedChangeVector = changeVectorUtils.formatChangeVector(
                                        data.DatabaseChangeVector,
                                        changeVectorUtils.shouldUseLongFormat([data.DatabaseChangeVector])
                                    );

                                    if (formattedChangeVector.length === 0) {
                                        return <span>{t("detailed.changeVectorNotDefined")}</span>;
                                    }

                                    return (
                                        <PopoverWithHoverWrapper
                                            message={formattedChangeVector.map((cv) => (
                                                <div key={cv.fullFormat}>
                                                    <small>{cv.fullFormat}</small>
                                                </div>
                                            ))}
                                        >
                                            <div id={id} className="d-inline-flex flex-wrap gap-1">
                                                <Button
                                                    variant="primary"
                                                    size="xs"
                                                    title={t("detailed.copyChangeVectorTitle")}
                                                    onClick={() => copyChangeVector(formattedChangeVector)}
                                                >
                                                    <Icon icon="copy-to-clipboard" margin="m-0" />
                                                </Button>
                                                {formattedChangeVector.map((cv) => (
                                                    <div
                                                        key={cv.fullFormat}
                                                        className="badge bg-secondary"
                                                        id="popoverContainer"
                                                    >
                                                        {cv.shortFormat}
                                                    </div>
                                                ))}
                                            </div>
                                        </PopoverWithHoverWrapper>
                                    );
                                }}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="storage" />
                                <span>{t("detailed.sizeOnDisk")}</span>
                            </td>
                            <DetailsBlock>
                                {(data, location) => {
                                    const id = "js-size-on-disk-" + location.nodeTag + "-" + location.shardNumber;
                                    return (
                                        <PopoverWithHoverWrapper
                                            message={
                                                <StudioTrans
                                                    ns="statistics"
                                                    i18nKey="detailed.sizeOnDiskDetails"
                                                    components={{ strong: <strong />, br: <br /> }}
                                                    options={{
                                                        data: genUtils.formatBytesToSize(data.SizeOnDisk.SizeInBytes),
                                                        temp: genUtils.formatBytesToSize(
                                                            data.TempBuffersSizeOnDisk.SizeInBytes
                                                        ),
                                                        total: genUtils.formatBytesToSize(
                                                            data.SizeOnDisk.SizeInBytes +
                                                                data.TempBuffersSizeOnDisk.SizeInBytes
                                                        ),
                                                    }}
                                                />
                                            }
                                        >
                                            <span id={id}>
                                                {genUtils.formatBytesToSize(
                                                    data.SizeOnDisk.SizeInBytes + data.TempBuffersSizeOnDisk.SizeInBytes
                                                )}
                                            </span>
                                        </PopoverWithHoverWrapper>
                                    );
                                }}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="etag" />
                                <span>{t("detailed.lastDocumentEtag")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.LastDocEtag}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="etag" />
                                <span>{t("detailed.lastDatabaseEtag")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.LastDatabaseEtag}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="server" />
                                <span>{t("detailed.architecture")}</span>
                            </td>
                            <DetailsBlock>
                                {(data) => (
                                    <>
                                        {data.Is64Bit
                                            ? t("detailed.architecture64Bit")
                                            : t("detailed.architecture32Bit")}
                                    </>
                                )}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="documents" />
                                <span>{t("detailed.documents")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfDocuments.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="new-counter" />
                                <span>{t("detailed.counters")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfCounterEntries.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="identities" />
                                <span>{t("detailed.identities")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfIdentities.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="indexing" />
                                <span>{t("detailed.indexes")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfIndexes.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="revisions" />
                                <span>{t("detailed.revisions")}</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{(data.CountOfRevisionDocuments ?? 0).toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="conflicts" />
                                <span>{t("detailed.conflicts")}</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{data.CountOfDocumentsConflicts.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="attachment" />
                                <span>{t("detailed.attachments.label")}</span>
                            </td>
                            <DetailsBlock>
                                {(data) => (
                                    <div>
                                        <span>
                                            {t("detailed.attachments.total", {
                                                count: data.CountOfAttachments,
                                                formattedCount: data.CountOfAttachments.toLocaleString(),
                                            })}
                                        </span>
                                        {data.CountOfAttachments !== data.CountOfUniqueAttachments && (
                                            <>
                                                <span className="text-muted">•</span>
                                                <small>
                                                    <span className="text-muted">
                                                        {t("detailed.attachments.unique", {
                                                            count: data.CountOfUniqueAttachments,
                                                            formattedCount:
                                                                data.CountOfUniqueAttachments.toLocaleString(),
                                                        })}
                                                    </span>
                                                </small>
                                                <span className="text-muted">•</span>
                                                <small>
                                                    <span className="text-muted">
                                                        {t("detailed.attachments.remote", {
                                                            count: data.CountOfRemoteAttachments,
                                                            formattedCount:
                                                                data.CountOfRemoteAttachments.toLocaleString(),
                                                        })}
                                                    </span>
                                                </small>
                                            </>
                                        )}
                                    </div>
                                )}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="cmp-xchg" />
                                <span>{t("detailed.compareExchange")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfCompareExchange.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="cmp-xchg" />
                                <span>{t("detailed.compareExchangeTombstones")}</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{data.CountOfCompareExchangeTombstones.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="zombie" />
                                <span>{t("detailed.tombstones")}</span>
                            </td>
                            <DetailsBlock>{(data) => <>{data.CountOfTombstones.toLocaleString()}</>}</DetailsBlock>
                        </tr>
                        <tr>
                            <td>
                                <Icon icon="timeseries-settings" />
                                <span>{t("detailed.timeSeriesSegments")}</span>
                            </td>
                            <DetailsBlock>
                                {(data) => <>{data.CountOfTimeSeriesSegments.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                    </tbody>
                </Table>
            </Card>
        </section>
    );
}
