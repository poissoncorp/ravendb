import React, { JSX } from "react";
import { withPreventDefault } from "components/utils/common";
import genUtils from "common/generalUtils";
import { useEventsCollector } from "hooks/useEventsCollector";
import Spinner from "react-bootstrap/Spinner";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import { LazyLoad } from "components/common/LazyLoad";
import { useAppSelector } from "components/store";
import { IndexItem, PerLocationIndexStats } from "components/pages/database/status/statistics/store/models";
import { LoadError } from "components/common/LoadError";
import { shallowEqual } from "react-redux";
import appUrl from "common/appUrl";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import app from "durandal/app";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";
import { statisticsViewSelectors } from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface IndexBlockProps {
    children: (locationData: PerLocationIndexStats, location: databaseLocationSpecifier) => JSX.Element;
    index: IndexItem;
    alwaysRenderValue?: boolean;
}

function IndexStatistics(props: { indexName: string }) {
    const { indexName } = props;
    const t = useStudioTranslation("statistics");

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const index = useAppSelector(statisticsViewSelectors.indexByName(indexName));

    const showErrorCounts = index.details.some((x) => x && x.errorsCount > 0);
    const showMapErrors = index.details.some((x) => x && x.mapErrors > 0);
    const performanceUrl = appUrl.forIndexPerformance(db.name, indexName);

    const showMapReferenceSection = index.details.some(
        (x) => x && (x.mapReferenceSuccesses > 0 || x.mapReferenceErrors > 0 || x.mapReferenceAttempts > 0)
    );
    const showMapReferenceErrors = index.details.some((x) => x && x.mapReferenceErrors > 0);
    const showMappedPerSecondRate = index.details.some((x) => x && x.mappedPerSecondRate > 1);
    const showReducedPerSecondRate = index.details.some((x) => x && x.reducedPerSecondRate > 1);
    const showReduceErrors = index.details.some((x) => x && x.reduceErrors > 0);

    const referencedCollections = index.sharedInfo.referencedCollections;

    const { reportEvent } = useEventsCollector();

    const showStaleReasons = (index: IndexItem, location: databaseLocationSpecifier) => {
        const view = new indexStalenessReasons(db.name, index.sharedInfo.name, location);
        reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    };

    return (
        <React.Fragment key={indexName}>
            <h4 className="text-elipsis mt-4">
                <a href={performanceUrl} title={indexName}>
                    {indexName}
                </a>
            </h4>
            <Table responsive striped key={indexName}>
                <tbody>
                    <tr>
                        <td style={{ width: "200px" }}>{t("indexes.staleness")}</td>
                        <DetailsBlock index={index}>
                            {(data, location) =>
                                data.isStale ? (
                                    <a
                                        href="#"
                                        title={t("indexes.staleReasonTitle")}
                                        className="flex-shrink-0 badge badge-warning"
                                        onClick={withPreventDefault(() => showStaleReasons(index, location))}
                                    >
                                        {t("indexes.stale")}
                                    </a>
                                ) : (
                                    <div title={t("indexes.upToDateTitle")}>
                                        <Icon icon="check" color="success" margin="m-0" />
                                    </div>
                                )
                            }
                        </DetailsBlock>
                    </tr>
                    <tr>
                        <td style={{ width: "200px" }}>{t("indexes.nodeTag")}</td>
                        <DetailsBlock index={index} alwaysRenderValue>
                            {(data, location) => <>{location.nodeTag}</>}
                        </DetailsBlock>
                    </tr>
                    {db.isSharded && (
                        <tr>
                            <td>{t("indexes.shardNumber")}</td>
                            <DetailsBlock index={index} alwaysRenderValue>
                                {(data, location) => <>{location.shardNumber}</>}
                            </DetailsBlock>
                        </tr>
                    )}
                    <tr>
                        <td>{t("indexes.entriesCount")}</td>
                        <DetailsBlock index={index}>{(data) => <>{data.entriesCount.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    {showErrorCounts && (
                        <tr>
                            <td>{t("indexes.errorsCount")}</td>
                            <DetailsBlock index={index}>
                                {(data) => <>{data.errorsCount.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                    )}
                    <tr>
                        <td>{t("indexes.mapAttempts")}</td>
                        <DetailsBlock index={index}>{(data) => <>{data.mapAttempts.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>{t("indexes.mapSuccesses")}</td>
                        <DetailsBlock index={index}>{(data) => <>{data.mapSuccesses.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    {showMapErrors && (
                        <tr>
                            <td>{t("indexes.mapErrors")}</td>
                            <DetailsBlock index={index}>
                                {(data) => <>{data.mapErrors.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                    )}
                    {showMapReferenceSection && (
                        <>
                            <tr>
                                <td>{t("indexes.mapReferenceAttempts")}</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.mapReferenceAttempts.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            <tr>
                                <td>{t("indexes.mapReferenceSuccesses")}</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.mapReferenceSuccesses.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            {showMapReferenceErrors && (
                                <tr>
                                    <td>{t("indexes.mapReferenceErrors")}</td>
                                    <DetailsBlock index={index}>
                                        {(data) => <>{data.mapReferenceErrors.toLocaleString()}</>}
                                    </DetailsBlock>
                                </tr>
                            )}
                        </>
                    )}
                    {showMappedPerSecondRate && (
                        <tr>
                            <td>{t("indexes.mappedPerSecondRate")}</td>
                            <DetailsBlock index={index}>
                                {(data) => (
                                    <>
                                        {data.mappedPerSecondRate > 0.01
                                            ? genUtils.formatNumberToStringFixed(data.mappedPerSecondRate, 2)
                                            : "0"}
                                    </>
                                )}
                            </DetailsBlock>
                        </tr>
                    )}

                    {index.sharedInfo.isReduceIndex && (
                        <>
                            <tr>
                                <td>{t("indexes.reduceAttempts")}</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.reduceAttempts.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            <tr>
                                <td>{t("indexes.reduceSuccesses")}</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.reduceSuccesses.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            {showReduceErrors && (
                                <tr>
                                    <td>{t("indexes.reduceErrors")}</td>
                                    <DetailsBlock index={index}>
                                        {(data) => <>{data.reduceErrors.toLocaleString()}</>}
                                    </DetailsBlock>
                                </tr>
                            )}
                            {showReducedPerSecondRate && (
                                <tr>
                                    <td>{t("indexes.reducedPerSecondRate")}</td>
                                    <DetailsBlock index={index}>
                                        {(data) => (
                                            <>
                                                {data.reducedPerSecondRate > 0.01
                                                    ? genUtils.formatNumberToStringFixed(data.reducedPerSecondRate, 2)
                                                    : "0"}
                                            </>
                                        )}
                                    </DetailsBlock>
                                </tr>
                            )}
                        </>
                    )}
                </tbody>
            </Table>
            {referencedCollections?.length > 0 && (
                <div className="small">
                    <span className="text-muted">
                        <Icon icon="referenced-collections" title={t("indexes.referencedCollectionsTitle")} />
                        {t("indexes.referencedCollections")}
                    </span>{" "}
                    <strong>{referencedCollections.join(", ")}</strong>
                </div>
            )}
        </React.Fragment>
    );
}

function DetailsBlock(props: IndexBlockProps): JSX.Element {
    const { children, index, alwaysRenderValue } = props;
    const t = useStudioTranslation("statistics");

    const loadStatuses = useAppSelector(statisticsViewSelectors.allIndexesLoadStatus);

    return (
        <>
            {loadStatuses.map((loadStatus, statusIndex) => {
                const location = loadStatus.location;
                const status = loadStatus.status;
                const locationDetails = index.details[statusIndex];

                const key = genUtils.formatLocation(location);
                if (alwaysRenderValue) {
                    return <td key={key}>{children(locationDetails, location)}</td>;
                }

                if (status === "success" && !locationDetails) {
                    return (
                        <td key={key} className="text-danger">
                            {t("indexes.notFoundOnLocation", { location: genUtils.formatLocation(location) })}
                        </td>
                    );
                }

                const faulty = status === "success" && locationDetails?.isFaultyIndex;

                if (faulty) {
                    return (
                        <td key={key} className="text-danger">
                            {t("indexes.faultyIndex")}
                        </td>
                    );
                }

                if (status === "failure") {
                    return (
                        <td key={key} className="text-danger">
                            <Icon icon="cancel" title={t("loadError")} margin="m-0" />
                        </td>
                    );
                }

                if (status === "loading" || status === "idle") {
                    return (
                        <td key={key}>
                            <LazyLoad active>
                                <div>{t("loading")}</div>
                            </LazyLoad>
                        </td>
                    );
                }

                return <td key={key}>{children(locationDetails, location)}</td>;
            })}
        </>
    );
}

export function IndexesDatabaseStats() {
    const t = useStudioTranslation("statistics");
    const globalIndexDetailsStatus = useAppSelector(statisticsViewSelectors.globalIndexDetailsStatus);
    const mapIndexNames = useAppSelector(statisticsViewSelectors.mapIndexNames, shallowEqual);
    const mapReduceIndexNames = useAppSelector(statisticsViewSelectors.mapReduceIndexNames, shallowEqual);

    if (globalIndexDetailsStatus === "loading") {
        return (
            <div className="text-center p-5">
                <Spinner />
            </div>
        );
    }

    if (globalIndexDetailsStatus === "failure") {
        return <LoadError error={t("indexes.loadFailed")} />;
    }

    const noData = mapIndexNames.length === 0 && mapReduceIndexNames.length === 0;

    return (
        <section className="mt-6">
            <h2 className="on-base-background">{t("indexes.title")}</h2>

            {noData && <EmptySet>{t("indexes.empty")}</EmptySet>}

            {mapIndexNames.length > 0 && (
                <Card className="p-4" key="maps">
                    <h3>{t("indexes.mapIndexes")}</h3>
                    <div>
                        {mapIndexNames.map((index) => (
                            <IndexStatistics key={index} indexName={index} />
                        ))}
                    </div>
                </Card>
            )}

            {mapReduceIndexNames.length > 0 && (
                <Card className="p-4" key="reduces">
                    <h3>{t("indexes.mapReduceIndexes")}</h3>
                    <div>
                        {mapReduceIndexNames.map((index) => (
                            <IndexStatistics key={index} indexName={index} />
                        ))}
                    </div>
                </Card>
            )}
        </section>
    );
}
