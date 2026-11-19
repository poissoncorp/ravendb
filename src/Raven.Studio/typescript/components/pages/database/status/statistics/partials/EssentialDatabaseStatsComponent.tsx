import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import Card from "react-bootstrap/Card";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { LazyLoad } from "components/common/LazyLoad";
import {
    refresh,
    statisticsViewSelectors,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { LoadError } from "components/common/LoadError";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EssentialDatabaseStatsComponentProps {
    rawJsonUrl: string;
}

const defaultLoadingText = "1,234,567";

export function EssentialDatabaseStatsComponent(props: EssentialDatabaseStatsComponentProps) {
    const { rawJsonUrl } = props;
    const t = useStudioTranslation("statistics");

    const essentialStats = useAppSelector(statisticsViewSelectors.essentialStats);
    const dispatch = useAppDispatch();

    const { data: stats } = essentialStats;

    if (essentialStats.status === "failure") {
        return <LoadError refresh={() => dispatch(refresh())} />;
    }

    return (
        <div className="mt-3">
            <Row>
                <Col>
                    <h2 className="on-base-background">
                        {t("essential.title")}
                        <Button variant="link" target="_blank" href={rawJsonUrl} title={t("essential.rawOutputTitle")}>
                            <Icon icon="json" margin="m-0" />
                        </Button>
                    </h2>
                </Col>
            </Row>
            <Card className="stats-list p-4">
                <div className="stats-item">
                    <Icon icon="documents" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.documentsCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfDocuments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="new-counter" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.countersCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfCounterEntries.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="attachment" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.attachmentsCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfAttachments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="indexing" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.indexesCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(stats, (x) => x.CountOfIndexes.toLocaleString(), defaultLoadingText)}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="revisions" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.revisionsCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfRevisionDocuments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="conflicts" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.conflictsCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfDocumentsConflicts.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="zombie" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.tombstonesCount")}</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfTombstones.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="timeseries-settings" margin="m-0" />
                    <div className="name">
                        <span>{t("essential.timeSeriesSegmentsCount")}</span>
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <div className="mb-2">
                                        <StudioTrans
                                            ns="statistics"
                                            i18nKey="essential.timeSeriesSegmentsInfo.storage"
                                            components={{ strong: <strong /> }}
                                        />
                                    </div>
                                    <div>{t("essential.timeSeriesSegmentsInfo.growth")}</div>
                                </>
                            }
                            placement="right"
                            wrapperClassName="flex-shrink-0"
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfTimeSeriesSegments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
            </Card>
        </div>
    );
}

function conditionalRender(
    dto: EssentialDatabaseStatistics,
    accessor: (dto: EssentialDatabaseStatistics) => any,
    defaultValue: string
) {
    if (dto) {
        return accessor(dto);
    }

    return defaultValue;
}
