import React from "react";
import Card from "react-bootstrap/Card";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { CounterBadge } from "components/common/CounterBadge";
import UsedId from "components/pages/database/settings/unusedDatabaseIds/partials/UsedId";
import UnusedDatabaseIdsAboutView from "components/pages/database/settings/unusedDatabaseIds/partials/UnusedDatabaseIdsAboutView";
import { LazyLoad } from "components/common/LazyLoad";
import UnusedIdsForm from "components/pages/database/settings/unusedDatabaseIds/partials/UnusedIdsForm";
import { useUnusedDatabaseIds } from "components/pages/database/settings/unusedDatabaseIds/useUnusedDatabaseIds";
import PotentialUnusedIdList from "components/pages/database/settings/unusedDatabaseIds/partials/PotentialUnusedIdList";
import "./UnusedDatabaseIds.scss";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function UnusedDatabaseIds() {
    const { isDirty, usedIds, unusedIds, unusedIdsActions, potentialUnusedId, isLoading, asyncSaveUnusedDatabaseIDs } =
        useUnusedDatabaseIds();
    const t = useStudioTranslation("unusedDatabaseIds");
    const commonT = useStudioTranslation("common");

    return (
        <div className="content-margin unused-database-ids">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title={t("title")} icon="database-id" />
                    <ButtonWithSpinner
                        type="button"
                        variant="primary"
                        className="mb-3"
                        icon="save"
                        onClick={asyncSaveUnusedDatabaseIDs.execute}
                        isSpinning={asyncSaveUnusedDatabaseIDs.loading}
                        disabled={asyncSaveUnusedDatabaseIDs.loading || !isDirty}
                    >
                        {commonT("save")}
                    </ButtonWithSpinner>
                    <Card className="mb-3">
                        <LazyLoad active={isLoading}>
                            <Card.Body>
                                <div className="vstack gap-2">
                                    <div className="d-flex gap-1 align-items-center">
                                        <h4 className="mb-0">{t("usedIds.heading")}</h4>
                                        <CounterBadge count={usedIds.length} />
                                    </div>
                                    <div className="used-ids-grid">
                                        {usedIds.map((usedId) => (
                                            <UsedId key={usedId.databaseId} usedIdData={usedId} />
                                        ))}
                                    </div>
                                </div>
                            </Card.Body>
                        </LazyLoad>
                    </Card>
                    <Card className="mb-3">
                        <LazyLoad active={isLoading}>
                            <Card.Body>
                                <div className="vstack gap-2">
                                    <div className="d-flex gap-1 align-items-center justify-content-between">
                                        <div className="d-flex gap-1">
                                            <h4 className="mb-0">{t("unusedIds.heading")}</h4>
                                            <CounterBadge count={unusedIds.length} />
                                        </div>
                                        <Button
                                            variant="link"
                                            size="xs"
                                            onClick={unusedIdsActions.removeAll}
                                            className="p-0"
                                            title={t("unusedIds.removeAllTitle")}
                                            disabled={unusedIds.length === 0}
                                        >
                                            {t("unusedIds.removeAll")}
                                        </Button>
                                    </div>
                                    <RichAlert variant="info" className="mt-1">
                                        {t("unusedIds.info")}
                                    </RichAlert>
                                    <UnusedIdsForm
                                        ids={unusedIds}
                                        addId={unusedIdsActions.add}
                                        removeId={unusedIdsActions.remove}
                                    />
                                    <PotentialUnusedIdList
                                        potentialUnusedId={potentialUnusedId}
                                        unusedIds={unusedIds}
                                        unusedIdsActions={unusedIdsActions}
                                    />
                                </div>
                            </Card.Body>
                        </LazyLoad>
                    </Card>
                </Col>
                <Col sm={12} lg={4}>
                    <UnusedDatabaseIdsAboutView />
                </Col>
            </Row>
        </div>
    );
}
