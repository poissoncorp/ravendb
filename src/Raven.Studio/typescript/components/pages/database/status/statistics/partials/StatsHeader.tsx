import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {
    refresh,
    statisticsViewSelectors,
    toggleDetails,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { StickyHeader } from "components/common/StickyHeader";
import React from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function StatsHeader() {
    const t = useStudioTranslation("statistics");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();

    const detailsVisible = useAppSelector(statisticsViewSelectors.detailsVisible);
    const isRefreshing = useAppSelector(statisticsViewSelectors.refreshing);

    return (
        <StickyHeader>
            <Row>
                <Col sm="auto">
                    <Button
                        variant="secondary"
                        onClick={() => dispatch(toggleDetails())}
                        title={t("header.toggleDetailsTitle")}
                    >
                        <Icon icon={detailsVisible ? "collapse-vertical" : "expand-vertical"} />
                        <span>{t("header.toggleDetails", { context: detailsVisible ? "hide" : "show" })}</span>
                    </Button>
                </Col>
                <Col />
                <Col sm="auto">
                    <ButtonWithSpinner
                        variant="primary"
                        onClick={() => dispatch(refresh())}
                        title={t("header.refreshTitle")}
                        isSpinning={isRefreshing}
                    >
                        {commonT("refresh")}
                    </ButtonWithSpinner>
                </Col>
            </Row>
        </StickyHeader>
    );
}
