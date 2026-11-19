import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FlexGrow } from "components/common/FlexGrow";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import TombstonesAlert from "components/pages/database/status/tombstones/TombstonesAlert";
import useConfirm from "components/common/ConfirmDialog";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useTombstonesStateColumns } from "components/pages/database/status/tombstones/useTombstonesStateColumns";
import { LazyLoad } from "components/common/LazyLoad";
import { PropsWithChildren, useMemo } from "react";
import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface TombstonesStateProps {
    location?: databaseLocationSpecifier;
}

interface TombstonesStateWithSizeProps extends TombstonesStateProps {
    width: number;
}

export default function TombstonesState(props: TombstonesStateProps) {
    return (
        <div className="content-padding">
            <SizeGetter render={({ width }) => <TombstonesStateWithSize width={width} {...props} />} />
        </div>
    );
}

function TombstonesStateWithSize({ location, width }: TombstonesStateWithSizeProps) {
    const t = useStudioTranslation("tombstones");
    const commonT = useStudioTranslation("common");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { databasesService } = useServices();

    // Changing the database causes re-mount
    const asyncGetTombstonesState = useAsync(() => databasesService.getTombstonesState(databaseName, location), []);
    const asyncForceTombstonesCleanup = useAsyncCallback(() =>
        databasesService.forceTombstonesCleanup(databaseName, location)
    );

    const { collectionsColumns, subscriptionsColumns, formatEtag } = useTombstonesStateColumns(width);

    const tombstoneStateResults = useMemo(() => {
        return asyncGetTombstonesState.result?.Results ?? [];
    }, [asyncGetTombstonesState.result]);

    const tombstoneStatePerSubscriptionInfo = useMemo(() => {
        return asyncGetTombstonesState.result?.PerSubscriptionInfoExtended ?? [];
    }, [asyncGetTombstonesState.result]);

    const collectionsTable = useReactTable({
        columns: collectionsColumns,
        data: tombstoneStateResults,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    const subscriptionsTable = useReactTable({
        columns: subscriptionsColumns,
        data: tombstoneStatePerSubscriptionInfo,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    const confirm = useConfirm();

    const forceCleanup = async () => {
        const isConfirmed = await confirm({
            title: t("forceCleanupConfirmTitle"),
            message: <TombstonesAlert />,
            icon: "force",
            confirmText: t("forceCleanup"),
            actionColor: "warning",
        });

        if (isConfirmed) {
            await asyncForceTombstonesCleanup.execute();
            await asyncGetTombstonesState.execute();
        }
    };

    if (asyncGetTombstonesState.status === "error") {
        return <LoadError error={t("loadFailed")} refresh={asyncGetTombstonesState.execute} />;
    }

    return (
        <>
            <AboutViewHeading title={t("title")} icon="tombstones" />
            <div className="d-flex align-items-start gap-3 flex-wrap">
                <ButtonWithSpinner
                    onClick={asyncGetTombstonesState.execute}
                    variant="primary"
                    isSpinning={asyncGetTombstonesState.loading}
                    icon="refresh"
                >
                    {commonT("refresh")}
                </ButtonWithSpinner>
                <ButtonWithSpinner
                    onClick={forceCleanup}
                    variant="warning"
                    isSpinning={asyncForceTombstonesCleanup.loading}
                    disabled={asyncForceTombstonesCleanup.loading || asyncGetTombstonesState.loading}
                    icon="force"
                >
                    {t("forceCleanup")}
                </ButtonWithSpinner>
                <FlexGrow />
                <div className="d-flex gap-3 flex-wrap">
                    <div>
                        <div className="card p-2 border-radius-xs vstack">
                            <small className="small-label">
                                <Icon icon="document" />
                                {t("minimumEtags.documents")}
                            </small>
                            <h5 className="mt-1 mb-0">
                                <strong>
                                    <EtagValueWithLoader isLoading={asyncGetTombstonesState.loading}>
                                        {formatEtag(asyncGetTombstonesState.result?.MinAllDocsEtag)}
                                    </EtagValueWithLoader>
                                </strong>
                            </h5>
                        </div>
                    </div>
                    <div>
                        <div className="card p-2 border-radius-xs vstack">
                            <small className="small-label">
                                <Icon icon="timeseries" />
                                {t("minimumEtags.timeSeries")}
                            </small>
                            <h5 className="mt-1 mb-0">
                                <strong>
                                    <EtagValueWithLoader isLoading={asyncGetTombstonesState.loading}>
                                        {formatEtag(asyncGetTombstonesState.result?.MinAllTimeSeriesEtag)}
                                    </EtagValueWithLoader>
                                </strong>
                            </h5>
                        </div>
                    </div>
                    <div>
                        <div className="card p-2 border-radius-xs vstack">
                            <small className="small-label">
                                <Icon icon="new-counter" />
                                {t("minimumEtags.counters")}
                            </small>
                            <h5 className="mt-1 mb-0">
                                <strong>
                                    <EtagValueWithLoader isLoading={asyncGetTombstonesState.loading}>
                                        {formatEtag(asyncGetTombstonesState.result?.MinAllCountersEtag)}
                                    </EtagValueWithLoader>
                                </strong>
                            </h5>
                        </div>
                    </div>
                </div>
            </div>
            <h3 className="mt-3">
                <Icon icon="documents" />
                {t("perCollection.title")}
            </h3>
            <VirtualTable
                table={collectionsTable}
                className="mt-3"
                isLoading={asyncGetTombstonesState.loading}
                heightInPx={300}
            />
            <h3 className="mt-5">
                <Icon icon="tasks" />
                {t("perTask.title")}
            </h3>
            <VirtualTable
                table={subscriptionsTable}
                className="mt-3"
                isLoading={asyncGetTombstonesState.loading}
                heightInPx={300}
            />
        </>
    );
}

function EtagValueWithLoader({ isLoading, children }: PropsWithChildren<{ isLoading: boolean }>) {
    const t = useStudioTranslation("tombstones");

    if (isLoading) {
        return (
            <LazyLoad active>
                <div>{t("loadingPlaceholder")}</div>
            </LazyLoad>
        );
    }

    return children;
}
