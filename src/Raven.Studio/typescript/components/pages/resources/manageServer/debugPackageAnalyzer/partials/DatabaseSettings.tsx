import React, { memo, useMemo, useState } from "react";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type SettingsResult = Raven.Server.Config.SettingsResult;
type ConfigurationEntryDatabaseValue = Raven.Server.Config.ConfigurationEntryDatabaseValue;
type SettingOrigin = "Database" | "Server" | "Default";

interface SettingRow {
    key: string;
    category: string;
    origin: SettingOrigin;
    value: string;
    description: string;
}

interface DatabaseSettingsProps {
    packageId: string;
    database: string;
    node: string;
}

interface DatabaseSettingsWithSizeProps extends DatabaseSettingsProps {
    width: number;
}

function useDatabaseSettingsColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );

    const settingsColumns: ColumnDef<SettingRow>[] = useMemo(() => {
        const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
        return [
            {
                header: t("databaseSettings.columns.key"),
                accessorKey: "key",
                cell: SettingKeyCell,
                size: getSize(33),
            },
            {
                header: t("databaseSettings.columns.category"),
                accessorKey: "category",
                size: getSize(19),
            },
            {
                header: t("databaseSettings.columns.origin"),
                accessorKey: "origin",
                cell: ({ getValue }) => <OriginBadge origin={getValue<SettingOrigin>()} />,
                size: getSize(13),
            },
            {
                header: t("databaseSettings.columns.effectiveValue"),
                accessorKey: "value",
                cell: SettingValueCell,
                size: getSize(35),
            },
        ];
    }, [bodyWidth, t]);

    return { settingsColumns };
}

function OriginBadge({ origin }: { origin: SettingOrigin }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    if (origin === "Database") {
        return (
            <Badge bg="warning" pill>
                {t("databaseSettings.origins.database")}
            </Badge>
        );
    }
    if (origin === "Server") {
        return (
            <Badge bg="info" pill>
                {t("databaseSettings.origins.server")}
            </Badge>
        );
    }
    return <span className="text-muted">{t("databaseSettings.origins.default")}</span>;
}

// On-demand per-node database configuration from the analyzer databases/configuration/settings
// endpoint (a raw SettingsResult). Parsed into a searchable, sortable key/value table - the
// effective value resolves Database override -> Server value -> Default, with secured values masked.
export default memo(function DatabaseSettings({ packageId, database, node }: DatabaseSettingsProps) {
    return (
        <SizeGetter
            render={({ width }) => (
                <DatabaseSettingsWithSize packageId={packageId} database={database} node={node} width={width} />
            )}
        />
    );
});

function DatabaseSettingsWithSize({ packageId, database, node, width }: DatabaseSettingsWithSizeProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();
    // default to the actionable subset: settings actually customized away from their default
    const [showDefaults, setShowDefaults] = useState<boolean>(false);

    const settings = useAsync(async () => {
        if (!node) {
            return null as SettingsResult | null;
        }
        return manageServerService.getDebugPackageDatabaseSettings(packageId, node, database);
    }, [packageId, node, database]);

    const rows = useMemo(() => parseSettings(settings.result, t), [settings.result, t]);
    // per-column filtering is handled by the table's built-in filters; this only applies the
    // "show default values" toggle (defaults are hidden by default to surface customized settings)
    const filtered = useMemo(
        () => (showDefaults ? rows : rows.filter((row) => row.origin !== "Default")),
        [rows, showDefaults]
    );

    const { settingsColumns } = useDatabaseSettingsColumns(width);

    const table = useReactTable({
        data: filtered,
        columns: settingsColumns,
        enableSorting: filtered.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: true,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => row.key,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(filtered.length, 500);

    return (
        <div className="database-settings">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <div className="hstack justify-content-between align-items-center flex-wrap gap-2">
                        <h3 className="m-0">{t("databaseSettings.title")}</h3>
                        {!settings.loading && !settings.error && rows.length > 0 && (
                            <div className="hstack gap-3 align-items-center">
                                <Form.Check
                                    type="switch"
                                    id="debug-package-settings-show-defaults"
                                    label={t("databaseSettings.showDefaults")}
                                    checked={showDefaults}
                                    onChange={(e) => setShowDefaults(e.target.checked)}
                                />
                                <span className="small-label">
                                    {t("databaseSettings.shownOfTotal", { shown: filtered.length, total: rows.length })}
                                </span>
                            </div>
                        )}
                    </div>
                    {settings.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> {t("databaseSettings.loading", { node })}
                        </div>
                    ) : settings.error ? (
                        <RichAlert variant="danger">{t("databaseSettings.loadError", { node })}</RichAlert>
                    ) : rows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            {t("databaseSettings.empty", { database, node })}
                        </EmptySet>
                    ) : filtered.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            {t("databaseSettings.noCustomized")}
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function SettingKeyCell({ row }: { row: { original: SettingRow } }) {
    return <span title={row.original.description}>{row.original.key}</span>;
}

function SettingValueCell({ getValue }: { getValue: () => unknown }) {
    const v = getValue() as string;
    return <span title={v ?? undefined}>{v ?? "-"}</span>;
}

// Effective value resolution mirrors the live settings model: a database override wins over the
// server value, which wins over the configured default. Secured / no-access values are masked.
function parseSettings(
    result: SettingsResult | null | undefined,
    t: StudioTranslate<"debugPackageAnalyzer">
): SettingRow[] {
    if (!result?.Settings) {
        return [];
    }

    return result.Settings.map((entry) => {
        const setting = entry as ConfigurationEntryDatabaseValue;
        const meta = setting.Metadata;
        const keys = meta.Keys ?? [];

        const dbKey = keys.find((k) => setting.DatabaseValues?.[k]?.HasValue);
        const serverKey = keys.find((k) => setting.ServerValues?.[k]);
        const key = dbKey ?? serverKey ?? keys[0] ?? "";

        let value: string;
        let origin: SettingOrigin;
        let hasAccess = true;

        if (dbKey) {
            value = setting.DatabaseValues[dbKey].Value;
            origin = "Database";
            hasAccess = setting.DatabaseValues[dbKey].HasAccess;
        } else if (serverKey && setting.ServerValues[serverKey].HasValue) {
            value = setting.ServerValues[serverKey].Value;
            origin = "Server";
            hasAccess = setting.ServerValues[serverKey].HasAccess;
        } else {
            value = meta.DefaultValue;
            origin = "Default";
        }

        if (meta.IsSecured || hasAccess === false) {
            value = t("databaseSettings.securedValue");
        }

        return { key, category: meta.Category, origin, value, description: meta.Description };
    });
}
