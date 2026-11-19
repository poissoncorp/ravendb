import { ColumnDef } from "@tanstack/react-table";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import document from "models/database/documents/document";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("virtualTable");

export const columnDocumentFlags: ColumnDef<document> = {
    id: "Flags",
    header: () => (
        <span>
            <Icon icon="flag" />
            {t("columns.flags")}
        </span>
    ),
    accessorFn: (x) => x.__metadata,
    cell: ({ getValue }) => {
        const metadata = getValue<document["__metadata"]>();
        const flags = metadata.flags ?? "";

        return (
            <span className="flags">
                <Icon
                    icon="attachment"
                    title={t("flags.attachments")}
                    className={classNames({ attachments: flags.includes("HasAttachments") })}
                />
                <Icon
                    icon="revisions"
                    title={t("flags.revisions")}
                    className={classNames({ revisions: flags.includes("HasRevisions") })}
                />
                <Icon
                    icon="new-counter"
                    title={t("flags.counters")}
                    className={classNames({ counters: flags.includes("HasCounters") })}
                />
                <Icon
                    icon="new-time-series"
                    title={t("flags.timeSeries")}
                    className={classNames({ "time-series": flags.includes("HasTimeSeries") })}
                />
                <Icon
                    icon="data-archival"
                    title={t("flags.archived")}
                    className={classNames({ archived: metadata.archived })}
                />
                {metadata.shardNumber != null && (
                    <span title={t("flags.shard", { shardNumber: metadata.shardNumber })} className="small-label">
                        <Icon icon="shard" color="shard" />
                        {metadata.shardNumber}
                    </span>
                )}
            </span>
        );
    },
    enableSorting: false,
    enableColumnFilter: false,
};
