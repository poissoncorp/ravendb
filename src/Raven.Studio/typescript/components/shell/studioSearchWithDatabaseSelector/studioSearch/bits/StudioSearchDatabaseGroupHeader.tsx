import { Icon } from "components/common/Icon";
import { StudioSearchResultDatabaseGroup } from "../studioSearchTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface StudioSearchDatabaseGroupHeaderProps {
    groupType: StudioSearchResultDatabaseGroup;
}

export default function StudioSearchDatabaseGroupHeader({ groupType }: StudioSearchDatabaseGroupHeaderProps) {
    const t = useStudioTranslation("studioSearchWithDatabaseSelector");

    switch (groupType) {
        case "collections":
            return (
                <>
                    <Icon icon="documents" />
                    {t("search.groups.collections")}
                </>
            );
        case "documents":
            return (
                <>
                    <Icon icon="document" />
                    {t("search.groups.documents")}
                </>
            );
        case "revisions":
            return (
                <>
                    <Icon icon="revisions" />
                    {t("search.groups.revisions")}
                </>
            );
        case "indexes":
            return (
                <>
                    <Icon icon="index" />
                    {t("search.groups.indexes")}
                </>
            );
        case "tasks":
            return (
                <>
                    <Icon icon="tasks" />
                    {t("search.groups.tasks")}
                </>
            );
        case "settings":
            return (
                <>
                    <Icon icon="settings" />
                    {t("search.groups.settings")}
                </>
            );
        case "stats":
            return (
                <>
                    <Icon icon="stats" />
                    {t("search.groups.stats")}
                </>
            );
        case "aiHub":
            return (
                <>
                    <Icon icon="ai" />
                    {t("search.groups.aiHub")}
                </>
            );
        default:
            assertUnreachable(groupType);
    }
}
