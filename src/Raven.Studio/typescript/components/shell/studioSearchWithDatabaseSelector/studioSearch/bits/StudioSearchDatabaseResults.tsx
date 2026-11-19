import React from "react";
import { EmptySet } from "components/common/EmptySet";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import StudioSearchDatabaseGroupHeader from "../bits/StudioSearchDatabaseGroupHeader";
import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultDatabaseGroup, StudioSearchResultItem } from "../studioSearchTypes";
import { useAppSelector } from "components/store";
import Dropdown from "react-bootstrap/Dropdown";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function StudioSearchDatabaseResults(props: {
    hasDatabaseMatch: boolean;
    databaseResults: StudioSearchResult["database"];
    activeItem?: StudioSearchResultItem;
    searchQuery: string;
}) {
    const { hasDatabaseMatch, databaseResults, activeItem, searchQuery } = props;
    const t = useStudioTranslation("studioSearchWithDatabaseSelector");

    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);

    if (!activeDatabase) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    <StudioTrans
                        ns="studioSearchWithDatabaseSelector"
                        i18nKey="search.noActiveDatabase"
                        components={{ br: <br /> }}
                    />
                </EmptySet>
            </Dropdown.Item>
        );
    }

    if (!searchQuery) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    <StudioTrans
                        ns="studioSearchWithDatabaseSelector"
                        i18nKey="search.searchDatabase"
                        options={{ name: activeDatabase.name }}
                        components={{ b: <b />, br: <br /> }}
                    />
                </EmptySet>
            </Dropdown.Item>
        );
    }

    if (!hasDatabaseMatch) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    <span>
                        <StudioTrans
                            ns="studioSearchWithDatabaseSelector"
                            i18nKey="search.noMatches"
                            options={{ query: props.searchQuery, name: activeDatabase.name }}
                            components={{ b: <b /> }}
                        />
                    </span>
                    <br />
                    <span>{t("search.refineSearch")}</span>
                </EmptySet>
            </Dropdown.Item>
        );
    }

    return getSortedKeys(databaseResults).map((groupType: StudioSearchResultDatabaseGroup) => (
        <div key={groupType} className="studio-search__database-col__group">
            <Dropdown.Header className="studio-search__database-col__group__header">
                <StudioSearchDatabaseGroupHeader groupType={groupType} />
            </Dropdown.Header>
            {databaseResults[groupType].map((item) => (
                <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
            ))}
        </div>
    ));
}

function getSortedKeys(databaseResults: StudioSearchResult["database"]): string[] {
    let keys = Object.keys(databaseResults).filter(
        (groupType: StudioSearchResultDatabaseGroup) => databaseResults[groupType].length > 0
    );

    // move revisions after documents
    if (keys.includes("revisions") && keys.includes("documents")) {
        keys = keys.filter((x) => x !== "revisions");
        keys.splice(keys.indexOf("documents") + 1, 0, "revisions");
    }

    return keys;
}
