import React, { useState } from "react";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import { Icon } from "components/common/Icon";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { FilterChips, FilterMenu, MenuHeader } from "components/common/filterMenu/FilterMenu";
import Select, {
    OptionWithIcon,
    SelectOption,
    SelectOptionWithIcon,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { SortDirection } from "components/models/common";
import { getSinkSortFields, ReplicationHubSinksState, SinkSortField } from "./useReplicationHubSinks";
import "./ReplicationHubSinksHeader.scss";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface ReplicationHubSinksHeaderProps {
    sinksState: ReplicationHubSinksState;
    onToggleAllExpanded: () => void;
}

export function ReplicationHubSinksHeader({ sinksState, onToggleAllExpanded }: ReplicationHubSinksHeaderProps) {
    const t = useStudioTranslation("ongoingTasks");
    const { filterCategories, hasActiveFilters, totalSinksCount, visibleSinks, areAllVisibleSinksExpanded } =
        sinksState;

    return (
        <>
            <div className="replication-hub-sinks-header d-flex align-items-center justify-content-between gap-2">
                <div className="d-flex align-items-center gap-2">
                    <h4 className="mb-0">{t("sinksHeader.title")}</h4>
                    <Badge pill bg="secondary">
                        {totalSinksCount}
                    </Badge>
                </div>
                <ButtonGroup size="sm" className="sinks-header-actions">
                    <SortDropdown sinksState={sinksState} />
                    <FilterMenu categories={filterCategories} title={t("sinksHeader.filterTitle")} />
                    <Button
                        variant="secondary"
                        onClick={onToggleAllExpanded}
                        title={t("sinksHeader.toggleAllTitle", {
                            context: areAllVisibleSinksExpanded ? "collapse" : "expand",
                        })}
                    >
                        <Icon icon={areAllVisibleSinksExpanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </ButtonGroup>
            </div>
            <FilterChips categories={filterCategories} className="replication-hub-sinks-chips">
                <span className="sinks-results-count">
                    <StudioTrans
                        ns="ongoingTasks"
                        i18nKey="sinksHeader.resultsCount"
                        options={{
                            count: totalSinksCount,
                            visible: visibleSinks.length,
                            total: totalSinksCount.toLocaleString(),
                        }}
                        components={{ strong: <strong /> }}
                    />
                </span>
            </FilterChips>
            <div className="visually-hidden" role="status">
                {hasActiveFilters
                    ? t("sinksHeader.resultsStatus", {
                          count: totalSinksCount,
                          visible: visibleSinks.length,
                          total: totalSinksCount.toLocaleString(),
                      })
                    : ""}
            </div>
        </>
    );
}

function getSortFieldInfo(t: StudioTranslate<"ongoingTasks">, field: SinkSortField) {
    return getSinkSortFields(t).find((x) => x.field === field);
}

const sortMenuPopperConfig = { modifiers: [{ name: "offset", options: { offset: [0, 7] } }] };

function getSortFieldOptions(t: StudioTranslate<"ongoingTasks">): SelectOptionWithIcon<SinkSortField>[] {
    return getSinkSortFields(t).map(({ field, label, icon }) => ({
        value: field,
        label,
        icon,
    }));
}

function getSortDirectionOptions(
    t: StudioTranslate<"ongoingTasks">,
    field: SinkSortField
): SelectOption<SortDirection>[] {
    return field === "status"
        ? [
              { value: "asc", label: t("sinksHeader.problemsFirst") },
              { value: "desc", label: t("sinksHeader.healthyFirst") },
          ]
        : [
              { value: "asc", label: t("sinksHeader.ascending") },
              { value: "desc", label: t("sinksHeader.descending") },
          ];
}

function SortDropdown({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const t = useStudioTranslation("ongoingTasks");
    const { sort } = sinksState;
    const fieldLabel = getSortFieldInfo(t, sort.field).label;
    const directionLabel = getSortDirectionOptions(t, sort.field).find((x) => x.value === sort.direction).label;
    const isDirectionSpelledOut = sort.field === "status";

    return (
        <Dropdown as={ButtonGroup} autoClose="outside" align="end" className="sinks-sort-dropdown">
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                className="sinks-sort-button"
                title={t("sinksHeader.sortedByTitle", { field: fieldLabel, direction: directionLabel.toLowerCase() })}
            >
                <span className="sinks-sort-button-label">{t("sinksHeader.sortByLabel")}</span>
                <span className="sinks-sort-button-value">{fieldLabel}</span>
                {isDirectionSpelledOut ? (
                    <span>, {directionLabel.toLowerCase()}</span>
                ) : (
                    <Icon icon={sort.direction === "asc" ? "arrow-thin-top" : "arrow-thin-bottom"} margin="ms-1" />
                )}
            </Dropdown.Toggle>
            <Dropdown.Menu className="sinks-sort-menu" popperConfig={sortMenuPopperConfig}>
                <SortMenu sinksState={sinksState} />
            </Dropdown.Menu>
        </Dropdown>
    );
}

function useEscapeClosesOnlyOpenList() {
    const [isListOpen, setIsListOpen] = useState(false);

    return {
        onMenuOpen: () => setIsListOpen(true),
        onMenuClose: () => setIsListOpen(false),
        onKeyDown: (e: React.KeyboardEvent) => {
            if (e.key === "Escape" && isListOpen) {
                e.stopPropagation();
            }
        },
    };
}

function SortMenu({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const t = useStudioTranslation("ongoingTasks");
    const { sort, setSort, isDefaultSort, resetSort } = sinksState;
    const sortFieldOptions = getSortFieldOptions(t);
    const directionOptions = getSortDirectionOptions(t, sort.field);
    const fieldListEscape = useEscapeClosesOnlyOpenList();
    const directionListEscape = useEscapeClosesOnlyOpenList();

    return (
        <>
            <MenuHeader
                title={t("sinksHeader.sortMenuTitle")}
                actionLabel={t("reset")}
                isActionVisible={!isDefaultSort}
                onAction={resetSort}
            />
            <div className="sinks-sort-selects filter-menu-field">
                <Select<SelectOptionWithIcon<SinkSortField>>
                    aria-label={t("sinksHeader.sortFieldAriaLabel")}
                    className="sinks-sort-field-select"
                    options={sortFieldOptions}
                    value={sortFieldOptions.find((x) => x.value === sort.field)}
                    onChange={(x) => setSort({ ...sort, field: x.value })}
                    components={{ Option: OptionWithIcon, SingleValue: SingleValueWithIcon }}
                    isSearchable={false}
                    menuPortalTarget={null}
                    {...fieldListEscape}
                />
                <Select<SelectOption<SortDirection>>
                    aria-label={t("sinksHeader.sortDirectionAriaLabel")}
                    className="sinks-sort-direction-select"
                    options={directionOptions}
                    value={directionOptions.find((x) => x.value === sort.direction)}
                    onChange={(x) => setSort({ ...sort, direction: x.value })}
                    isSearchable={false}
                    menuPortalTarget={null}
                    {...directionListEscape}
                />
            </div>
        </>
    );
}
