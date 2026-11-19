import { IndexStatus, IndexFilterCriteria, IndexType, IndexGroupBy, IndexSortBy } from "components/models/indexes";
import { ChangeEvent } from "react";
import Form from "react-bootstrap/Form";
import { produce } from "immer";
import { Icon } from "components/common/Icon";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import { InputItem, SortDirection } from "components/models/common";
import { Switch } from "components/common/Checkbox";
import { SortDropdown, SortDropdownRadioList, sortItem } from "components/common/SortDropdown";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType;
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface IndexFilterProps {
    filter: IndexFilterCriteria;
    setFilter: (x: IndexFilterCriteria) => void;
    filterByStatusOptions: InputItem<IndexStatus>[];
    filterByTypeOptions: InputItem<IndexType>[];
    filterByEngineOptions: InputItem<SearchEngineType>[];
    indexesCount: number;
}

export default function IndexFilter(props: IndexFilterProps) {
    const { filter, setFilter, filterByStatusOptions, filterByTypeOptions, filterByEngineOptions, indexesCount } =
        props;
    const t = useStudioTranslation("indexesList");

    const sortByOptions = getSortByOptions(t);
    const sortDirectionOptions = getSortDirectionOptions(t);
    const groupByOptions = getGroupByOptions(t);

    // TODO sharding

    /* TODO
    let totalProcessedPerSecond = 0;

    this.indexGroups().forEach(indexGroup => {
        const indexesInGroup = indexGroup.indexes().filter(i => !i.filteredOut());
        indexesCount += indexesInGroup.length;

        totalProcessedPerSecond += _.sum(indexesInGroup
            .filter(i => i.progress() || (i.replacement() && i.replacement().progress()))
            .map(i => {
                let sum = 0;

                const progress = i.progress();
                if (progress) {
                    sum += progress.globalProgress().processedPerSecond();
                }

                const replacement = i.replacement();
                if (replacement) {
                    const replacementProgress = replacement.progress();
                    if (replacementProgress) {
                        sum += replacementProgress.globalProgress().processedPerSecond();
                    }
                }

                return sum;
            }));
    });
    */

    /* TODO
    const indexingErrorsOnlyPart = filter.showOnlyIndexesWithIndexingErrors ? (
        <>
            <Badge pill color="warning" className="mx-1">
                indexing errors only
            </Badge>{" "}
        </>
    ) : (
        ""
    );*/

    const onFilterValueChange = <T extends keyof IndexFilterCriteria>(key: T, value: IndexFilterCriteria[T]) => {
        setFilter(
            produce(filter, (draft) => {
                draft[key] = value;
            })
        );
    };

    return (
        <div className="hstack flex-wrap align-items-end gap-3 my-3 justify-content-end">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">{t("filter.nameLabel")}</div>
                <div className="clearable-input">
                    <Form.Control
                        type="text"
                        accessKey="/"
                        placeholder={t("filter.namePlaceholder")}
                        title={t("filter.nameTitle")}
                        className="filtering-input"
                        value={filter.searchText}
                        onChange={(e: ChangeEvent<HTMLInputElement>) =>
                            onFilterValueChange("searchText", e.target.value)
                        }
                    />
                    {filter.searchText && (
                        <div className="clear-button">
                            <Button variant="secondary" size="sm" onClick={() => onFilterValueChange("searchText", "")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>

            <MultiCheckboxToggle
                inputItems={filterByStatusOptions}
                label={t("filter.stateLabel")}
                selectedItems={filter.statuses}
                setSelectedItems={(x) => onFilterValueChange("statuses", x)}
                selectAll
                selectAllLabel={t("filter.stateAll")}
                selectAllCount={indexesCount}
            />
            <MultiCheckboxToggle
                inputItems={filterByTypeOptions}
                label={t("filter.typeLabel")}
                selectedItems={filter.types}
                setSelectedItems={(x) => onFilterValueChange("types", x)}
                selectAllCount={indexesCount}
            />
            <MultiCheckboxToggle
                inputItems={filterByEngineOptions}
                label={t("filter.engineLabel")}
                selectedItems={filter.searchEngine}
                setSelectedItems={(x) => onFilterValueChange("searchEngine", x)}
                selectAllCount={indexesCount}
            />
            <div>
                <div className="small-label ms-1 mb-1">{t("filter.sortAndGroupLabel")}</div>

                <SortDropdown label={<SortLabel filter={filter} />}>
                    <SortDropdownRadioList
                        radioOptions={sortByOptions}
                        label={t("filter.sortByLabel")}
                        selected={filter.sortBy}
                        setSelected={(x) => onFilterValueChange("sortBy", x)}
                    />
                    <SortDropdownRadioList
                        radioOptions={sortDirectionOptions}
                        label={t("filter.sortDirectionLabel")}
                        selected={filter.sortDirection}
                        setSelected={(x) => onFilterValueChange("sortDirection", x)}
                    />
                    <SortDropdownRadioList
                        radioOptions={groupByOptions}
                        label={t("filter.groupByLabel")}
                        selected={filter.groupBy}
                        setSelected={(x) => onFilterValueChange("groupBy", x)}
                    />
                </SortDropdown>
            </div>
            {/* TODO: `Processing Speed: <strong>${Math.floor(totalProcessedPerSecond).toLocaleString()}</strong> docs / sec`;*/}
            <PopoverWithHoverWrapper
                placement="bottom"
                message={<StudioTrans ns="indexesList" i18nKey="filter.autoRefreshInfo" components={{ br: <br /> }} />}
            >
                <Switch
                    id="autoRefresh"
                    toggleSelection={() => onFilterValueChange("autoRefresh", !filter.autoRefresh)}
                    selected={filter.autoRefresh}
                    color="info"
                    className="mt-1"
                >
                    <span>{t("filter.autoRefresh", { context: filter.autoRefresh ? "on" : "off" })}</span>
                </Switch>
            </PopoverWithHoverWrapper>
        </div>
    );
}

function getSortByOptions(t: StudioTranslate<"indexesList">): sortItem<IndexSortBy>[] {
    return [
        { value: "name", label: t("filter.sortBy.name") },
        { value: "createdTimestamp", label: t("filter.sortBy.creationTime") },
        { value: "lastIndexingTime", label: t("filter.sortBy.lastIndexingTime") },
        { value: "lastQueryingTime", label: t("filter.sortBy.lastQueryingTime") },
    ];
}

function getSortDirectionOptions(t: StudioTranslate<"indexesList">): sortItem<SortDirection>[] {
    return [
        { value: "asc", label: t("filter.sortDirection.ascending") },
        { value: "desc", label: t("filter.sortDirection.descending") },
    ];
}

function getGroupByOptions(t: StudioTranslate<"indexesList">): sortItem<IndexGroupBy>[] {
    return [
        { value: "Collection", label: t("filter.groupBy.collection") },
        { value: "None", label: t("filter.groupBy.none") },
    ];
}

function SortLabel({ filter }: { filter: IndexFilterCriteria }) {
    const t = useStudioTranslation("indexesList");
    const sortByOptions = getSortByOptions(t);
    const groupByOptions = getGroupByOptions(t);

    return (
        <>
            {sortByOptions.find((x) => x.value === filter.sortBy).label}
            {filter.sortDirection === "asc" ? (
                <Icon icon="arrow-thin-top" margin="ms-1" />
            ) : (
                <Icon icon="arrow-thin-bottom" margin="ms-1" />
            )}
            {filter.groupBy !== "None" && (
                <span className="ms-2">{groupByOptions.find((x) => x.value === filter.groupBy).label}</span>
            )}
        </>
    );
}
