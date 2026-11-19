import React, { ChangeEvent } from "react";
import { DatabaseFilterByStateOption, DatabaseFilterCriteria } from "components/models/databases";
import Form from "react-bootstrap/Form";
import { useAppSelector } from "components/store";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import { InputItem } from "components/models/common";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { databasesViewSelectors } from "components/pages/resources/databases/store/databasesViewSelectors";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type FilterByStateOptions = InputItem<DatabaseFilterByStateOption>[];

interface DatabasesFilterProps {
    searchCriteria: DatabaseFilterCriteria;
    setFilterCriteria: (criteria: DatabaseFilterCriteria) => void;
}

export function DatabasesFilter(props: DatabasesFilterProps) {
    const { searchCriteria, setFilterCriteria } = props;
    const t = useStudioTranslation("databases");

    const allDatabasesCount = useAppSelector(databaseSelectors.allDatabasesCount);
    const filterByStateOptions: FilterByStateOptions = useAppSelector(databasesViewSelectors.filterByStateOptions);

    const onSearchNameChange = (e: ChangeEvent<HTMLInputElement>) => {
        setFilterCriteria({
            name: e.target.value,
            states: searchCriteria.states,
        });
    };

    const onSearchStatusesChange = (states: DatabaseFilterByStateOption[]) => {
        setFilterCriteria({
            name: searchCriteria.name,
            states,
        });
    };

    return (
        <>
            <div className="d-flex flex-column flex-grow">
                <div className="small-label ms-1 mb-1">{t("filter.byName")}</div>
                <div className="clearable-input">
                    <Form.Control
                        type="text"
                        accessKey="/"
                        placeholder={t("filter.namePlaceholder")}
                        title={t("filter.nameTitle")}
                        value={searchCriteria.name}
                        onChange={onSearchNameChange}
                        className="filtering-input"
                    />
                    {searchCriteria.name && (
                        <div className="clear-button">
                            <Button
                                variant="secondary"
                                size="sm"
                                onClick={() =>
                                    setFilterCriteria({
                                        name: "",
                                        states: searchCriteria.states,
                                    })
                                }
                            >
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div>
                <MultiCheckboxToggle<DatabaseFilterByStateOption>
                    inputItems={filterByStateOptions}
                    label={t("filter.byState")}
                    selectedItems={searchCriteria.states}
                    setSelectedItems={onSearchStatusesChange}
                    selectAll
                    selectAllLabel={t("filter.all")}
                    selectAllCount={allDatabasesCount}
                />
            </div>
        </>
    );
}
