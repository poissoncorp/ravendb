import { ChangeEvent } from "react";
import { Icon } from "components/common/Icon";
import Select, {
    SelectOption,
    SelectOptionWithIconAndSeparator,
    OptionWithIconAndSeparator,
    SingleValueWithIcon,
} from "components/common/select/Select";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { CertificatesSortMode } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { useAppDispatch, useAppSelector } from "components/store";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import useDebouncedInput from "components/hooks/useDebouncedInput";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

export default function CertificatesFilters() {
    const t = useStudioTranslation("certificates");
    const dispatch = useAppDispatch();

    const nameOrThumbprintFilter = useAppSelector(certificatesSelectors.nameOrThumbprintFilter);
    const clearanceFilter = useAppSelector(certificatesSelectors.clearanceFilter);
    const clearanceFilterOptions = useAppSelector(certificatesSelectors.clearanceFilterOptions);
    const stateFilter = useAppSelector(certificatesSelectors.stateFilter);
    const stateFilterOptions = useAppSelector(certificatesSelectors.stateFilterOptions);
    const databaseFilter = useAppSelector(certificatesSelectors.databaseFilter);
    const databaseOptions: SelectOption[] = useAppSelector(databaseSelectors.allDatabaseNames).map((x) => ({
        value: x,
        label: x,
    }));
    const sortMode = useAppSelector(certificatesSelectors.sortMode);
    const managementTypeFilter = useAppSelector(certificatesSelectors.managementTypeFilter);
    const managementTypeFilterOptions = useAppSelector(certificatesSelectors.managementTypeFilterOptions);

    const { localValue: nameOrThumbprintFilterInputValue, handleChange: nameOrThumbprintFilterInputHandleChange } =
        useDebouncedInput({
            value: nameOrThumbprintFilter,
            onDebouncedUpdate: (value: string) => dispatch(certificatesActions.nameOrThumbprintFilterSet(value)),
        });

    const sortOptions = getSortOptions(t);

    return (
        <div className="hstack gap-2 mt-2 flex-wrap">
            <div className="flex-grow">
                <span className="small-label">{t("filters.nameOrThumbprint")}</span>
                <div className="clearable-input">
                    <Form.Control
                        onChange={(x: ChangeEvent<HTMLInputElement>) =>
                            nameOrThumbprintFilterInputHandleChange(x.target.value)
                        }
                        value={nameOrThumbprintFilterInputValue}
                        placeholder={t("filters.nameOrThumbprintPlaceholder")}
                        className="rounded-pill pe-4"
                    />
                    {nameOrThumbprintFilter && (
                        <div className="clear-button">
                            <Button
                                variant="secondary"
                                size="sm"
                                onClick={() => nameOrThumbprintFilterInputHandleChange("")}
                            >
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div>
                <span className="small-label">{t("filters.database")}</span>
                <SelectCreatable<SelectOption>
                    options={databaseOptions}
                    onChange={(x) => dispatch(certificatesActions.databaseFilterSet(x?.value))}
                    value={databaseOptions.find((x) => x.value === databaseFilter)}
                    className="rounded-pill"
                    placeholder={t("filters.databasePlaceholder")}
                    isRoundedPill
                    isClearable
                    styles={{
                        container: (base) => ({
                            ...base,
                            width: "250px",
                        }),
                    }}
                />
            </div>
            <div>
                <span className="small-label">{t("filters.clearance")}</span>
                <Select
                    isMulti
                    isClearable
                    options={clearanceFilterOptions}
                    value={clearanceFilterOptions.filter((x) => clearanceFilter.includes(x.value))}
                    onChange={(options) =>
                        dispatch(certificatesActions.clearanceFilterSet(options ? options.map((o) => o.value) : []))
                    }
                    styles={{
                        container: (base) => ({
                            ...base,
                            minWidth: "220px",
                        }),
                    }}
                />
            </div>
            <div>
                <span className="small-label">{t("filters.state")}</span>
                <Select
                    isMulti
                    isClearable
                    options={stateFilterOptions}
                    value={stateFilterOptions.filter((x) => stateFilter.includes(x.value))}
                    onChange={(options) =>
                        dispatch(certificatesActions.stateFilterSet(options ? options.map((o) => o.value) : []))
                    }
                    styles={{
                        container: (base) => ({
                            ...base,
                            minWidth: "220px",
                        }),
                    }}
                />
            </div>
            <div>
                <span className="small-label">{t("filters.managementType")}</span>
                <Select
                    isMulti
                    isClearable
                    options={managementTypeFilterOptions}
                    value={managementTypeFilterOptions.filter((x) => managementTypeFilter.includes(x.value))}
                    onChange={(options) =>
                        dispatch(
                            certificatesActions.managementTypeFilterSet(options ? options.map((o) => o.value) : [])
                        )
                    }
                    styles={{
                        container: (base) => ({
                            ...base,
                            minWidth: "220px",
                        }),
                    }}
                />
            </div>
            <div style={{ minWidth: 250 }}>
                <span className="small-label">{t("filters.sort")}</span>
                <Select<SelectOptionWithIconAndSeparator<CertificatesSortMode>>
                    options={sortOptions}
                    onChange={(x) => dispatch(certificatesActions.sortModeSet(x.value))}
                    value={sortOptions.find((x) => x.value === sortMode)}
                    className="rounded-pill"
                    placeholder={t("filters.databasePlaceholder")}
                    components={{ Option: OptionWithIconAndSeparator, SingleValue: SingleValueWithIcon }}
                    isRoundedPill
                />
            </div>
        </div>
    );
}

function getSortOptions(t: StudioTranslate<"certificates">): SelectOptionWithIconAndSeparator<CertificatesSortMode>[] {
    return [
        { value: "By Name - Asc", label: t("sort.nameAsc"), icon: "arrow-up" },
        { value: "By Name - Desc", label: t("sort.nameDesc"), icon: "arrow-down", horizontalSeparatorLine: true },
        { value: "By Expiration Date - Asc", label: t("sort.expirationDateAsc"), icon: "arrow-up" },
        {
            value: "By Expiration Date - Desc",
            label: t("sort.expirationDateDesc"),
            icon: "arrow-down",
            horizontalSeparatorLine: true,
        },
        { value: "By Valid-From Date - Asc", label: t("sort.validFromDateAsc"), icon: "arrow-up" },
        {
            value: "By Valid-From Date - Desc",
            label: t("sort.validFromDateDesc"),
            icon: "arrow-down",
            horizontalSeparatorLine: true,
        },
        { value: "By Last Used Date - Asc", label: t("sort.lastUsedDateAsc"), icon: "arrow-up" },
        {
            value: "By Last Used Date - Desc",
            label: t("sort.lastUsedDateDesc"),
            icon: "arrow-down",
        },
    ];
}
