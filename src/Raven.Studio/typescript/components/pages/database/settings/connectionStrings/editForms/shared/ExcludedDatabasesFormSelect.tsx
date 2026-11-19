import { FormLabel, FormSelect } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Control, FieldValues, Path } from "react-hook-form";
import { useMemo } from "react";
import { ConnectionStringUsage } from "../../connectionStringsTypes";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ExcludedDatabasesFormSelectProps<TFieldValues extends FieldValues> {
    control: Control<TFieldValues>;
    name: Path<TFieldValues>;
    usedBy?: ConnectionStringUsage[];
}

export default function ExcludedDatabasesFormSelect<TFieldValues extends FieldValues>({
    control,
    name,
    usedBy,
}: ExcludedDatabasesFormSelectProps<TFieldValues>) {
    const t = useStudioTranslation("connectionStrings");
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const usedByDatabaseNames = useMemo(() => new Set(usedBy?.map((x) => x.databaseName).filter(Boolean)), [usedBy]);
    const databaseOptions = useMemo(
        () =>
            allDatabaseNames.map((name) => ({
                value: name,
                label: name,
                isDisabled: usedByDatabaseNames.has(name),
            })),
        [allDatabaseNames, usedByDatabaseNames]
    );

    return (
        <div className="mb-2">
            <FormLabel>{t("form.excludedDatabasesLabel")}</FormLabel>
            <FormSelect
                control={control}
                name={name}
                isMulti
                options={databaseOptions}
                placeholder={t("form.excludedDatabasesPlaceholder")}
                isClearable
            />
        </div>
    );
}
