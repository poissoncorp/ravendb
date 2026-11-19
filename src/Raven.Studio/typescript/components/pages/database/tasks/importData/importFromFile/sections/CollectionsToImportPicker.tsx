import React, { useEffect, useState } from "react";
import { useForm, useFormContext, useFormState, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import { Icon } from "components/common/Icon";
import { Switch } from "components/common/Checkbox";
import { FormInput } from "components/common/Form";
import InnerForm from "components/common/InnerForm";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

export default function CollectionsToImportPicker() {
    const t = useStudioTranslation("importFromFile");
    const { control: importControl, setValue, getValues } = useFormContext<ImportFromFileFormData>();

    const includedCollections = useWatch({ control: importControl, name: "collections.includedCollections" }) ?? [];

    const [manualCollections, setManualCollections] = useState<string[]>(
        () => getValues("collections.includedCollections") ?? []
    );
    const [addedCount, setAddedCount] = useState(0);

    const addCollectionForm = useForm({
        resolver: yupResolver(getAddCollectionSchema(manualCollections, t)),
        defaultValues: { collectionName: "" },
    });
    const { control: addControl, handleSubmit, setValue: setAddValue, clearErrors, setFocus } = addCollectionForm;

    const areAllSelected =
        manualCollections.length > 0 && manualCollections.every((name) => includedCollections.includes(name));

    const setIncludedCollections = (names: string[]) =>
        setValue("collections.includedCollections", names, { shouldDirty: true });

    const addCollection = ({ collectionName }: AddCollectionFormData) => {
        const trimmed = collectionName.trim();

        setManualCollections((prev) => [...prev, trimmed]);
        setIncludedCollections([...includedCollections, trimmed]);
        setAddedCount((prev) => prev + 1);
        setAddValue("collectionName", "");
        clearErrors("collectionName");
    };

    // a disabled input ignores focus(), so wait until the add form has finished submitting
    const { isSubmitting: isAddSubmitting } = useFormState({ control: addControl });

    useEffect(() => {
        if (isAddSubmitting) {
            return;
        }

        setFocus("collectionName");
    }, [addedCount, isAddSubmitting, setFocus]);

    const removeCollection = (name: string) => {
        setManualCollections((prev) => prev.filter((x) => x !== name));
        setIncludedCollections(includedCollections.filter((x) => x !== name));
    };

    const toggleCollection = (name: string, include: boolean) => {
        if (!include) {
            setIncludedCollections(includedCollections.filter((x) => x !== name));
            return;
        }

        setIncludedCollections([...includedCollections, name]);
    };

    const toggleAll = () => {
        setIncludedCollections(areAllSelected ? [] : [...manualCollections]);
    };

    return (
        <div className="mt-4">
            <InnerForm onSubmit={handleSubmit(addCollection)} className="mb-3">
                <InputGroup>
                    <FormInput
                        type="text"
                        control={addControl}
                        name="collectionName"
                        placeholder={t("collections.namePlaceholder")}
                    />
                    <Button variant="secondary" className="text-nowrap" onClick={handleSubmit(addCollection)}>
                        <Icon icon="plus" /> {t("collections.addButton")}
                    </Button>
                </InputGroup>
            </InnerForm>
            <div className="import-list-header mb-2">
                <span className="flex-grow-1 fw-semibold">{t("collections.nameHeader")}</span>
                <div className="d-flex align-items-center gap-2">
                    <span>{t("selectAll")}</span>
                    <Switch
                        id="select-all-collections"
                        className="m-0"
                        color="primary"
                        disabled={manualCollections.length === 0}
                        selected={areAllSelected}
                        toggleSelection={toggleAll}
                    />
                </div>
            </div>
            <div className="import-collections-list">
                {manualCollections.length === 0 && (
                    <div className="import-list-item text-muted">{t("collections.empty")}</div>
                )}
                {manualCollections.map((name) => (
                    <div key={name} className="import-list-item">
                        <Switch
                            id={`import-collection-${encodeURIComponent(name)}`}
                            color="primary"
                            selected={includedCollections.includes(name)}
                            toggleSelection={(e) => toggleCollection(name, e.target.checked)}
                        >
                            {name}
                        </Switch>
                        <Button
                            variant="link"
                            size="sm"
                            className="p-0 text-danger"
                            title={t("collections.removeTitle")}
                            onClick={() => removeCollection(name)}
                        >
                            <Icon icon="trash" margin="m-0" />
                        </Button>
                    </div>
                ))}
            </div>
        </div>
    );
}

interface AddCollectionFormData {
    collectionName: string;
}

const getAddCollectionSchema = (existingCollections: string[], t: StudioTranslate<"importFromFile">) =>
    yup.object({
        collectionName: yup
            .string()
            .trim()
            .required(t("collections.nameRequired"))
            .test("not-duplicate", t("collections.nameDuplicate"), (value) =>
                existingCollections.every((name) => name.toLowerCase() !== value?.toLowerCase())
            ),
    });
