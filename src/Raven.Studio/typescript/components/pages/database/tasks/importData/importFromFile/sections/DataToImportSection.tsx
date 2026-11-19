import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { documentToggleKeys, ImportFromFileFormData } from "../importFromFileValidation";
import { getItemsToWarnAbout } from "../importFromFileUtils";
import { getLicenseLimitWarning } from "../importLicenseLimits";
import { useImportRestrictions } from "../useImportRestrictions";
import RestrictedSwitch from "./RestrictedSwitch";
import CollectionsToImportPicker from "./CollectionsToImportPicker";
import Card from "react-bootstrap/Card";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function DataToImportSection() {
    const t = useStudioTranslation("importFromFile");
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const documents = useWatch({ control, name: "documents" });
    const { documentToggles: documentToggleRestrictions } = useImportRestrictions();
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const subscriptionsLimitWarning = getLicenseLimitWarning(licenseStatus, "subscriptions");

    const isImportAll = useWatch({ control, name: "collections.isImportAllCollections" });

    const itemsToWarnAbout = getItemsToWarnAbout({ documents });

    const forceDocumentsOn = (value: boolean) => {
        if (value) {
            setValue("documents.isIncludeDocuments", true, { shouldDirty: true });
        }
    };

    const selectableDocumentToggleNames = documentToggleKeys.filter((name) => !documentToggleRestrictions[name]);

    const areAllDocumentsSelected = selectableDocumentToggleNames.every((name) => documents[name]);

    const setAllDocuments = (value: boolean) => {
        selectableDocumentToggleNames.forEach((name) => setValue(`documents.${name}`, value, { shouldDirty: true }));
    };

    return (
        <ImportSection id="data-to-import" title={t("sections.dataToImport")}>
            <h4 id="collections-to-import" className="mb-2">
                {t("data.chooseCollections")}
            </h4>
            <Card className="mb-4 p-4">
                <div className="d-flex gap-2">
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: isImportAll })}
                        onClick={() => setValue("collections.isImportAllCollections", true, { shouldDirty: true })}
                    >
                        <Icon icon="documents" margin="m-0" />
                        {t("data.importAllCollections")}
                    </button>
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: !isImportAll })}
                        onClick={() => setValue("collections.isImportAllCollections", false, { shouldDirty: true })}
                    >
                        <Icon icon="document-group" addon="edit" margin="m-0" />
                        {t("data.customizeCollections")}
                    </button>
                </div>
                {!isImportAll && <CollectionsToImportPicker />}
            </Card>

            <div className="d-flex justify-content-between align-items-center mb-2">
                <h4 id="documents-and-extensions" className="m-0">
                    {t("data.selectDocuments")}
                </h4>
                <Button variant="link" size="sm" onClick={() => setAllDocuments(!areAllDocumentsSelected)}>
                    {areAllDocumentsSelected ? t("deselectAll") : t("selectAll")}
                </Button>
            </div>
            <Card className="p-4">
                <FormSwitch control={control} name="documents.isIncludeDocuments" className="pb-1">
                    {t("data.includeDocuments")}
                </FormSwitch>
                <div className="ms-4 d-flex flex-column gap-1">
                    <FormSwitch control={control} name="documents.isIncludeAttachments" afterChange={forceDocumentsOn}>
                        {t("data.includeAttachments")}
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeCounters" afterChange={forceDocumentsOn}>
                        {t("data.includeCounters")}
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeRevisions" afterChange={forceDocumentsOn}>
                        {t("data.includeRevisions")}
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeTimeSeries" afterChange={forceDocumentsOn}>
                        {t("data.includeTimeSeries")}
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeTimeSeriesDeletedRanges">
                        {t("data.includeTimeSeriesDeletedRanges")}
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeLegacyAttachments">
                        {t("data.includeLegacyAttachments")}
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeDocumentsTombstones">
                        {t("data.includeDocumentsTombstones")}
                    </FormSwitch>
                </div>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeArtificialDocuments">
                    {t("data.includeArtificialDocuments")}{" "}
                    <PopoverWithHoverWrapper message={t("data.artificialDocumentsInfo")}>
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <hr className="my-1" />
                <RestrictedSwitch
                    control={control}
                    name="documents.isIncludeArchivedDocuments"
                    restriction={documentToggleRestrictions.isIncludeArchivedDocuments}
                >
                    {t("data.includeArchivedDocuments")}
                </RestrictedSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeExpiredDocuments">
                    {t("data.includeExpiredDocuments")}
                </FormSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeConflicts">
                    {t("data.includeConflicts")}
                </FormSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeCompareExchange">
                    {t("data.includeCompareExchange")}
                </FormSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeCompareExchangeTombstones">
                    {t("data.includeCompareExchangeTombstones")}
                </FormSwitch>
                <hr className="my-1" />
                <RestrictedSwitch
                    control={control}
                    name="documents.isIncludeSubscriptions"
                    warning={subscriptionsLimitWarning}
                >
                    {t("data.includeSubscriptions")}
                </RestrictedSwitch>

                {itemsToWarnAbout.length > 0 && (
                    <Alert variant="warning" className="mt-3 mb-0">
                        <Icon icon="warning" color="warning" />{" "}
                        {t("data.withoutDocumentsWarning", { items: itemsToWarnAbout.join(", ") })}
                    </Alert>
                )}
            </Card>
        </ImportSection>
    );
}
