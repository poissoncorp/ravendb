import SizeGetter from "components/common/SizeGetter";
import { FormLabel } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import AllRevisionsWithSize from "components/pages/database/documents/allRevisions/partials/AllRevisionsWithSize";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import {
    OptionWithCount,
    SelectOptionWithCount,
    SingleValueWithCount,
} from "components/pages/database/documents/allRevisions/partials/AllRevisionsSelectComponents";
import useAllRevisionsFilters from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsFilters";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { useEffect, useRef, useState } from "react";
import useConfirm from "components/common/ConfirmDialog";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import messagePublisher from "common/messagePublisher";
import { AllRevisionsFetcherRef } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import { HStack } from "components/common/utilities/HStack";
import { VStack } from "components/common/utilities/VStack";
import AllRevisionsAboutView from "components/pages/database/documents/allRevisions/partials/AllRevisionsAboutView";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { FlexGrow } from "components/common/FlexGrow";
import classNames from "classnames";
import { Switch } from "components/common/Checkbox";
import useBoolean from "components/hooks/useBoolean";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export default function AllRevisions() {
    const t = useStudioTranslation("allRevisions");
    const commonT = useStudioTranslation("common");
    const { type, collection, reload: reloadOptions } = useAllRevisionsFilters();
    const [selectedRows, setSelectedRows] = useState<RevisionsPreviewResultItem[]>([]);

    const fetcherRef = useRef<AllRevisionsFetcherRef>(null);

    const confirm = useConfirm();
    const { databasesService } = useServices();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;

    const { value: isPaginated, setValue: setIsPaginated, toggle: toggleIsPaginated } = useBoolean(false);
    const isSmallSample = allRevisionsUtils.isSmallSample(type.value, collection.value);
    const isPaginationAvailable = !isSharded && !isSmallSample;

    // Reset selected rows when filters change
    useEffect(() => {
        setSelectedRows([]);
    }, [type.value, collection.value]);

    const asyncRemoveRevisions = useAsyncCallback(async () => {
        const uniqueIds = Array.from(new Set(selectedRows.map((x) => x.Id)));

        for (const id of uniqueIds) {
            await databasesService.deleteRevisionsForDocuments(activeDatabaseName, {
                DocumentIds: [id],
                RevisionsChangeVectors: selectedRows.filter((x) => x.Id === id).map((x) => x.ChangeVector),
                RemoveForceCreatedRevisions: true,
            });
        }

        messagePublisher.reportSuccess(t("removeSuccess", { count: selectedRows.length }));
        setSelectedRows([]);
        await reloadOptions();

        collectionsTracker.default
            .getAllRevisionsCollection()
            .documentCount(
                collectionsTracker.default.getAllRevisionsCollection().documentCount() - selectedRows.length
            );
    });

    const handleRemoveConfirmation = async () => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    <StudioTrans
                        ns="allRevisions"
                        i18nKey="deleteConfirm.title"
                        components={{ strong: <strong /> }}
                        options={{ count: selectedRows.length }}
                    />
                </span>
            ),
            icon: "trash",
            actionColor: "danger",
            confirmText: commonT("delete"),
            message: (
                <RichAlert variant="warning">
                    <StudioTrans ns="allRevisions" i18nKey="deleteConfirm.message" components={{ br: <br /> }} />
                </RichAlert>
            ),
        });

        if (isConfirmed) {
            await asyncRemoveRevisions.execute();
            await fetcherRef.current?.reload();
        }
    };

    return (
        <VStack className="content-padding" gap={2}>
            <VStack>
                {hasDatabaseAdminAccess && (
                    <HStack className="justify-content-between">
                        <ButtonWithSpinner
                            variant="danger"
                            onClick={handleRemoveConfirmation}
                            disabled={selectedRows.length === 0}
                            isSpinning={asyncRemoveRevisions.loading}
                            icon="trash"
                            className="w-fit-content rounded-pill"
                        >
                            {selectedRows.length != 0
                                ? t("removeSelectedButton", { count: selectedRows.length })
                                : t("removeButton")}
                        </ButtonWithSpinner>
                        <AllRevisionsAboutView />
                    </HStack>
                )}
                <HStack gap={2} className={classNames("align-items-end", { "my-3": hasDatabaseAdminAccess })}>
                    <div>
                        <FormLabel className="small-label">{t("filters.collectionLabel")}</FormLabel>
                        <SelectCreatable
                            options={collection.options}
                            isLoading={collection.isLoading}
                            placeholder={t("filters.collectionPlaceholder")}
                            value={collection.options.find((x) => x.value === collection.value)}
                            onChange={(x: SelectOptionWithCount<string>) => collection.setValue(x?.value ?? "")}
                            isClearable
                            components={{ Option: OptionWithCount, SingleValue: SingleValueWithCount }}
                            isRoundedPill
                        />
                    </div>
                    <div>
                        <FormLabel className="small-label">{t("filters.typeLabel")}</FormLabel>
                        <MultiRadioToggle<RevisionType>
                            inputItems={type.options}
                            selectedItem={type.value}
                            setSelectedItem={type.setValue}
                        />
                    </div>
                    <FlexGrow />
                    {isPaginationAvailable && (
                        <Switch
                            selected={isPaginated}
                            toggleSelection={toggleIsPaginated}
                            color="primary"
                            title={t("pagination.title")}
                            className="mb-1"
                        >
                            {t("pagination.label")}
                        </Switch>
                    )}
                    {!hasDatabaseAdminAccess && <AllRevisionsAboutView />}
                </HStack>
            </VStack>
            {isSmallSample && (
                <RichAlert variant="warning">
                    {t("smallSampleWarning", { count: allRevisionsUtils.smallSampleSize })}
                </RichAlert>
            )}
            <SizeGetter
                isHeighRequired
                render={({ width, height }) => (
                    <AllRevisionsWithSize
                        width={width}
                        height={height}
                        selectedType={type.value}
                        selectedCollectionName={collection.value}
                        fetcherRef={fetcherRef}
                        selectedRows={selectedRows}
                        setSelectedRows={setSelectedRows}
                        isPaginated={isPaginated}
                        onIsPaginatedChange={setIsPaginated}
                    />
                )}
            />
        </VStack>
    );
}
