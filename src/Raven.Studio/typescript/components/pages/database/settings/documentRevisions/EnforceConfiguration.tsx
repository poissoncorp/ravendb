import React from "react";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import * as yup from "yup";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";
import { FormGroup, FormInput, FormSwitch, FormLabel } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EnforceConfigurationProps {
    toggle: () => void;
    onConfirm: (includeForceCreated: boolean, collections: string[], maxOpsPerSecond: number | null) => Promise<void>;
}

export default function EnforceConfiguration(props: EnforceConfigurationProps) {
    const { toggle, onConfirm } = props;
    const t = useStudioTranslation("documentRevisions");
    const commonT = useStudioTranslation("common");

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const { control, formState, setValue, handleSubmit } = useForm<FormData>({
        resolver: formResolver,
        defaultValues: {
            isIncludeForceCreated: false,
            isAllCollections: false,
            collections: [],
            maxOpsPerSecond: null,
        },
    });

    const { isAllCollections, collections } = useWatch({ control });

    const onEnforce: SubmitHandler<FormData> = (formData) => {
        const formCollections = formData.isAllCollections ? null : formData.collections;

        onConfirm(formData.isIncludeForceCreated, formCollections, formData.maxOpsPerSecond);
        toggle();
    };

    return (
        <Modal show onHide={toggle} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Body>
                <Form id="enforce-configuration" className="vstack gap-2" onSubmit={handleSubmit(onEnforce)}>
                    <FormCollectionsSelect
                        control={control}
                        collectionsFormName="collections"
                        collections={collections}
                        isAllCollectionsFormName="isAllCollections"
                        isAllCollections={isAllCollections}
                        allCollectionNames={allCollectionNames}
                        setValue={setValue}
                        isCreatable={false}
                    />
                    <FormSwitch
                        control={control}
                        name="isIncludeForceCreated"
                        className="mt-2"
                        disabled={formState.isSubmitting}
                    >
                        {t("enforceModal.includeForceCreated")}
                    </FormSwitch>
                    <FormGroup marginClass="m-0">
                        <FormLabel>
                            {t("enforceModal.maxOpsLabel")}
                            <PopoverWithHoverWrapper message={t("enforceModal.maxOpsInfo")}>
                                <Icon icon="info-new" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </FormLabel>
                        <FormInput
                            type="number"
                            placeholder={t("enforceModal.maxOpsPlaceholder")}
                            control={control}
                            name="maxOpsPerSecond"
                        />
                    </FormGroup>
                    <hr className="my-1" />
                    <p>
                        <StudioTrans
                            ns="documentRevisions"
                            i18nKey="enforceModal.description"
                            components={{ strong: <strong /> }}
                        />
                    </p>
                    <p>{t("enforceModal.removalNote")}</p>
                    <RichAlert variant="warning">
                        <p>{t("enforceModal.noSpecificConfigIntro")}</p>
                        <ul>
                            <li>
                                <StudioTrans
                                    ns="documentRevisions"
                                    i18nKey="enforceModal.nonConflictingItem"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </li>
                            <li className="mt-3">
                                <StudioTrans
                                    ns="documentRevisions"
                                    i18nKey="enforceModal.conflictingItem"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </li>
                        </ul>
                    </RichAlert>
                </Form>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={toggle}>
                    {commonT("cancel")}
                </Button>
                <Button form="enforce-configuration" type="submit" variant="warning">
                    <Icon icon="rocket" />
                    {t("enforceModal.submit")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

const schema = yup.object({
    isIncludeForceCreated: yup.boolean(),
    isAllCollections: yup.boolean(),
    collections: yup
        .array()
        .of(yup.string())
        .when("isAllCollections", {
            is: false,
            then: (schema) => schema.min(1),
        }),
    maxOpsPerSecond: yup.number().nullable().integer().min(1),
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;
