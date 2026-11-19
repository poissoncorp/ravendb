import { FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { FormGroup } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SelectOption } from "components/common/select/Select";
import { useRef } from "react";
import ReactAce from "react-ace/lib/ace";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import AceEditor from "components/common/ace/AceEditor";
import { contextScriptSamplesTabs } from "../../editGenAiTaskSamplesData";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditGenAiTaskContextFields() {
    const t = useStudioTranslation("editGenAiTask");
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    const scriptRef = useRef<ReactAce>(null);

    return (
        <>
            <FormGroup>
                <FormLabel>
                    {t("context.sourceCollection")}
                    <PopoverWithHoverWrapper message={t("context.sourceCollectionInfo")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete control={control} name="collectionName" options={collectionOptions} />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    {t("context.script")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editGenAiTask"
                                i18nKey="context.scriptInfo"
                                components={{ code: <code />, strong: <strong />, br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    aceRef={scriptRef}
                    control={control}
                    name="script"
                    mode="javascript"
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: (
                                <AceEditor.LoadFileAction
                                    onLoad={(value) => setValue("script", value, { shouldValidate: true })}
                                />
                            ),
                        },
                    ]}
                    samplesPanel={{ tabs: contextScriptSamplesTabs }}
                />
            </FormGroup>
        </>
    );
}
