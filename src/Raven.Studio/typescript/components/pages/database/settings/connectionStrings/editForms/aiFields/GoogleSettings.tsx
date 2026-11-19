import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormSelect, FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useFormContext, useWatch } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import RichAlert from "components/common/RichAlert";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import PromptCacheField from "./PromptCacheField";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type FormData = ConnectionFormData<AiConnection>;

export default function GoogleSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const t = useStudioTranslation("connectionStrings");
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("googleSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            AiVersion: formValues.googleSettings.aiVersion,
            ApiKey: formValues.googleSettings.apiKey,
            EnablePromptCache: formValues.googleSettings.enablePromptCache,
            Model: formValues.googleSettings.model,
            Endpoint: formValues.googleSettings.endpoint,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("Google", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "Google", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.googleSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.googleSettings.endpoint?.trim() ?? "";

            if (!apiKey) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "Google",
                GoogleSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint,
                    AiVersion: formValues.googleSettings.aiVersion,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return result
                    .map((x) => x.replace(/^models\//, ""))
                    .sort()
                    .map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [formValues.googleSettings.apiKey, formValues.googleSettings.endpoint, formValues.googleSettings.aiVersion],
        300
    );

    return (
        <>
            <RichAlert variant="info">{t("ai.google.compatibility")}</RichAlert>
            <div className="mb-2">
                <FormLabel className="col-form-label">
                    {t("ai.aiVersionLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper message={t("ai.google.aiVersionInfo")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name="googleSettings.aiVersion"
                    options={
                        [
                            { label: "V1", value: "V1" },
                            { label: "V1_Beta", value: "V1_Beta" },
                        ] satisfies SelectOption<FormData["googleSettings"]["aiVersion"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                    isClearable
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.apiKeyLabel")}
                    <PopoverWithHoverWrapper message={t("ai.google.apiKeyInfo")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>

                <FormInput control={control} name="googleSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.endpointLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper message={t("ai.google.endpointInfo")}>
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="googleSettings.endpoint"
                    placeholder={t("ai.endpointPlaceholder")}
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.modelLabel")}
                    <PopoverWithHoverWrapper message={t("ai.google.modelInfo")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="googleSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder={t("ai.modelPlaceholder")}
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <PromptCacheField baseName="googleSettings" serverDefaultValue={false} />
            {formValues.modelType === "TextEmbeddings" && (
                <div className="mb-2">
                    <FormLabel>
                        {t("ai.dimensionsLabel")} <OptionalLabel />
                        <PopoverWithHoverWrapper message={t("ai.dimensionsInfo")}>
                            <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="googleSettings.dimensions"
                        type="number"
                        disabled={isUsedByAnyTask}
                    />
                </div>
            )}
            {formValues.modelType === "TextEmbeddings" && <EmbeddingsMaxConcurrentBatches baseName="googleSettings" />}
            <div className="d-flex mb-2">
                <FlexGrow />
                <ButtonWithSpinner
                    variant="secondary"
                    icon="rocket"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                >
                    {t("form.testConnection")}
                </ButtonWithSpinner>
            </div>
            {asyncTest.result && <ConnectionTestResult testResult={asyncTest.result} />}
        </>
    );
}

const endpointOptions: SelectOption[] = ["https://generativelanguage.googleapis.com"].map((x) => ({
    label: x,
    value: x,
}));
