import { FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { useAsyncCallback } from "react-async-hook";
import { FlexGrow } from "components/common/FlexGrow";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { SelectOption } from "components/common/select/Select";
import TemperatureField from "./TemperatureField";
import PromptCacheField from "./PromptCacheField";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AzureOpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const t = useStudioTranslation("connectionStrings");
    const { control, trigger } = useFormContext<ConnectionFormData<AiConnection>>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("azureOpenAiSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            ApiKey: formValues.azureOpenAiSettings.apiKey,
            Endpoint: formValues.azureOpenAiSettings.endpoint,
            EnablePromptCache: formValues.azureOpenAiSettings.enablePromptCache,
            Model: formValues.azureOpenAiSettings.model,
            DeploymentName: formValues.azureOpenAiSettings.deploymentName,
            Dimensions: formValues.azureOpenAiSettings.dimensions,
            Temperature: formValues.azureOpenAiSettings.isSetTemperature
                ? formValues.azureOpenAiSettings.temperature
                : null,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("AzureOpenAi", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "AzureOpenAi", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.azureOpenAiSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.azureOpenAiSettings.endpoint?.trim() ?? "";

            if (!apiKey || !endpoint) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "AzureOpenAi",
                AzureOpenAiSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [formValues.azureOpenAiSettings.apiKey, formValues.azureOpenAiSettings.endpoint],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.apiKeyLabel")}
                    <PopoverWithHoverWrapper message={t("ai.azureOpenAi.apiKeyInfo")}>
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>

                <FormInput control={control} name="azureOpenAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.endpointLabel")}
                    <PopoverWithHoverWrapper message={t("ai.azureOpenAi.endpointInfo")}>
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>

                <FormInput control={control} name="azureOpenAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.modelLabel")}
                    <PopoverWithHoverWrapper message={t("ai.azureOpenAi.modelInfo")}>
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="azureOpenAiSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder={t("ai.azureOpenAi.modelPlaceholder")}
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.azureOpenAi.deploymentNameLabel")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="ai.azureOpenAi.deploymentNameInfo"
                                components={{
                                    br: <br />,
                                    a: (
                                        <a href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource" />
                                    ),
                                }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" id="deploymentName" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="azureOpenAiSettings.deploymentName" type="text" />
            </div>
            {formValues.modelType === "TextEmbeddings" && (
                <div className="mb-2">
                    <FormLabel>
                        {t("ai.dimensionsLabel")} <OptionalLabel />
                        <PopoverWithHoverWrapper
                            message={
                                <StudioTrans
                                    ns="connectionStrings"
                                    i18nKey="ai.azureOpenAi.dimensionsInfo"
                                    components={{ br: <br /> }}
                                />
                            }
                        >
                            <Icon icon="info" color="info" id="dimensions" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="azureOpenAiSettings.dimensions"
                        type="number"
                        disabled={isUsedByAnyTask}
                    />
                </div>
            )}
            <PromptCacheField baseName="azureOpenAiSettings" serverDefaultValue={true} />
            <TemperatureField baseName="azureOpenAiSettings" />
            {formValues.modelType === "TextEmbeddings" && (
                <EmbeddingsMaxConcurrentBatches baseName="azureOpenAiSettings" />
            )}
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
