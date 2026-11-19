import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
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
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import TemperatureField from "./TemperatureField";
import PromptCacheField from "./PromptCacheField";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type FormData = ConnectionFormData<AiConnection>;

export default function OpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const t = useStudioTranslation("connectionStrings");
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("openAiSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            ApiKey: formValues.openAiSettings.apiKey,
            Endpoint: formValues.openAiSettings.endpoint,
            EnablePromptCache: formValues.openAiSettings.enablePromptCache,
            Model: formValues.openAiSettings.model,
            OrganizationId: formValues.openAiSettings.organizationId,
            ProjectId: formValues.openAiSettings.projectId,
            ReasoningEffort: formValues.modelType === "Chat" ? formValues.openAiSettings.reasoningEffort : null,
            Temperature: formValues.openAiSettings.isSetTemperature ? formValues.openAiSettings.temperature : null,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("OpenAi", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "OpenAi", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.openAiSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.openAiSettings.endpoint?.trim() ?? "";
            const projectId = formValues.openAiSettings.projectId?.trim() ?? "";
            const organizationId = formValues.openAiSettings.organizationId?.trim() ?? "";

            if (!apiKey) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "OpenAi",
                OpenAiSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint,
                    OrganizationId: organizationId,
                    ProjectId: projectId,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [
            formValues.openAiSettings.apiKey,
            formValues.openAiSettings.endpoint,
            formValues.openAiSettings.organizationId,
            formValues.openAiSettings.projectId,
        ],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.apiKeyLabel")}
                    <PopoverWithHoverWrapper message={t("ai.openAi.apiKeyInfo")}>
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="openAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.endpointLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper message={t("ai.openAi.endpointInfo")}>
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="openAiSettings.endpoint"
                    placeholder={t("ai.endpointPlaceholder")}
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.modelLabel")}
                    <PopoverWithHoverWrapper message={t("ai.openAi.modelInfo")}>
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="openAiSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder={t("ai.modelPlaceholder")}
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.openAi.organizationIdLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.openAi.organizationIdHeader"
                                        components={{ code: <code /> }}
                                    />
                                </p>
                                <p>{t("ai.openAi.organizationIdUsage")}</p>
                                <p>
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.openAi.organizationIdNotSet"
                                        components={{
                                            br: <br />,
                                            a: (
                                                <a href="https://platform.openai.com/docs/guides/production-best-practices/setting-up-your-organization" />
                                            ),
                                        }}
                                    />
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="organizationId" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="openAiSettings.organizationId" type="text" />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.openAi.projectIdLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.openAi.projectIdHeader"
                                        components={{ code: <code /> }}
                                    />
                                </p>
                                <p>{t("ai.openAi.projectIdUsage")}</p>
                                <p>{t("ai.openAi.projectIdNotSet")}</p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="projectId" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="openAiSettings.projectId" type="text" />
            </div>
            {formValues.modelType === "Chat" && (
                <div className="mb-2">
                    <FormLabel>
                        {t("ai.reasoningEffortLabel")} <OptionalLabel />
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <p>
                                        <StudioTrans
                                            ns="connectionStrings"
                                            i18nKey="ai.openAi.reasoningEffortInfo"
                                            components={{ code: <code /> }}
                                        />
                                    </p>
                                    <p>
                                        <StudioTrans
                                            ns="connectionStrings"
                                            i18nKey="ai.openAi.reasoningEffortValues"
                                            components={{ code: <code /> }}
                                        />
                                    </p>
                                    <p>{t("ai.openAi.reasoningEffortNotValidated")}</p>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" id="reasoningEffort" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name="openAiSettings.reasoningEffort"
                        placeholder={t("ai.reasoningEffortPlaceholder")}
                        options={reasoningEffortOptions}
                        isClearable
                    />
                </div>
            )}
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
                        name="openAiSettings.dimensions"
                        type="number"
                        disabled={isUsedByAnyTask}
                    />
                </div>
            )}
            <PromptCacheField baseName="openAiSettings" serverDefaultValue={true} />
            <TemperatureField baseName="openAiSettings" />
            {formValues.modelType === "TextEmbeddings" && <EmbeddingsMaxConcurrentBatches baseName="openAiSettings" />}
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

const endpointOptions: SelectOption[] = ["https://api.openai.com/v1/"].map((x) => ({ label: x, value: x }));

const reasoningEffortOptions: SelectOption[] = ["none", "minimal", "low", "medium", "high", "xhigh", "max"].map(
    (x) => ({ label: x, value: x })
);
