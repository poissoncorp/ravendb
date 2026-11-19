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
import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import PromptCacheField from "./PromptCacheField";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type FormData = ConnectionFormData<AiConnection>;

export default function AnthropicSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const t = useStudioTranslation("connectionStrings");
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("anthropicSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            ApiKey: formValues.anthropicSettings.apiKey,
            Model: formValues.anthropicSettings.model,
            Endpoint: formValues.anthropicSettings.endpoint,
            MaxOutputTokens: formValues.anthropicSettings.maxOutputTokens,
            ReasoningEffort: formValues.anthropicSettings.reasoningEffort,
            EnablePromptCache: formValues.anthropicSettings.enablePromptCache,
            ApiVersion: formValues.anthropicSettings.apiVersion,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("Anthropic", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "Anthropic", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const apiKey = formValues.anthropicSettings.apiKey?.trim() ?? "";
            const endpoint = formValues.anthropicSettings.endpoint?.trim() ?? "";

            if (!apiKey) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "Anthropic",
                AnthropicSettings: {
                    ApiKey: apiKey,
                    Endpoint: endpoint,
                    ApiVersion: formValues.anthropicSettings.apiVersion,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return result.map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [
            formValues.anthropicSettings.apiKey,
            formValues.anthropicSettings.endpoint,
            formValues.anthropicSettings.apiVersion,
        ],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.apiKeyLabel")}
                    <PopoverWithHoverWrapper message={t("ai.anthropic.apiKeyInfo")}>
                        <Icon icon="info" color="info" id="apiKey" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="anthropicSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.endpointLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={t("ai.anthropic.endpointInfo", { url: "https://api.anthropic.com/v1/" })}
                    >
                        <Icon icon="info" color="info" id="endpoint" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="anthropicSettings.endpoint"
                    placeholder={t("ai.endpointPlaceholder")}
                    options={endpointOptions}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.modelLabel")}
                    <PopoverWithHoverWrapper message={t("ai.anthropic.modelInfo")}>
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="anthropicSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder={t("ai.modelPlaceholder")}
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.anthropic.maxOutputTokensLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="ai.anthropic.maxOutputTokensInfo"
                                options={{ value: defaultMaxOutputTokens }}
                                components={{ code: <code /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" id="maxOutputTokens" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name="anthropicSettings.maxOutputTokens"
                    type="number"
                    placeholder={t("ai.defaultPlaceholder", { value: defaultMaxOutputTokens })}
                />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.reasoningEffortLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>{t("ai.anthropic.reasoningEffortInfo")}</p>
                                <p>
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.anthropic.reasoningEffortValues"
                                        components={{ code: <code /> }}
                                    />
                                </p>
                                <p className="mb-0">{t("ai.anthropic.reasoningEffortNotValidated")}</p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" id="reasoningEffort" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="anthropicSettings.reasoningEffort"
                    placeholder={t("ai.reasoningEffortPlaceholder")}
                    options={reasoningEffortOptions}
                    isClearable
                />
            </div>
            <PromptCacheField
                baseName="anthropicSettings"
                serverDefaultValue={true}
                label={t("ai.anthropic.promptCacheLabel")}
                description={
                    <>
                        {t("ai.anthropic.promptCacheInfo")}
                        <br />
                        <br />
                        <ul className="mb-0">
                            <li>
                                <StudioTrans
                                    ns="connectionStrings"
                                    i18nKey="ai.anthropic.promptCacheDefault"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="connectionStrings"
                                    i18nKey="ai.anthropic.promptCacheTrue"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="connectionStrings"
                                    i18nKey="ai.anthropic.promptCacheFalse"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                        </ul>
                    </>
                }
            />
            <div className="mb-2">
                <FormLabel>
                    {t("ai.anthropic.apiVersionLabel")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="ai.anthropic.apiVersionInfo"
                                options={{ value: defaultApiVersion }}
                                components={{ code: <code /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" id="apiVersion" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name="anthropicSettings.apiVersion"
                    type="text"
                    placeholder={t("ai.defaultPlaceholder", { value: defaultApiVersion })}
                />
            </div>
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

const defaultMaxOutputTokens = 16000;

const defaultApiVersion = "2023-06-01";

const endpointOptions: SelectOption[] = ["https://api.anthropic.com/v1/"].map((x) => ({ label: x, value: x }));

const reasoningEffortOptions: SelectOption[] = ["low", "medium", "high", "xhigh", "max"].map((x) => ({
    label: x,
    value: x,
}));
