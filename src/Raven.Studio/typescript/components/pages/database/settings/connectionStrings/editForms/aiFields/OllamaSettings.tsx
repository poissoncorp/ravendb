import { FlexGrow } from "components/common/FlexGrow";
import { FormInput, FormLabel, FormSelect, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useFormContext, useWatch } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EmbeddingsMaxConcurrentBatches from "./EmbeddingsMaxConcurrentBatchesField";
import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import TemperatureField from "./TemperatureField";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<AiConnection>;

export default function OllamaSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const t = useStudioTranslation("connectionStrings");
    const { control, trigger } = useFormContext<FormData>();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const formValues = useWatch({ control });

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("ollamaSettings");
        if (!isValid) {
            return;
        }

        const settings = {
            Model: formValues.ollamaSettings.model,
            Uri: formValues.ollamaSettings.uri,
            Think: formValues.ollamaSettings.think,
            Temperature: formValues.ollamaSettings.isSetTemperature ? formValues.ollamaSettings.temperature : null,
        };
        return isServerWide
            ? tasksService.testServerWideAiConnectionString("Ollama", formValues.modelType, settings)
            : tasksService.testAiConnectionString(databaseName, "Ollama", formValues.modelType, settings);
    });

    const asyncGetModelOptions = useAsyncDebounce(
        async () => {
            const uri = formValues.ollamaSettings.uri?.trim() ?? "";

            if (!uri) {
                return [];
            }

            const dto: AiModelsRequestDto = {
                ConnectorType: "Ollama",
                OllamaSettings: {
                    Uri: uri,
                    Think: formValues.ollamaSettings.think,
                },
            };

            try {
                const result = await tasksService.getAiModels(dto);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [formValues.ollamaSettings.uri, formValues.ollamaSettings.think],
        300
    );

    return (
        <>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.ollama.uriLabel")}
                    <PopoverWithHoverWrapper message={t("ai.ollama.uriInfo")}>
                        <Icon icon="info" color="info" id="uri" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput control={control} name="ollamaSettings.uri" type="text" />
            </div>
            <div className="mb-2">
                <FormLabel>
                    {t("ai.modelLabel")}
                    <PopoverWithHoverWrapper message={t("ai.ollama.modelInfo")}>
                        <Icon icon="info" color="info" id="model" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="ollamaSettings.model"
                    isDisabled={isUsedByAnyTask}
                    placeholder={t("ai.ollama.modelPlaceholder")}
                    options={asyncGetModelOptions.result ?? []}
                    isLoading={asyncGetModelOptions.loading}
                />
            </div>

            {formValues.modelType === "Chat" && (
                <div className="mb-2">
                    <FormLabel>
                        {t("ai.ollama.thinkingModeLabel")}
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    {t("ai.ollama.thinkingModeInfo")}
                                    <ul className="mb-1">
                                        <li className="mt-1">
                                            <StudioTrans
                                                ns="connectionStrings"
                                                i18nKey="ai.ollama.thinkingModeEnabled"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                        <li className="mt-1">
                                            <StudioTrans
                                                ns="connectionStrings"
                                                i18nKey="ai.ollama.thinkingModeDisabled"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                        <li className="mt-1">
                                            <StudioTrans
                                                ns="connectionStrings"
                                                i18nKey="ai.ollama.thinkingModeDefault"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                    </ul>
                                    <p>{t("ai.ollama.thinkingModeTradeOff")}</p>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormSelect control={control} name="ollamaSettings.think" options={getThinkOptions(t)} />
                </div>
            )}
            <TemperatureField baseName="ollamaSettings" />
            {formValues.modelType === "TextEmbeddings" && <EmbeddingsMaxConcurrentBatches baseName="ollamaSettings" />}
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

function getThinkOptions(t: StudioTranslate<"connectionStrings">): SelectOption<boolean>[] {
    return [
        { label: t("ai.ollama.thinkDefault"), value: null },
        { label: t("ai.ollama.thinkEnabled"), value: true },
        { label: t("ai.ollama.thinkDisabled"), value: false },
    ];
}
