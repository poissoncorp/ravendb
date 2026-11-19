import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { FormLabel } from "components/common/Form";
import ClickableCard from "components/common/ClickableCard";
import { useAppSelector } from "components/store";
import { connectionStringSelectors } from "../../store/connectionStringsSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type FormData = ConnectionFormData<AiConnection>;

interface ModelTypeFieldProps {
    initialModelType: FormData["modelType"];
}

export default function ModelTypeField({ initialModelType }: ModelTypeFieldProps) {
    const t = useStudioTranslation("connectionStrings");
    const { control, setValue } = useFormContext<FormData>();

    const formValues = useWatch({ control });

    const viewContext = useAppSelector(connectionStringSelectors.viewContext);

    const isChatVisible = viewContext !== "task" || initialModelType !== "TextEmbeddings";
    const isTextEmbeddingsVisible = viewContext !== "task" || initialModelType !== "Chat";
    const isAllVisible = isChatVisible && isTextEmbeddingsVisible;

    return (
        <div className="mb-2">
            <FormLabel>
                {t("ai.modelType.label")}
                <PopoverWithHoverWrapper message={t("ai.modelType.info")}>
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <div className="d-flex gap-2">
                {isChatVisible && (
                    <ClickableCard
                        icon="llm"
                        title={t("ai.modelType.chat")}
                        description={t("ai.modelType.chatDescription")}
                        className={isAllVisible ? "w-50" : "w-100"}
                        isSelected={formValues.modelType === "Chat"}
                        onClick={() => setValue("modelType", "Chat")}
                    />
                )}
                {isTextEmbeddingsVisible && (
                    <ClickableCard
                        icon="document2"
                        title={t("ai.modelType.textEmbeddings")}
                        description={t("ai.modelType.textEmbeddingsDescription")}
                        className={isAllVisible ? "w-50" : "w-100"}
                        isSelected={formValues.modelType === "TextEmbeddings"}
                        onClick={() => setValue("modelType", "TextEmbeddings")}
                    />
                )}
            </div>
        </div>
    );
}
