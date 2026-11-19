import { StepItem } from "components/common/steps/Steps";
import { StepInRangeValidationResult } from "components/common/steps/useSteps";
import { CreateDatabaseFromBackupFormData } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { CreateDatabaseRegularFormData } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularValidation";
import moment from "moment";
import { UseAsyncReturn } from "react-async-hook";
import { Path, UseFormTrigger } from "react-hook-form";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("databases");

function getEncryptionData(databaseName: string, encryptionKey: string) {
    const encryptionKeyFileName = `Key-of-${databaseName}-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;

    const encryptionKeyText = t("createDatabase.encryptionKeyText", { databaseName, encryptionKey });

    return {
        encryptionKeyFileName,
        encryptionKeyText,
    };
}

type FormData = CreateDatabaseFromBackupFormData | CreateDatabaseRegularFormData;

export interface CreateDatabaseStep<T extends FormData> {
    id: keyof T;
    label: string;
    active: boolean;
    isInvalid?: boolean;
    isLoading?: boolean;
}

interface GetStepValidationProps<T extends FormData> {
    stepId: Path<T>;
    trigger: UseFormTrigger<T>;
    asyncDatabaseNameValidation: UseAsyncReturn<boolean, []>;
}

function getStepValidation<T extends FormData>({
    stepId,
    trigger,
    asyncDatabaseNameValidation,
}: GetStepValidationProps<T>) {
    if (stepId === "basicInfoStep") {
        return async () => (await asyncDatabaseNameValidation.execute()) && (await trigger(stepId));
    }

    return async () => await trigger(stepId);
}

interface GetStepInRangeValidationProps<T extends FormData> {
    currentStep: number;
    targetStep: number;
    activeStepIds: Extract<Path<T>, CreateDatabaseStep<T>["id"]>[];
    trigger: UseFormTrigger<T>;
    asyncDatabaseNameValidation: UseAsyncReturn<boolean, []>;
}

function getStepInRangeValidation<T extends FormData>({
    currentStep,
    targetStep,
    activeStepIds,
    trigger,
    asyncDatabaseNameValidation,
}: GetStepInRangeValidationProps<T>) {
    return async function (): Promise<StepInRangeValidationResult> {
        for (let i = currentStep; i <= targetStep; i++) {
            const stepValidation = createDatabaseUtils.getStepValidation<T>({
                stepId: activeStepIds[i],
                trigger,
                asyncDatabaseNameValidation,
            });

            const isValid = await stepValidation();
            if (!isValid) {
                return {
                    isValid: false,
                    invalidStep: i,
                };
            }
        }

        return {
            isValid: true,
        };
    };
}

function mapToStepItem<T extends FormData>(step: CreateDatabaseStep<T>): StepItem {
    return {
        label: step.label,
        isInvalid: step.isInvalid,
        isLoading: step.isLoading,
    };
}

export const createDatabaseUtils = {
    getEncryptionData,
    getStepValidation,
    getStepInRangeValidation,
    mapToStepItem,
    get encryptionKeyNotSavedMessage() {
        return t("createDatabase.encryptionKeyNotSaved");
    },
};
