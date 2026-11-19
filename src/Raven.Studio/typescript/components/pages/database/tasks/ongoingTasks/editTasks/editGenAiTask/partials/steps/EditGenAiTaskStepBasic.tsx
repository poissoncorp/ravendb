import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import EditGenAiTaskBasicFields from "../fields/EditGenAiTaskBasicFields";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { aiConnectionStringUtils } from "components/pages/database/settings/connectionStrings/editForms/aiConnectionStringUtils";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { mapAiConnectionStringToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditGenAiTaskStepBasic() {
    const t = useStudioTranslation("editGenAiTask");
    const hasGenAi = useAppSelector(licenseSelectors.statusValue("HasGenAi"));

    const connectionStringTest = useAppSelector(editGenAiTaskSelectors.connectionStringTest);

    return (
        <div>
            <div className="hstack justify-content-between">
                <AboutViewHeading
                    title={t("basic.title")}
                    marginBottom={2}
                    icon="genai"
                    licenseBadgeText={hasGenAi ? null : "Enterprise AI"}
                />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                <StudioTrans ns="editGenAiTask" i18nKey="basic.intro" components={{ br: <br /> }} />
            </p>
            <div className={hasGenAi ? "" : "item-disabled pe-none"}>
                <EditGenAiTaskBasicFields />
            </div>
            <div className="mt-2">
                <ConnectionTestResult testResult={connectionStringTest.data} />
            </div>
        </div>
    );
}

export function EditGenAiTaskStepBasicFooter() {
    const t = useStudioTranslation("editGenAiTask");
    const hasGenAi = useAppSelector(licenseSelectors.statusValue("HasGenAi"));
    const dispatch = useAppDispatch();

    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const connectionStringTest = useAppSelector(editGenAiTaskSelectors.connectionStringTest);
    const aiConnections = useAppSelector(connectionStringSelectors.connectionsByType("Ai"));
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const selectedConnection = aiConnections.find((x) => x.name === formValues.connectionStringName);

    const handleTest = () => {
        const connectionString = mapAiConnectionStringToDto(selectedConnection);

        dispatch(
            editGenAiTaskActions.testConnectionString({
                databaseName,
                connectorType: aiConnectionStringUtils.getConnectorType(connectionString),
                modelType: connectionString.ModelType,
                settings: aiConnectionStringUtils.mapAiConnectionStringToSettingsDto(connectionString),
            })
        );
    };

    const handleNext = async () => {
        const isValid = await trigger([
            "name",
            "state",
            "responsibleNode",
            "connectionStringName",
            "maxConcurrency",
            "isStartingPoint",
            "startingPointType",
            "startingPointChangeVector",
        ]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("context"));
        }
    };

    return (
        <div className="hstack gap-2 justify-content-between">
            <EditGenAiTaskCancelButton />

            {hasGenAi && (
                <div className="hstack gap-2">
                    <PopoverWithHoverWrapper
                        message={selectedConnection ? t("basic.testConnectionInfo") : t("basic.selectConnectionToTest")}
                    >
                        <ButtonWithSpinner
                            variant="info"
                            className="rounded-pill"
                            onClick={handleTest}
                            isSpinning={connectionStringTest.status === "loading"}
                            disabled={!selectedConnection}
                            icon="test"
                        >
                            {t("basic.testConnection")}
                        </ButtonWithSpinner>
                    </PopoverWithHoverWrapper>

                    <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                        {t("footer.next")} <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </div>
            )}
        </div>
    );
}
