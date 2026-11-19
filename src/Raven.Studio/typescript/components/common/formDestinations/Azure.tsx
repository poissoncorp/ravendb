import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import { FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapAzureToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import OverrideConfigurationViaExternalScriptToggle from "components/common/formDestinations/OverrideConfigurationViaExternalScriptToggle";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import AzureAuthTypeToggle from "components/common/formDestinations/AzureAuthTypeToggle";

export default function Azure() {
    const t = useStudioTranslation("formDestinations");
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { azure: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("Azure", mapAzureToDto(formValues));
    });

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Azure
                </FormSwitch>
                <Collapse in={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <div>
                        <OverrideConfigurationViaExternalScriptToggle fieldBase={fieldBase} />
                        {formValues.config.isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={fieldBase} />
                        ) : (
                            <div className="vstack gap-3 mt-2">
                                <div className="mb-2">
                                    <FormLabel className="d-flex gap-1 align-items-center">
                                        {t("azure.storageContainerLabel")}{" "}
                                        <PopoverWithHoverWrapper
                                            message={
                                                <StudioTrans
                                                    ns="formDestinations"
                                                    i18nKey="azure.storageContainerInfo"
                                                    components={{ span: <span className="text-info" /> }}
                                                />
                                            }
                                        >
                                            <Icon icon="info" color="info" margin="m-0" />
                                        </PopoverWithHoverWrapper>
                                        {asyncTest.result?.Success ? (
                                            <Badge bg="success" pill>
                                                <Icon icon="check" />
                                                {t("connection.success")}
                                            </Badge>
                                        ) : asyncTest.result?.Error ? (
                                            <Badge bg="danger" pill>
                                                <Icon icon="warning" />
                                                {t("connection.failed")}
                                            </Badge>
                                        ) : null}
                                    </FormLabel>
                                    <FormInput
                                        name={getName("storageContainer")}
                                        control={control}
                                        placeholder={t("azure.storageContainerPlaceholder")}
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <FormLabel>
                                        <StudioTrans
                                            ns="formDestinations"
                                            i18nKey="remoteFolderName.label"
                                            components={{ small: <small className="text-muted fw-light" /> }}
                                        />
                                    </FormLabel>
                                    <FormInput
                                        name={getName("remoteFolderName")}
                                        control={control}
                                        placeholder={t("remoteFolderName.placeholder")}
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <FormLabel>{t("azure.accountNameLabel")}</FormLabel>
                                    <FormInput
                                        name={getName("accountName")}
                                        control={control}
                                        placeholder={t("azure.accountNamePlaceholder")}
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <AzureAuthTypeToggle
                                        control={control}
                                        name={getName("authType")}
                                        className="mb-2"
                                    />
                                    {formValues.authType === "sasToken" ? (
                                        <FormInput
                                            key="sasToken"
                                            name={getName("sasToken")}
                                            control={control}
                                            placeholder={t("azure.sasTokenPlaceholder")}
                                            type="password"
                                            passwordPreview
                                            autoComplete="off"
                                        />
                                    ) : (
                                        <FormInput
                                            key="accountKey"
                                            name={getName("accountKey")}
                                            control={control}
                                            placeholder={t("azure.accountKeyPlaceholder")}
                                            type="password"
                                            passwordPreview
                                            autoComplete="off"
                                        />
                                    )}
                                </div>
                                <div className="d-flex">
                                    <FlexGrow />
                                    <ButtonWithSpinner
                                        type="button"
                                        variant="secondary"
                                        onClick={asyncTest.execute}
                                        isSpinning={asyncTest.loading}
                                        icon="rocket"
                                    >
                                        {t("connection.testCredentials")}
                                    </ButtonWithSpinner>
                                </div>
                                {asyncTest.result?.Error && (
                                    <div className="mt-3">
                                        <ConnectionTestResult testResult={asyncTest.result} />
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </Collapse>
            </Card.Body>
        </Card>
    );
}

const fieldBase = "destinations.azure";

type FormFieldNames = keyof FormDestinations["destinations"]["azure"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
