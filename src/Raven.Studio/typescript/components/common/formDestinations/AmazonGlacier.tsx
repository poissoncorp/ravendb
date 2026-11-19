import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import { FormInput, FormLabel, FormSelectCreatable, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapGlacierToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import { availableGlacierRegions } from "components/utils/common";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import OverrideConfigurationViaExternalScriptToggle from "components/common/formDestinations/OverrideConfigurationViaExternalScriptToggle";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AmazonGlacier() {
    const t = useStudioTranslation("formDestinations");
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { glacier: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("Glacier", mapGlacierToDto(formValues));
    });

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Amazon Glacier
                </FormSwitch>
                <Collapse in={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <div>
                        <OverrideConfigurationViaExternalScriptToggle fieldBase={fieldBase} />
                        {formValues.config.isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={fieldBase} />
                        ) : (
                            <div className="vstack gap-3 mt-2">
                                <div className="mb-2">
                                    <FormLabel className="d-flex align-items-center gap-1">
                                        {t("glacier.vaultNameLabel")}
                                        <PopoverWithHoverWrapper
                                            message={
                                                <StudioTrans
                                                    ns="formDestinations"
                                                    i18nKey="glacier.vaultNameInfo"
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
                                        name={getName("vaultName")}
                                        control={control}
                                        placeholder={t("glacier.vaultNamePlaceholder")}
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
                                    <FormLabel>{t("amazon.region")}</FormLabel>
                                    <FormSelectCreatable
                                        name={getName("awsRegionName")}
                                        control={control}
                                        placeholder={t("amazon.regionSelectPlaceholder")}
                                        options={availableGlacierRegions}
                                    />
                                </div>
                                <div className="mb-2">
                                    <FormLabel>{t("amazon.accessKeyLabel")}</FormLabel>
                                    <FormInput
                                        name={getName("awsAccessKey")}
                                        control={control}
                                        placeholder={t("amazon.accessKeyPlaceholder")}
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <FormLabel>{t("amazon.secretKeyLabel")}</FormLabel>
                                    <FormInput
                                        name={getName("awsSecretKey")}
                                        control={control}
                                        placeholder={t("amazon.secretKeyPlaceholder")}
                                        type="password"
                                        passwordPreview
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="d-flex justify-content-end">
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

const fieldBase = "destinations.glacier";

type FormFieldNames = keyof FormDestinations["destinations"]["glacier"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
