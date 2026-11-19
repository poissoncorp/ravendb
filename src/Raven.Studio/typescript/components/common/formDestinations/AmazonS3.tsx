import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import { FormInput, FormSelectCreatable, FormSwitch } from "../Form";
import OverrideConfiguration from "./OverrideConfiguration";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapS3ToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { availableS3Regions } from "components/utils/common";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import { FormLabel } from "components/common/Form";
import OverrideConfigurationViaExternalScriptToggle from "components/common/formDestinations/OverrideConfigurationViaExternalScriptToggle";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AmazonS3() {
    const t = useStudioTranslation("formDestinations");
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { s3: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("S3", mapS3ToDto(formValues));
    });

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Amazon S3
                </FormSwitch>
                <Collapse in={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <div>
                        <OverrideConfigurationViaExternalScriptToggle fieldBase={fieldBase} />
                        {formValues.config.isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={fieldBase} />
                        ) : (
                            <>
                                <div className="vstack gap-2">
                                    <FormSwitch
                                        control={control}
                                        name={getName("isUseCustomHost")}
                                        className="w-100"
                                        color="secondary"
                                    >
                                        {t("s3.useCustomHost")}
                                    </FormSwitch>
                                    {formValues.isUseCustomHost && (
                                        <FormSwitch
                                            control={control}
                                            name={getName("forcePathStyle")}
                                            className="w-100"
                                            color="secondary"
                                        >
                                            <span className="d-flex gap-1 align-items-center">
                                                {t("s3.forcePathStyle")}
                                                <PopoverWithHoverWrapper
                                                    message={
                                                        <StudioTrans
                                                            ns="formDestinations"
                                                            i18nKey="s3.forcePathStyleInfo"
                                                            components={{ code: <code /> }}
                                                            options={{
                                                                pathStyleUrl: "https://{Server-URL}/{Bucket-Name}",
                                                                virtualHostedUrl:
                                                                    "{https://{Bucket-Name}.{Server-URL}}",
                                                            }}
                                                        />
                                                    }
                                                >
                                                    <Icon icon="info" color="info" />
                                                </PopoverWithHoverWrapper>
                                            </span>
                                        </FormSwitch>
                                    )}
                                    {formValues.isUseCustomHost && (
                                        <FormSwitch
                                            control={control}
                                            name={getName("disableChecksumValidation")}
                                            className="w-100"
                                            color="secondary"
                                        >
                                            <span className="d-flex gap-1 align-items-center">
                                                {t("s3.disableChecksumValidation")}
                                                <PopoverWithHoverWrapper
                                                    message={t("s3.disableChecksumValidationInfo")}
                                                >
                                                    <Icon icon="info" color="info" />
                                                </PopoverWithHoverWrapper>
                                            </span>
                                        </FormSwitch>
                                    )}
                                </div>
                                <div className="vstack gap-3 mt-2">
                                    {formValues.isUseCustomHost && (
                                        <div className="mb-2">
                                            <FormLabel>{t("s3.customServerUrlLabel")}</FormLabel>
                                            <FormInput
                                                control={control}
                                                name={getName("customServerUrl")}
                                                placeholder={t("s3.customServerUrlPlaceholder")}
                                                type="text"
                                                autoComplete="off"
                                            />
                                        </div>
                                    )}
                                    <div className="mb-2">
                                        <FormLabel className="d-flex align-items-center gap-1">
                                            {t("s3.bucketNameLabel")}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <StudioTrans
                                                        ns="formDestinations"
                                                        i18nKey="bucketInfo"
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
                                            control={control}
                                            name={getName("bucketName")}
                                            placeholder={t("s3.bucketNamePlaceholder")}
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
                                            control={control}
                                            name={getName("remoteFolderName")}
                                            placeholder={t("remoteFolderName.placeholder")}
                                            type="text"
                                            autoComplete="off"
                                        />
                                    </div>
                                    <div className="mb-2">
                                        <FormLabel>
                                            <StudioTrans
                                                ns="formDestinations"
                                                i18nKey="amazon.region"
                                                components={{ small: <small className="text-muted fw-light" /> }}
                                                options={{
                                                    context: formValues.isUseCustomHost ? "optional" : undefined,
                                                }}
                                            />
                                        </FormLabel>
                                        {formValues.isUseCustomHost ? (
                                            <FormInput
                                                type="text"
                                                control={control}
                                                name={getName("awsRegionName")}
                                                placeholder={t("s3.regionPlaceholder")}
                                                autoComplete="off"
                                            />
                                        ) : (
                                            <FormSelectCreatable
                                                name={getName("awsRegionName")}
                                                control={control}
                                                placeholder={t("amazon.regionSelectPlaceholder")}
                                                options={availableS3Regions}
                                            />
                                        )}
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
                                </div>
                                {asyncTest.result?.Error && (
                                    <div className="mt-3">
                                        <ConnectionTestResult testResult={asyncTest.result} />
                                    </div>
                                )}
                            </>
                        )}
                    </div>
                </Collapse>
            </Card.Body>
        </Card>
    );
}

const fieldBase = "destinations.s3";

type FormFieldNames = keyof FormDestinations["destinations"]["s3"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
