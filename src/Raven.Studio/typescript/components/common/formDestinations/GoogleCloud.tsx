import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import Button from "react-bootstrap/Button";
import { FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapGoogleCloudToDto } from "./utils/formDestinationsMapsToDto";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import useBoolean from "components/hooks/useBoolean";
import classNames from "classnames";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import OverrideConfigurationViaExternalScriptToggle from "components/common/formDestinations/OverrideConfigurationViaExternalScriptToggle";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface GoogleCloudProps {
    isForNewConnection: boolean;
}
export default function GoogleCloud({ isForNewConnection }: GoogleCloudProps) {
    const t = useStudioTranslation("formDestinations");
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { googleCloud: formValues },
    } = useWatch({ control });

    const { value: isCredentialsJsonVisible, toggle: toggleCredentialsJsonVisible } = useBoolean(
        isForNewConnection || !formValues.isEnabled
    );

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("GoogleCloud", mapGoogleCloudToDto(formValues));
    });

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Google Cloud
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
                                        {t("googleCloud.bucketLabel")}{" "}
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    <StudioTrans
                                                        ns="formDestinations"
                                                        i18nKey="bucketInfo"
                                                        components={{ span: <span className="text-info" /> }}
                                                    />
                                                    <hr className="my-2" />
                                                    <a
                                                        href="https://cloud.google.com/storage/docs/bucket-naming"
                                                        target="_blank"
                                                    >
                                                        <Icon icon="newtab" />
                                                        {t("googleCloud.namingGuidelines")}
                                                    </a>
                                                </>
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
                                        name={getName("bucketName")}
                                        control={control}
                                        placeholder={t("googleCloud.bucketPlaceholder")}
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
                                    <FormLabel>{t("googleCloud.credentialsJsonLabel")}</FormLabel>
                                    <FormInput
                                        name={getName("googleCredentialsJson")}
                                        control={control}
                                        placeholder={t("googleCloud.credentialsJsonPlaceholder", {
                                            example: googleCredentialsJsonExample,
                                        })}
                                        type="textarea"
                                        as="textarea"
                                        autoComplete="off"
                                        rows={15}
                                        className={classNames({ "d-none": !isCredentialsJsonVisible })}
                                    />
                                </div>
                                <Button
                                    type="button"
                                    variant="secondary"
                                    className="w-fit-content mb-2"
                                    onClick={toggleCredentialsJsonVisible}
                                >
                                    {isCredentialsJsonVisible ? (
                                        <>
                                            <Icon icon="preview-off" />
                                            {t("googleCloud.hideCredentials")}
                                        </>
                                    ) : (
                                        <>
                                            <Icon icon="preview" />
                                            {t("googleCloud.showCredentials")}
                                        </>
                                    )}
                                </Button>
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

const googleCredentialsJsonExample = `{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=-----END PRIVATE KEY-----",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
}`;

const fieldBase = "destinations.googleCloud";

type FormFieldNames = keyof FormDestinations["destinations"]["googleCloud"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
