import { Icon } from "components/common/Icon";
import React from "react";
import { RemoteAttachmentsDestinationFormData } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";
import { UseAsyncReturn } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import { FormGroup, FormInput, FormLabel, FormSelect, FormSelectCreatable, FormSwitch } from "components/common/Form";
import Collapse from "react-bootstrap/Collapse";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Badge from "react-bootstrap/Badge";
import { availableS3Regions, storageClassOptions } from "components/utils/common";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import AzureAuthTypeToggle from "components/common/formDestinations/AzureAuthTypeToggle";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface RemoteAttachmentsDestinationFieldsProps {
    asyncTest: UseAsyncReturn<Raven.Server.Web.System.NodeConnectionTestResult, []>;
}

export function RemoteAttachmentsS3Fields({ asyncTest }: RemoteAttachmentsDestinationFieldsProps) {
    const t = useStudioTranslation("remoteAttachments");
    const { control } = useFormContext<RemoteAttachmentsDestinationFormData>();
    const s3Values = useWatch({ control, name: "s3" });

    return (
        <div className="vstack mt-3">
            <FormSwitch control={control} name="s3.isUseCustomHost" className="w-100">
                {t("s3.useCustomHost")}
            </FormSwitch>

            <Collapse in={s3Values?.isUseCustomHost} mountOnEnter unmountOnExit>
                <div>
                    <FormSwitch control={control} name="s3.forcePathStyle" className="w-100 mt-1 mb-2">
                        <span className="d-flex gap-1 align-items-center">
                            {t("s3.forcePathStyle")}
                            <PopoverWithHoverWrapper
                                message={
                                    <StudioTrans
                                        ns="remoteAttachments"
                                        i18nKey="s3.forcePathStyleInfo"
                                        components={{ br: <br />, code: <code /> }}
                                        options={{
                                            pathStyleUrl: "https://{Server-URL}/{Bucket-Name}",
                                            virtualHostedUrl: "https://{Bucket-Name}.{Server-URL}",
                                        }}
                                    />
                                }
                            >
                                <Icon icon="info" color="info" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormSwitch>

                    <FormGroup marginClass="mb-1">
                        <FormLabel>{t("s3.customServerUrl")}</FormLabel>
                        <FormInput
                            control={control}
                            name="s3.customServerUrl"
                            placeholder={t("s3.customServerUrlPlaceholder")}
                            type="text"
                            autoComplete="off"
                        />
                    </FormGroup>
                </div>
            </Collapse>

            <FormGroup marginClass="mt-2 mb-3">
                <FormLabel>{t("fields.identifier")}</FormLabel>
                <FormInput
                    type="text"
                    name="identifier"
                    placeholder={t("fields.identifierPlaceholder")}
                    control={control}
                />
            </FormGroup>

            <FormGroup>
                <FormLabel className="d-flex align-items-center gap-1">
                    {t("s3.bucketName")}
                    <ConnectionPill asyncTest={asyncTest} />
                </FormLabel>
                <FormInput
                    control={control}
                    name="s3.bucketName"
                    placeholder={t("s3.bucketNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>
                    <StudioTrans
                        ns="remoteAttachments"
                        i18nKey="fields.remoteFolderName"
                        components={{ small: <small className="text-muted fw-light" /> }}
                    />
                </FormLabel>
                <FormInput
                    control={control}
                    name="s3.remoteFolderName"
                    placeholder={t("fields.remoteFolderNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>
                    <StudioTrans
                        ns="remoteAttachments"
                        i18nKey="s3.region"
                        components={{ small: <small className="text-muted fw-light" /> }}
                        options={{ context: s3Values?.isUseCustomHost ? "optional" : undefined }}
                    />
                </FormLabel>
                {s3Values?.isUseCustomHost ? (
                    <FormInput
                        type="text"
                        control={control}
                        name="s3.awsRegionName"
                        placeholder={t("s3.regionInputPlaceholder")}
                        autoComplete="off"
                    />
                ) : (
                    <FormSelectCreatable
                        name="s3.awsRegionName"
                        control={control}
                        placeholder={t("s3.regionSelectPlaceholder")}
                        options={availableS3Regions}
                    />
                )}
            </FormGroup>

            <FormGroup>
                <FormLabel>{t("s3.accessKey")}</FormLabel>
                <FormInput
                    name="s3.awsAccessKey"
                    control={control}
                    placeholder={t("s3.accessKeyPlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>{t("s3.secretKey")}</FormLabel>
                <FormInput
                    name="s3.awsSecretKey"
                    control={control}
                    placeholder={t("s3.secretKeyPlaceholder")}
                    type="password"
                    passwordPreview
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>{t("s3.storageClass")}</FormLabel>
                <FormSelect
                    name="s3.storageClass"
                    control={control}
                    options={storageClassOptions}
                    menuPlacement="top"
                />
            </FormGroup>

            {asyncTest.result?.Error && (
                <div className="mt-3">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}
        </div>
    );
}

interface ConnectionPillProps {
    asyncTest: UseAsyncReturn<Raven.Server.Web.System.NodeConnectionTestResult, []>;
}
function ConnectionPill({ asyncTest }: ConnectionPillProps) {
    const t = useStudioTranslation("remoteAttachments");

    if (!asyncTest.result) {
        return null;
    }

    if (asyncTest.result.Success) {
        return (
            <Badge bg="success" pill>
                <Icon icon="check" />
                {t("connection.success")}
            </Badge>
        );
    }

    return (
        <Badge bg="danger" pill>
            <Icon icon="warning" />
            {t("connection.failure")}
        </Badge>
    );
}

export function RemoteAttachmentsAzureFields({ asyncTest }: RemoteAttachmentsDestinationFieldsProps) {
    const t = useStudioTranslation("remoteAttachments");
    const { control } = useFormContext<RemoteAttachmentsDestinationFormData>();
    const authType = useWatch({ control, name: "azure.authType" });

    return (
        <div className="mt-3">
            <FormGroup>
                <FormLabel>{t("fields.identifier")}</FormLabel>
                <FormInput
                    type="text"
                    name="identifier"
                    placeholder={t("fields.identifierPlaceholder")}
                    control={control}
                />
            </FormGroup>

            <FormGroup>
                <FormLabel className="d-flex gap-1 align-items-center">
                    {t("azure.storageContainer")}
                    <ConnectionPill asyncTest={asyncTest} />
                </FormLabel>
                <FormInput
                    name="azure.storageContainer"
                    control={control}
                    placeholder={t("azure.storageContainerPlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>
                    <StudioTrans
                        ns="remoteAttachments"
                        i18nKey="fields.remoteFolderName"
                        components={{ small: <small className="text-muted fw-light" /> }}
                    />
                </FormLabel>
                <FormInput
                    name="azure.remoteFolderName"
                    control={control}
                    placeholder={t("fields.remoteFolderNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>{t("azure.accountName")}</FormLabel>
                <FormInput
                    name="azure.accountName"
                    control={control}
                    placeholder={t("azure.accountNamePlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <AzureAuthTypeToggle control={control} name="azure.authType" className="mb-2" />
                {authType === "sasToken" ? (
                    <FormInput
                        key="sasToken"
                        name="azure.sasToken"
                        control={control}
                        placeholder={t("azure.sasTokenPlaceholder")}
                        type="password"
                        passwordPreview
                        autoComplete="off"
                    />
                ) : (
                    <FormInput
                        key="accountKey"
                        name="azure.accountKey"
                        control={control}
                        placeholder={t("azure.accountKeyPlaceholder")}
                        type="password"
                        passwordPreview
                        autoComplete="off"
                    />
                )}
            </FormGroup>

            {asyncTest.result?.Error && (
                <div className="mt-3">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}
        </div>
    );
}
