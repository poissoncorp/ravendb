import { Icon } from "components/common/Icon";
import Collapse from "react-bootstrap/Collapse";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { useFormContext, useWatch } from "react-hook-form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { FormInput, FormLabel, FormSelectAutocomplete, FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { availableS3Regions } from "components/utils/common";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function BackupSourceAmazonS3() {
    const t = useStudioTranslation("databases");
    const { control } = useFormContext<FormData>();

    const {
        sourceStep: {
            sourceData: {
                amazonS3: { isUseCustomHost },
            },
        },
    } = useWatch({
        control,
    });

    return (
        <div className="mt-2">
            <Row>
                <Col lg={{ offset: 3 }}>
                    <FormSwitch control={control} name="sourceStep.sourceData.amazonS3.isUseCustomHost" color="primary">
                        {t("createFromBackup.source.amazonS3.useCustomHost")}
                    </FormSwitch>
                </Col>
            </Row>

            <Collapse in={isUseCustomHost}>
                <div>
                    <Row>
                        <Col lg={{ offset: 3 }}>
                            <FormSwitch
                                color="primary"
                                control={control}
                                name="sourceStep.sourceData.amazonS3.isForcePathStyle"
                            >
                                {t("createFromBackup.source.amazonS3.forcePathStyle")}{" "}
                                <PopoverWithHoverWrapper
                                    message={
                                        <StudioTrans
                                            ns="databases"
                                            i18nKey="createFromBackup.source.amazonS3.forcePathStyleInfo"
                                            components={{ code: <code /> }}
                                        />
                                    }
                                >
                                    <Icon icon="info" color="info" margin="m-0" />
                                </PopoverWithHoverWrapper>
                            </FormSwitch>
                        </Col>
                    </Row>
                    <Row className="mt-2">
                        <Col lg="3">
                            <FormLabel className="col-form-label">
                                {t("createFromBackup.source.amazonS3.customServerUrl")}
                            </FormLabel>
                        </Col>
                        <Col>
                            <FormInput
                                type="text"
                                control={control}
                                name="sourceStep.sourceData.amazonS3.customHost"
                                placeholder={t("createFromBackup.source.amazonS3.customServerUrlPlaceholder")}
                            />
                        </Col>
                    </Row>
                </div>
            </Collapse>

            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">{t("createFromBackup.source.amazonS3.accessKey")}</FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.amazonS3.accessKey"
                        placeholder={t("createFromBackup.source.amazonS3.accessKeyPlaceholder")}
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label" aria-required>
                        {t("createFromBackup.source.amazonS3.secretKey")}
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        control={control}
                        name="sourceStep.sourceData.amazonS3.secretKey"
                        placeholder={t("createFromBackup.source.amazonS3.secretKeyPlaceholder")}
                        type="password"
                        passwordPreview
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.amazonS3.sessionToken"
                            components={{ br: <br />, small: <small /> }}
                        />
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        control={control}
                        name="sourceStep.sourceData.amazonS3.sessionToken"
                        placeholder={t("createFromBackup.source.amazonS3.sessionTokenPlaceholder")}
                        type="password"
                        passwordPreview
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.amazonS3.awsRegion"
                            options={{ context: isUseCustomHost ? "optional" : undefined }}
                            components={{ br: <br />, small: <small /> }}
                        />
                    </FormLabel>
                </Col>
                <Col>
                    {isUseCustomHost ? (
                        <FormInput
                            type="text"
                            control={control}
                            name="sourceStep.sourceData.amazonS3.awsRegion"
                            placeholder={t("createFromBackup.source.amazonS3.awsRegionPlaceholder")}
                            autoComplete="off"
                        />
                    ) : (
                        <FormSelectAutocomplete
                            name="sourceStep.sourceData.amazonS3.awsRegion"
                            control={control}
                            placeholder={t("createFromBackup.source.amazonS3.awsRegionSelectPlaceholder")}
                            options={availableS3Regions}
                        />
                    )}
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">{t("createFromBackup.source.amazonS3.bucketName")}</FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.amazonS3.bucketName"
                        placeholder={t("createFromBackup.source.amazonS3.bucketNamePlaceholder")}
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.amazonS3.remoteFolderName"
                            components={{ br: <br />, small: <small /> }}
                        />
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.amazonS3.remoteFolderName"
                        placeholder={t("createFromBackup.source.remoteFolderNamePlaceholder")}
                    />
                </Col>
            </Row>
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="amazonS3" />
        </div>
    );
}

function SourceRestorePoint({ index, remove }: RestorePointElementProps) {
    const { resourcesService } = useServices();
    const { mapToSelectOptions } = useRestorePointUtils();

    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { amazonS3: amazonS3Data },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(async () => {
        if (!amazonS3Data.accessKey || !amazonS3Data.secretKey || !amazonS3Data.awsRegion || !amazonS3Data.bucketName) {
            return [];
        }

        const dto = await resourcesService.getRestorePoints_S3Backup(
            {
                AwsAccessKey: amazonS3Data.accessKey,
                AwsSecretKey: amazonS3Data.secretKey,
                AwsRegionName: amazonS3Data.awsRegion,
                BucketName: amazonS3Data.bucketName,
                RemoteFolderName: amazonS3Data.remoteFolderName,
                AwsSessionToken: amazonS3Data.sessionToken,
                CustomServerUrl: amazonS3Data.isUseCustomHost ? amazonS3Data.customHost : null,
                ForcePathStyle: amazonS3Data.isUseCustomHost && amazonS3Data.isForcePathStyle,
                DisableChecksumValidation: amazonS3Data.isUseCustomHost && amazonS3Data.isDisableChecksumValidation,
                Disabled: false,
                GetBackupConfigurationScript: null,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [
        amazonS3Data.accessKey,
        amazonS3Data.secretKey,
        amazonS3Data.sessionToken,
        amazonS3Data.awsRegion,
        amazonS3Data.bucketName,
        amazonS3Data.remoteFolderName,
        amazonS3Data.isUseCustomHost,
        amazonS3Data.customHost,
        amazonS3Data.isForcePathStyle,
        amazonS3Data.isDisableChecksumValidation,
        isSharded,
    ]);

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}
