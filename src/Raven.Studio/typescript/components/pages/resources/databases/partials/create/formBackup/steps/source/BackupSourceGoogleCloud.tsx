import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { FormInput, FormLabel } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function BackupSourceGoogleCloud() {
    const t = useStudioTranslation("databases");
    const { control } = useFormContext<FormData>();

    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        {t("createFromBackup.source.googleCloud.bucketName")}
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.googleCloud.bucketName"
                        placeholder={t("createFromBackup.source.googleCloud.bucketNamePlaceholder")}
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        {t("createFromBackup.source.googleCloud.credentials")}
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="textarea"
                        as="textarea"
                        rows={18}
                        control={control}
                        name="sourceStep.sourceData.googleCloud.credentialsJson"
                        placeholder={t("createFromBackup.source.googleCloud.credentialsPlaceholder", {
                            example: CREDENTIALS_JSON_EXAMPLE,
                        })}
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.remoteFolderName"
                            components={{ small: <small /> }}
                        />
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.googleCloud.remoteFolderName"
                        placeholder={t("createFromBackup.source.googleCloud.remoteFolderNamePlaceholder")}
                    />
                </Col>
            </Row>
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="googleCloud" />
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
            sourceData: { googleCloud: googleCloudData },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(async () => {
        if (!googleCloudData.bucketName || !googleCloudData.credentialsJson) {
            return [];
        }

        const dto = await resourcesService.getRestorePoints_GoogleCloudBackup(
            {
                BucketName: googleCloudData.bucketName,
                GoogleCredentialsJson: googleCloudData.credentialsJson,
                RemoteFolderName: googleCloudData.remoteFolderName,
                Disabled: false,
                GetBackupConfigurationScript: null,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [googleCloudData.bucketName, googleCloudData.credentialsJson, googleCloudData.remoteFolderName]);

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}

const CREDENTIALS_JSON_EXAMPLE = `{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=\n-----END PRIVATE KEY-----\n",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
}`;
