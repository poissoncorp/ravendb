import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useServices } from "components/hooks/useServices";
import { FormInput, FormLabel } from "components/common/Form";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import moment from "moment";
import generalUtils from "common/generalUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function BackupSourceRavenCloud() {
    const t = useStudioTranslation("databases");
    const { control } = useFormContext<FormData>();

    const {
        sourceStep: {
            sourceData: { ravenCloud: ravenCloudData },
        },
    } = useWatch({
        control,
    });

    const expireDateMoment = ravenCloudData.awsSettings?.expireDate
        ? moment.utc(ravenCloudData.awsSettings.expireDate)
        : null;

    return (
        <>
            <Row className="mt-2">
                <Col lg="3">
                    <LinkLabel />
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.ravenCloud.link"
                        placeholder={t("createFromBackup.source.ravenCloud.linkPlaceholder")}
                    />
                </Col>
            </Row>
            {expireDateMoment && (
                <Row className="mt-2 align-items-center">
                    <Col lg="3">
                        <FormLabel className="col-form-label">
                            {t("createFromBackup.source.ravenCloud.linkExpiresIn")}
                        </FormLabel>
                    </Col>
                    <Col>
                        <Icon icon="clock" />
                        {expireDateMoment.isBefore()
                            ? t("createFromBackup.source.ravenCloud.linkExpired")
                            : generalUtils.formatDurationByDate(expireDateMoment)}
                    </Col>
                </Row>
            )}
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="ravenCloud" />
        </>
    );
}

function LinkLabel() {
    const t = useStudioTranslation("databases");

    return (
        <PopoverWithHoverWrapper
            message={
                <ol className="m-0">
                    <li>
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.ravenCloud.linkHelpLogin"
                            components={{ code: <code /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.ravenCloud.linkHelpBackups"
                            components={{ code: <code /> }}
                        />
                    </li>
                    <li>{t("createFromBackup.source.ravenCloud.linkHelpSelect")}</li>
                    <li>
                        <StudioTrans
                            ns="databases"
                            i18nKey="createFromBackup.source.ravenCloud.linkHelpGenerate"
                            components={{ code: <code /> }}
                        />
                    </li>
                </ol>
            }
        >
            <FormLabel className="col-form-label">
                {t("createFromBackup.source.ravenCloud.linkLabel")} <Icon icon="info" color="info" margin="m-0" />
            </FormLabel>
        </PopoverWithHoverWrapper>
    );
}

function SourceRestorePoint({ index, remove }: RestorePointElementProps) {
    const { resourcesService } = useServices();
    const { mapToSelectOptions } = useRestorePointUtils();
    const { trigger, setValue, control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { ravenCloud },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(async () => {
        if (!ravenCloud.link || !(await trigger("sourceStep.sourceData.ravenCloud.link"))) {
            return [];
        }

        const credentials = await resourcesService.getCloudBackupCredentialsFromLink(ravenCloud.link);

        setValue("sourceStep.sourceData.ravenCloud.awsSettings", {
            sessionToken: credentials.AwsSessionToken,
            accessKey: credentials.AwsAccessKey,
            secretKey: credentials.AwsSecretKey,
            regionName: credentials.AwsRegionName,
            remoteFolderName: credentials.RemoteFolderName,
            bucketName: credentials.BucketName,
            expireDate: credentials.Expires,
        });

        const dto = await resourcesService.getRestorePoints_S3Backup(
            {
                AwsSessionToken: credentials.AwsSessionToken,
                AwsAccessKey: credentials.AwsAccessKey,
                AwsSecretKey: credentials.AwsSecretKey,
                AwsRegionName: credentials.AwsRegionName,
                RemoteFolderName: credentials.RemoteFolderName,
                BucketName: credentials.BucketName,
                Disabled: false,
                GetBackupConfigurationScript: null,
                CustomServerUrl: null, // TODO RavenDB-14716
                ForcePathStyle: false,
                DisableChecksumValidation: false,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [ravenCloud.link]);

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}
