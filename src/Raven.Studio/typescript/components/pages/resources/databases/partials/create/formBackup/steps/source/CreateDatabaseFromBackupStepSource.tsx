import { FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { OptionWithIcon, SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import React, { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "../../createDatabaseFromBackupValidation";
import BackupSourceLocal from "./BackupSourceLocal";
import BackupSourceAmazonS3 from "./BackupSourceAmazonS3";
import BackupSourceRavenCloud from "./BackupSourceRavenCloud";
import BackupSourceAzure from "./BackupSourceAzure";
import BackupSourceGoogleCloud from "components/pages/resources/databases/partials/create/formBackup/steps/source/BackupSourceGoogleCloud";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import AuthenticationOffMessage from "components/pages/resources/databases/partials/create/shared/AuthenticationOffMessage";
import EncryptionUnavailableMessage from "components/pages/resources/databases/partials/create/shared/EncryptionUnavailableMessage";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

export default function CreateDatabaseFromBackupStepSource() {
    const t = useStudioTranslation("databases");
    const { control, setValue } = useFormContext<FormData>();
    const {
        sourceStep: { sourceData, sourceType, isSetMaxReadOpsPerSecond },
    } = useWatch({
        control,
    });

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const firstRestorePoint = sourceData[sourceType]?.pointsWithTags?.[0]?.restorePoint;
    const isRestorePointEncrypted = firstRestorePoint?.isEncrypted ?? false;
    const isRestorePointSnapshot = firstRestorePoint?.isSnapshotRestore ?? false;

    useEffect(() => {
        const canBeEncryptedOnServer = hasEncryption && isSecureServer;
        const isEncrypted = canBeEncryptedOnServer && isRestorePointEncrypted && !isRestorePointSnapshot;

        setValue(`sourceStep.isEncrypted`, isEncrypted);
    }, [isSecureServer, hasEncryption, isRestorePointEncrypted, isRestorePointSnapshot, setValue]);

    return (
        <>
            <Collapse in={sourceType == null}>
                <div className="d-flex justify-content-center">
                    <img src={backupSourceImg} alt={t("createFromBackup.source.imageAlt")} className="step-img" />
                </div>
            </Collapse>
            <h2>{t("createFromBackup.source.title")}</h2>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">{t("createFromBackup.source.title")}</FormLabel>
                </Col>
                <Col>
                    <FormSelect
                        control={control}
                        name="sourceStep.sourceType"
                        options={getSourceOptions(t)}
                        isSearchable={false}
                        components={{
                            Option: OptionWithIcon,
                            SingleValue: SingleValueWithIcon,
                        }}
                    />
                </Col>
            </Row>
            <Collapse in={sourceType != null}>
                <div>
                    {sourceType === "local" && <BackupSourceLocal />}
                    {sourceType === "ravenCloud" && <BackupSourceRavenCloud />}
                    {sourceType === "amazonS3" && <BackupSourceAmazonS3 />}
                    {sourceType === "azure" && <BackupSourceAzure />}
                    {sourceType === "googleCloud" && <BackupSourceGoogleCloud />}

                    <FormSwitch
                        className="mt-4"
                        control={control}
                        name="sourceStep.isDisableOngoingTasksAfterRestore"
                        color="primary"
                    >
                        <Icon icon="ongoing-tasks" addon="cancel" />
                        {t("createFromBackup.source.disableOngoingTasks")}
                    </FormSwitch>
                    <FormSwitch control={control} name="sourceStep.isSkipIndexes" color="primary">
                        <Icon icon="index" />
                        {t("createFromBackup.source.skipIndexes")}
                    </FormSwitch>
                    <FormSwitch control={control} name="sourceStep.isSetMaxReadOpsPerSecond" color="primary">
                        <Icon icon="traffic-watch" />
                        {t("createFromBackup.source.setMaxReadOpsPerSecond")}
                        <PopoverWithHoverWrapper message={t("createFromBackup.source.maxReadOpsPerSecondInfo")}>
                            <Icon icon="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormSwitch>
                    {isSetMaxReadOpsPerSecond && (
                        <FormInput
                            type="number"
                            control={control}
                            name="sourceStep.maxReadOpsPerSecond"
                            placeholder={t("createFromBackup.source.maxReadOpsPerSecondPlaceholder")}
                            min={1}
                            className="mb-2"
                        />
                    )}
                    <IsEncryptedField
                        isRestorePointSnapshot={isRestorePointSnapshot}
                        isRestorePointEncrypted={isRestorePointEncrypted}
                    />
                </div>
            </Collapse>
        </>
    );
}

const getSourceOptions = (t: StudioTranslate<"databases">): SelectOptionWithIcon<RestoreSource>[] => [
    {
        value: "local",
        label: t("createFromBackup.source.localServerDirectory"),
        icon: "storage",
    },
    {
        value: "ravenCloud",
        label: "RavenDB Cloud",
        icon: "cloud",
    },
    {
        value: "amazonS3",
        label: "Amazon S3",
        icon: "aws",
    },
    {
        value: "azure",
        label: "Microsoft Azure",
        icon: "azure",
    },
    {
        value: "googleCloud",
        label: "Google Cloud Platform",
        icon: "gcp",
    },
];

interface IsEncryptedFieldProps {
    isRestorePointSnapshot: boolean;
    isRestorePointEncrypted: boolean;
}

function IsEncryptedField({ isRestorePointSnapshot, isRestorePointEncrypted }: IsEncryptedFieldProps) {
    const t = useStudioTranslation("databases");
    const { control, formState } = useFormContext<FormData>();

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const isEncryptionDisabled = !hasEncryption || !isSecureServer || formState.isSubmitting || isRestorePointSnapshot;

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: !hasEncryption,
                    message: <EncryptionUnavailableMessage />,
                },
                {
                    isActive: !isSecureServer,
                    message: <AuthenticationOffMessage />,
                },
                {
                    isActive: isRestorePointSnapshot,
                    message: (
                        <span>
                            {isRestorePointEncrypted
                                ? t("createFromBackup.source.encryptedSnapshotInfo")
                                : t("createFromBackup.source.snapshotNotEncrypted")}
                        </span>
                    ),
                },
            ]}
            popoverPlacement="top"
        >
            <FormSwitch color="primary" control={control} name="sourceStep.isEncrypted" disabled={isEncryptionDisabled}>
                <Icon icon="encryption" />
                {t("createDatabase.encryptAtRest")}
                {!hasEncryption && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
            </FormSwitch>
        </ConditionalPopover>
    );
}
