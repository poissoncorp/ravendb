import generalUtils from "common/generalUtils";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import {
    RestorePoint,
    RestorePointBackupType,
} from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { useAppSelector } from "components/store";
import moment from "moment";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type RestorePointSnapshotDisabledReason = "Server is not secure" | "Has no encryption in license";

export type RestorePointOption = SelectOption<RestorePoint> & {
    isDisabled: boolean;
    disabledReason?: RestorePointSnapshotDisabledReason;
};

export interface RestorePointGroupedOption {
    label: string;
    options: RestorePointOption[];
}

export function useRestorePointUtils() {
    const t = useStudioTranslation("databases");
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);
    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));

    const getDisabledReason = (restorePoint: RestorePoint): RestorePointSnapshotDisabledReason | null => {
        if (!restorePoint.isEncrypted || !restorePoint.isSnapshotRestore) {
            return null;
        }
        if (!hasEncryption) {
            return "Has no encryption in license";
        }
        if (!isSecureServer) {
            return "Server is not secure";
        }

        return null;
    };

    const mapToSelectOptions = (
        dto: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints
    ): RestorePointGroupedOption[] => {
        const groups: RestorePointGroupedOption[] = [];

        dto.List.forEach((dtoRestorePoint) => {
            const databaseName =
                dtoRestorePoint.DatabaseName ?? t("createFromBackup.source.restorePoint.unknownDatabase");

            if (!groups.find((x) => x.label === databaseName)) {
                groups.push({ label: databaseName, options: [] });
            }

            const group = groups.find((x) => x.label === databaseName);
            const restorePointValue = mapFromDto(dtoRestorePoint);
            const disabledReason = getDisabledReason(restorePointValue);

            group.options.push({
                value: restorePointValue,
                label: t("createFromBackup.source.restorePoint.optionLabel", {
                    context: restorePointValue.backupType,
                    dateTime: restorePointValue.dateTime,
                }),
                isDisabled: !!disabledReason,
                disabledReason,
            });
        });

        return groups;
    };

    return { mapToSelectOptions };
}

function mapFromDto(dto: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoint): RestorePoint {
    let backupType: RestorePointBackupType;
    if (dto.IsSnapshotRestore) {
        backupType = dto.IsIncremental ? "incrementalSnapshot" : "snapshot";
    } else if (dto.IsIncremental) {
        backupType = "incremental";
    } else {
        backupType = "full";
    }

    return {
        dateTime: moment(dto.DateTime).format(generalUtils.dateFormat),
        location: dto.Location,
        fileName: dto.FileName,
        isSnapshotRestore: dto.IsSnapshotRestore,
        isIncremental: dto.IsIncremental,
        isEncrypted: dto.IsEncrypted,
        filesToRestore: dto.FilesToRestore,
        databaseName: dto.DatabaseName,
        nodeTag: dto.NodeTag || "-",
        backupType,
    };
}
