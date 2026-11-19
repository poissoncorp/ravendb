import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");

class tasksCommonContent {
        
    static get generalBackupInfo(): string {
        return `<div class="margin-bottom-xs">${t("backup.info.differences")}</div> 
        <ul>
            <li>${t("backup.info.data")}
                <small><ul>
                    <li>${t("backup.info.dataBackup")}</li>
                    <li>${t("backup.info.dataSnapshot")}</li>
                </ul></small>
            </li>
        </ul>
        <div class="margin-bottom-xs">${t("backup.info.indexDataDifferences")}</div>
        <ul>
            <li>${t("backup.info.speed")}
                <small><ul>
                   <li>${t("backup.info.speedDetails")}</li>
                </ul></small>
            </li>
            <li>${t("backup.info.size")}
                <small><ul>
                    <li>${t("backup.info.sizeDetails")}</li>
                </ul></small>
            </li>
            <li>${t("backup.info.restore")}
                <small><ul>
                    <li>${t("backup.info.restoreDetails")}</li>
                </ul></small>
            </li>
        </ul>
        <div>${t("backup.info.notes")}</div>
        <ul>
            <li><small>${t("backup.info.incrementalSnapshot")}</small></li>
            <li><small>${t("backup.info.incrementalSnapshotSharded")}</small></li>
        </ul>`;
    }
    
    static get backupAgeInfo(): string {
        return "<ul>" +
            `<li>${t("backup.ageInfo.minimumTime")}<br></li>` +
            `<li>${t("backup.ageInfo.fullBackupDeleted")}<br>` +
            `${t("backup.ageInfo.incrementalBackups")}<br></li>` +
            `<li>${t("backup.ageInfo.deletionSchedule")}</li>` +
        "</ul>";
    }
 
    static get ftpHostInfo(): string {
        return `${t("backup.ftpHostInfo.protocol")}<br />` +
            `${t("backup.ftpHostInfo.defaultProtocol")}<br />` +
            `${t("backup.ftpHostInfo.completeUrl")}<br />` +
            t("backup.ftpHostInfo.example", { url: "ftp://host.name:port/backup-folder/nested-backup-folder" });
    }
    
    static get serverwideSnapshotEncryptionInfo(): string {
        return `${t("backup.serverWideSnapshotEncryption.intro")}<br></li>` +
            "<ul>" +
                `<li>${t("backup.serverWideSnapshotEncryption.encrypted")}<br></li>` +
                `<li>${t("backup.serverWideSnapshotEncryption.notEncrypted")}</li>` +
            "</ul>";
    }
    
    static textForPopover(storageName: string, targetOperation: string) : string {
        return `${tasksCommonContent.storageCreatedManuallyText(storageName, targetOperation)}<br /> ` +
            t("backup.testCredentials");
    }

    static textForPopoverGCS(storageName: string, targetOperation: string): string {
        return `${tasksCommonContent.storageCreatedManuallyText(storageName, targetOperation)}<br /> ` +
            `${t("backup.testCredentials")}<br />` +
            `<a href='https://cloud.google.com/storage/docs/bucket-naming' target='_blank'>${t("backup.bucketNamingGuidelines")}</a>`;
    }

    private static storageCreatedManuallyText(storageName: string, targetOperation: string): string {
        return t("backup.storageCreatedManually", {
            storageName: generalUtils.escapeHtml(storageName),
            targetOperation: generalUtils.escapeHtml(targetOperation),
        });
    }

    static get externalScriptNotAllowedForNonClusterAdmins(): string {
        return t("externalScriptNotAllowedForNonClusterAdmins");
    }

    static get etlTaskNameLocked(): string {
        return t("etlTaskNameLocked");
    }

    // Name can't be changed for any ETL task, but e.g. AI Tasks are not marked as ETL on the UI
    static get taskNameLocked(): string {
        return t("taskNameLocked");
    }
}

export = tasksCommonContent;
