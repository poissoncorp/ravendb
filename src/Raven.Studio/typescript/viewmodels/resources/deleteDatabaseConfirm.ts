import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import databases = require("components/models/databases");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("deleteDatabaseConfirm");

class deleteDatabaseConfirm extends confirmViewModelBase<deleteDatabaseConfirmResult> {

    view = require("views/resources/deleteDatabaseConfirm.html");
    
    private isKeepingFiles = ko.observable<boolean>(true);
    private encryptedCount: number;

    private readonly databasesToDelete: databases.DatabaseSharedInfo[];

    readonly dataNotRecoveredHtml = ko.pureComputed(() =>
        t("keepFiles.dataNotRecovered", { exportAdvice: generalUtils.escapeHtml(t("keepFiles.exportAdvice")) })
    );

    readonly secretKeyDeletedHtml = ko.pureComputed(() =>
        t("keepFiles.secretKeyDeleted", {
            count: this.encryptedCount,
            exportKey: generalUtils.escapeHtml(t("keepFiles.exportKeyWithRvn")),
        })
    );

    constructor(databasesToDelete: databases.DatabaseSharedInfo[]) {
        super();
        this.databasesToDelete = databasesToDelete;

        this.encryptedCount = databasesToDelete.filter(x => x.isEncrypted).length;
    }

    goToManageDbGroup() {
        router.navigate(appUrl.forManageDatabaseGroup(this.databasesToDelete[0].name));
        this.cancel();
    }
    
    keepFiles() {
        this.isKeepingFiles(true);
        this.confirm();
    }

    deleteEverything() {
        this.isKeepingFiles(false);
        this.confirm();
    }

    exportDatabase() {
        router.navigate(appUrl.forExportDatabase(this.databasesToDelete[0].name));
        this.cancel();
    }

    protected getConfirmButton(): HTMLElement {
        return $(".modal-footer:visible .btn-danger")[0] as HTMLElement;
    }

    protected prepareResponse(can: boolean): deleteDatabaseConfirmResult {
        return {
            can: can,
            keepFiles: this.isKeepingFiles()
        };
    }
}

export = deleteDatabaseConfirm;
