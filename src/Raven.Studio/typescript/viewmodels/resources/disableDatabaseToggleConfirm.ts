import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import databases = require("components/models/databases");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("databases");

class disableDatabaseToggleConfirm extends confirmViewModelBase<confirmDialogResult> {

    view = require("views/resources/disableDatabaseToggleConfirm.html");

    desiredAction = ko.observable<string>();
    deletionText: string;
    multipleDatabasesHtml: string;
    confirmDeletionText: string;

    private readonly databases: databases.DatabaseSharedInfo[];

    private readonly disable: boolean;

    constructor(databases: databases.DatabaseSharedInfo[], disable: boolean) {
        super(null);
        this.disable = disable;
        this.databases = databases;

        this.deletionText = disable
            ? t("toggleDatabaseConfirm.disablingSingle")
            : t("toggleDatabaseConfirm.enablingSingle");
        this.multipleDatabasesHtml = disable
            ? t("toggleDatabaseConfirm.disablingMany", { count: databases.length })
            : t("toggleDatabaseConfirm.enablingMany", { count: databases.length });
        this.confirmDeletionText = disable ? t("actions.disable") : t("actions.enable");
    }
}

export = disableDatabaseToggleConfirm;
