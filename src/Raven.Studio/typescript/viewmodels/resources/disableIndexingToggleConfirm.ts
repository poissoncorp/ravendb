import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("databases");

class disableIndexingToggleConfirm extends confirmViewModelBase<confirmDialogResult> {

    view = require("views/resources/disableIndexingToggleConfirm.html");

    text: string;
    reloadWarningHtml: string;
    reloadByTogglingHtml: string;
    reloadByRestartingHtml: string;
    confirmText: string;

    private readonly disable: boolean;

    constructor(disable: boolean) {
        super(null);
        this.disable = disable;

        this.text = t("toggleIndexingConfirm.intro", { context: disable ? "disable" : "enable" });
        this.reloadWarningHtml = t("toggleIndexingConfirm.reloadWarning");
        this.reloadByTogglingHtml = t("toggleIndexingConfirm.reloadByToggling");
        this.reloadByRestartingHtml = t("toggleIndexingConfirm.reloadByRestarting");
        this.confirmText = disable ? t("actions.disableIndexing") : t("actions.enableIndexing");
    }
}

export = disableIndexingToggleConfirm;
