import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("editCmpXchg");

class deleteCompareExchangeConfirm extends dialogViewModelBase {

    view = require("views/database/documents/deleteCompareExchangeConfirm.html");

    private itemKeys = ko.observableArray<string>();
    
    title: string;
    subTitleHtml: string;

    constructor(itemsKeys: Array<string>) {
        super(null);

        if (itemsKeys.length === 0) {
            throw new Error("Must have at least one compare exchange item to delete.");
        }

        this.itemKeys(itemsKeys);

        if (this.itemKeys().length === 1) {
            this.title = t("deleteConfirm.title", { context: "single" });
            this.subTitleHtml = t("deleteConfirm.subTitle");
        } else {
            this.title = t("deleteConfirm.title", { context: "multiple" });
            this.subTitleHtml = t("deleteConfirm.subTitleMultiple", { count: this.itemKeys().length });
        }
    }

    deleteItems() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }
}

export = deleteCompareExchangeConfirm;
