import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("editorWarningsConfirm");

class warningsData {
    source: string;
    warnings = ko.observableArray<AceAjax.Annotation>();
}

class editorWarningsConfirm extends dialogViewModelBase {

    view = require("views/database/documents/editorWarningsConfirm.html");

    private textForTitle: string;
    private warningsList = ko.observableArray<warningsData>();
    private readonly onGoto: (warning: AceAjax.Annotation, source?: string) => void;

    constructor(title: string,
                warningsList: Array<warningsData>, 
                onGoto: (warning: AceAjax.Annotation, source?: string) => void) {
        super(null);

        this.textForTitle = title;
        this.warningsList(warningsList);
        this.onGoto = onGoto;
    }

    confirm() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }

    goTo(warning: AceAjax.Annotation, source: string) {
        dialog.close(this, false);
        this.onGoto(warning, source);
    }

    warningLocation(warning: AceAjax.Annotation) {
        return ko.pureComputed(() => {
            return t("location", { line: warning.row + 1, column: warning.column });
        })
    }
}

export = editorWarningsConfirm;
