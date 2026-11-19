import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");
import prismjs = require("prismjs");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("showDataDialog");

type supportedLangs = "javascript" | "csharp" | "plain";

class showDataDialog extends dialogViewModelBase {

    view = require("views/common/showDataDialog.html");
    
    private readonly title: string;
    private readonly lang: supportedLangs;

    width = ko.observable<string>("");
    inputData = ko.observable<string>();

    inputDataFormatted = ko.pureComputed(() => {
        const input = this.inputData();
        if (input === undefined) {
            return "";
        }
        if (this.lang === "plain") {
            return generalUtils.escapeHtml(input);
        }
        
        return prismjs.highlight(input, prismjs.languages[this.lang], this.lang);
    });

    

    constructor(title: string, inputData: string, lang: supportedLangs, elementToFocusOnDismissal?: string) {
        super({ elementToFocusOnDismissal: elementToFocusOnDismissal });
        this.lang = lang;
        this.title = title;

        this.inputData(inputData);
    }

    close() {
        dialog.close(this);
    }

    copyToClipboard() {
      copyToClipboard.copy(this.inputData(), t("copied", { title: this.title }), document.getElementById("showDataDialog"));
      this.close();
    }
}

export = showDataDialog; 
