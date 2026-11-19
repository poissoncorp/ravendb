import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("secretsCommands");

class distributeSecretCommand extends commandBase {

    private name: string;

    private secret: string;

    private nodeTags: string[];

    constructor(name: string, secret: string, nodeTags: string[]) {
        super();
        this.nodeTags = nodeTags;
        this.secret = secret;
        this.name = name;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name,
            node: this.nodeTags
        };
        const url = endpoints.global.secretKey.adminSecretsDistribute + this.urlEncodeArgs(args);
        
        return this.post<void>(url, this.secret, null, { dataType: 'text' })
            .fail((response: JQueryXHR) => this.reportError(t("distributeFailed"), response.responseText, response.statusText));
    }
}

export = distributeSecretCommand;
