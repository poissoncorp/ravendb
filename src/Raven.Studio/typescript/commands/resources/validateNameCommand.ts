import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class validateNameCommand extends commandBase {

    private type: Raven.Server.Web.Studio.StudioTasksHandler.ItemType;

    private name: string;

    private dataPath?: string;

    constructor(type: Raven.Server.Web.Studio.StudioTasksHandler.ItemType, name: string, dataPath?: string) {
        super();
        this.dataPath = dataPath;
        this.name = name;
        this.type = type;
    }

    execute(): JQueryPromise<Raven.Client.Util.NameValidation> {
        const args = {
            type: this.type,
            name: this.name,
            dataPath: this.dataPath || ""
        };
        
        const url = endpoints.global.studioTasks.studioTasksIsValidName + this.urlEncodeArgs(args); 
        
        return this.query<Raven.Client.Util.NameValidation>(url, null)
           .fail((response: JQueryXHR) => { 
               this.reportError(t("validateNameFailed", { context: this.type }), response.responseText, response.statusText);
            });
    }
}

export = validateNameCommand;
