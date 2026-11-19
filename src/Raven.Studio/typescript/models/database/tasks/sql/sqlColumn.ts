/// <reference path="../../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");

class sqlColumn {
    sqlName: string;
    propertyName = ko.observable<string>();
    type: Raven.Server.SqlMigration.Schema.ColumnType;
    
    constructor(column: Raven.Server.SqlMigration.Schema.TableColumn) {
        this.sqlName = column.Name;
        this.propertyName(column.Name);
        this.type = column.Type;
    }
    
    clone() {
        return new sqlColumn({
            Name: this.sqlName,
            Type: this.type
        });
    }
    
    columnTypeAsHtml(binaryAsAttachment: boolean) {
        if (binaryAsAttachment && this.type === "Binary") {
            return `: <i title="${generalUtils.escapeHtml(t("sqlMigration.attachment"))}" class="icon-attachment"></i>`;
        }
        return ': ' + this.type;
    }

    isUnsupported() {
        return this.type === "Unsupported";
    }
}


export = sqlColumn;
