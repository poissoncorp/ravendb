import appUrl = require("common/appUrl");

import durandalRouter = require("plugins/router");
import shardingContext = require("viewmodels/common/sharding/shardingContext");
import viewModelBase = require("viewmodels/viewModelBase");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("importParent");

class importParent extends viewModelBase {

    context: shardingContext;
    
    view = require("views/database/tasks/importParent.html");

    importDataOptionsUrl = appUrl.forCurrentDatabase().importDataOptionsUrl;

    getView() {
        return this.view;
    }
    
    router: DurandalRootRouter;

    constructor() {
        super();
        
        this.context = new shardingContext("allShards");
        
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'databases/tasks/import/migrateRavenDB',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/migrateRavenDbDatabase')),
                    title: t("routes.migrateRavenDb"),
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().migrateRavenDbDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/importCollectionFromCsv')),
                    title: t("routes.importCsv"),
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().importCollectionFromCsv,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/sql',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/importDatabaseFromSql')),
                    title: t("routes.importSql"),
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromSql,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/migrateDatabase')),
                    title: t("routes.migrate"),
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().migrateDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                }
            ])
            .buildNavigationModel();
    }
    
    wrapModuleId(item: any) {
        const container = require('viewmodels/common/sharding/shardAwareContainer');
        return new container("both", item, this.context);
    }
}

export = importParent; 
