import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import notificationCenter = require("common/notifications/notificationCenter");
import getIndexNamesCommand = require("commands/database/index/getIndexNamesCommand");
import compactDatabaseCommand = require("commands/resources/compactDatabaseCommand");
import genUtils = require("common/generalUtils");
import dialog = require("plugins/dialog");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import databases = require("components/models/databases");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("databases");

class compactDatabaseDialog extends dialogViewModelBase {

    view = require("views/resources/compactDatabaseDialog.html");
    
    database: databases.DatabaseSharedInfo;
    
    shard = ko.observable<number>();
    
    allIndexes = ko.observableArray<string>([]);
    indexesToCompact = ko.observableArray<string>([]);
        
    compactDocuments = ko.observable<boolean>(true);
    compactAllIndexes = ko.observable<boolean>();

    skipOptimizeIndexes = ko.observable<boolean>(false);

    filterText = ko.observable<string>();
    filteredIndexes: KnockoutComputed<Array<string>>;

    compactEnabled: KnockoutComputed<boolean>;
    selectAllIndexesEnabled: KnockoutComputed<boolean>;
    
    numberOfIndexesFormatted: KnockoutComputed<string>;
    numberOfSelectedIndexesFormatted: KnockoutComputed<string>;
    
    numberOfNodes: KnockoutComputed<number>;
    currentNodeTag: KnockoutComputed<string>;
    
    shards: number[] = [];

    titleHtml: string;
    selectShardPlaceholder: string;
    offlineWarningHtml: string;
    compressionWarningHtml: string;

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        shard: this.shard,
    });
    
    constructor(db: databases.DatabaseSharedInfo, initialShardNumber?: number) {
        super();
        
        this.shards = db.isSharded ? db.shards.filter(x => x.currentNode.isRelevant).map(x => DatabaseUtils.default.shardNumber(x.name)) : [];
        this.shard(initialShardNumber ?? undefined);
        
        this.database = db;
        this.titleHtml = t("compactDialog.title", { name: genUtils.escapeHtml(db.name) });
        this.selectShardPlaceholder = t("compactDialog.selectShard");
        this.offlineWarningHtml = t("compactDialog.offlineWarning");
        this.compressionWarningHtml = t("compactDialog.compressionWarning");
        this.initObservables();
        this.initValidation();
    }

    activate() {
        new getIndexNamesCommand(databasesManager.default.getDatabaseByName(this.database.name))
            .execute()
            .done(indexNames => {
                this.allIndexes(indexNames);
                this.compactAllIndexes(!!indexNames.length);
            });
    }

    protected initObservables() {
        this.numberOfIndexesFormatted = ko.pureComputed(() => {
            return `(${genUtils.formatAsCommaSeperatedString(this.allIndexes().length, 0)})`;
        })

        this.numberOfSelectedIndexesFormatted = ko.pureComputed(() => {
            const count = this.indexesToCompact().length;
            return t("compactDialog.selectedIndexes", {
                count,
                formattedCount: genUtils.formatAsCommaSeperatedString(count, 0),
            });
        })
        
        this.compactEnabled = ko.pureComputed(() => {
            return this.compactDocuments() || !!this.indexesToCompact().length;
        })
        
        this.filteredIndexes = ko.pureComputed(() => {
            const filter = this.filterText();
            
            if (!this.filterText()) {
                return this.allIndexes();
            }
            
            return this.allIndexes().filter(x => x.toLowerCase().includes(filter.toLowerCase()));
        });

        this.compactAllIndexes.subscribe(compactAll => {
            this.indexesToCompact(compactAll ? [...this.allIndexes()] : []);
        });
        
        this.selectAllIndexesEnabled = ko.pureComputed(() => {
            return !_.isEqual(this.allIndexes(), this.indexesToCompact());
        });
        
        this.numberOfNodes = ko.pureComputed(() => {
            return clusterTopologyManager.default.topology().nodes().length;
        });

        this.currentNodeTag = ko.pureComputed(() => {
            return clusterTopologyManager.default.topology().nodeTag();
        });
    }   
    
    private initValidation() {
        this.shard.extend({
            required: {
                onlyIf: () => this.database.isSharded
            },
        })
    }
    
    compactDatabase() {
        if (this.isValid(this.validationGroup)) {
            const effectiveDbName = this.database.isSharded ? this.database.name + "$" + this.shard() : this.database.name;
            new compactDatabaseCommand(effectiveDbName, this.compactDocuments(), this.indexesToCompact(), this.skipOptimizeIndexes())
                .execute()
                .done(result => {
                    notificationCenter.instance.monitorOperation(null, result.OperationId)

                    notificationCenter.instance.openDetailsForOperationById(null, result.OperationId);

                    dialog.close(this);
                })
        }
    }

    selectAllIndexes() {
        this.indexesToCompact([...this.allIndexes()]);
    }
}

export = compactDatabaseDialog;
