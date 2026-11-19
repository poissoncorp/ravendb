import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("editIndex");

class indexInfoForDelete {
    indexName: string;
    reduceOutputCollection: string;
    referenceCollection: string;
    lockMode: IndexLockMode;

    readonly referenceCollectionExtension = "/References";
    
    constructor(indexName: string, reduceOutputCollection: string, hasPatternForReduceOutputCollection: boolean, lockMode: IndexLockMode) {
        this.indexName = indexName;
        this.reduceOutputCollection = reduceOutputCollection;
        this.referenceCollection = hasPatternForReduceOutputCollection ? reduceOutputCollection + this.referenceCollectionExtension : "";
        this.lockMode = lockMode;
    }
}

interface IndexToDelete {
    //TODO: remove observable
    name: KnockoutObservable<string> | string;
    reduceOutputCollectionName: KnockoutObservable<string> | string;
    patternForReferencesToReduceOutputCollection: KnockoutObservable<string> | string;
    lockMode: KnockoutObservable<IndexLockMode> | IndexLockMode;
}

class deleteIndexesConfirm extends dialogViewModelBase {

    view = require("views/database/indexes/deleteIndexesConfirm.html");
    
    title: string;
    subTitleHtml: string;
    showWarning = false;
    warningNoteHtml = t("deleteConfirm.warning.note");
    warningImportantHtml = t("deleteConfirm.warning.important");
    deleteTask = $.Deferred<boolean>();

    indexesInfoForDelete: indexInfoForDelete[] = [];
    lockedIndexNames: string[] = [];

    private indexes: IndexToDelete[];

    private db: database;

    constructor(indexes: IndexToDelete[], db: database) {
        super();
        this.db = db;
        this.indexes = indexes;

        if (!indexes || indexes.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }
        
        const allIndexes = indexes
            .map(x => new indexInfoForDelete(
                ko.unwrap(x.name),
                ko.unwrap(x.reduceOutputCollectionName),
                !!ko.unwrap(x.patternForReferencesToReduceOutputCollection),
                ko.unwrap(x.lockMode)
            ));

        allIndexes.forEach(index => {
            if (index.lockMode === "LockedError" || index.lockMode === "LockedIgnore") {
                this.lockedIndexNames.push(index.indexName);
                return;
            }

            this.indexesInfoForDelete.push(index);
        })
        
        if (this.indexesInfoForDelete.length === 1) {
            this.title = t("deleteConfirm.title", { context: "single" });
            this.subTitleHtml = t("deleteConfirm.subTitle");
        } else {
            this.title = t("deleteConfirm.title", { context: "multiple" });
            this.subTitleHtml = t("deleteConfirm.subTitleMultiple", { count: this.indexesInfoForDelete.length });
        }

        this.showWarning = this.indexesInfoForDelete.some(x => x.reduceOutputCollection);
    }

    deleteIndexes() {
        const deleteTasks = this.indexesInfoForDelete.map(indexItem => new deleteIndexCommand(indexItem.indexName, this.db).execute());

        // eslint-disable-next-line prefer-spread
        $.when.apply($, deleteTasks)
            .done(() => {
                if (this.indexesInfoForDelete.length > 1) {
                    messagePublisher.reportSuccess(t("deleteConfirm.success", { count: this.indexesInfoForDelete.length }));
                }
                this.deleteTask.resolve(true);
            })
            .fail(() => this.deleteTask.reject());

        dialog.close(this);
    }

    cancel() {
        this.deleteTask.resolve(false);
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;
