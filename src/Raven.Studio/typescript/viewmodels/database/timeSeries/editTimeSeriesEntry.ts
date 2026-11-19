import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import timeSeriesEntryModel = require("models/database/timeSeries/timeSeriesEntryModel");
import saveTimeSeriesCommand = require("commands/database/documents/timeSeries/saveTimeSeriesCommand");
import popoverUtils = require("common/popoverUtils");
import moment = require("moment");
import typeUtils = require("common/typeUtils");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("editTimeSeries");

class editTimeSeriesEntry extends dialogViewModelBase {
    
    view = require("views/database/timeSeries/editTimeSeriesEntry.html");

    static aggregationColumns = timeSeriesEntryModel.aggregationColumns;
    
    static utcTimeFormat = generalUtils.timeSeriesFullDateFormat;
    static localTimeFormat = generalUtils.timeSeriesFullDateFormat;

    static get incrementalTimeSeriesInfo(): string {
        return `<ul class="margin-top margin-top-xs no-padding-left margin-left">
            <li><small>${t("entry.incrementalInfo.incremental")}</small></li>
            <li><small>${t("entry.incrementalInfo.regular")}</small></li>
         </ul>`;
    }
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    datePickerOptions = {
        format: generalUtils.timeSeriesFullDateFormat,
        sideBySide: true
    };
    
    model = ko.observable<timeSeriesEntryModel>();
    
    dateFormattedAsUtc: KnockoutComputed<string>;
    dateFormattedAsLocal: KnockoutComputed<string>;
    
    lockSeriesName: boolean;
    lockTimeStamp: boolean;

    valuesNames = ko.observableArray<string>([]);

    showValuesPerNode = ko.observable<boolean>(false);
    
    constructor(private documentId: string,
                private db: database,
                private timeSeriesName: string,
                private valuesNamesProvider: (timeseriesName: string) => string[],
                private editDto?: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        super();
        
        this.lockTimeStamp = !!editDto;
        this.lockSeriesName = !!this.timeSeriesName;
        
        const model = editDto 
            ? new timeSeriesEntryModel(timeSeriesName, editDto) 
            : timeSeriesEntryModel.empty(timeSeriesName || undefined);
        
        this.model(model);
        
        this.dateFormattedAsUtc = ko.pureComputed(() => {
            if (model.timestamp()) {
                const date = moment(model.timestamp());
                if (!date.isValid()) {
                    return t("entry.invalidDate");
                }
                return t("dateWithZone", { context: "utc", date: date.utc().format(editTimeSeriesEntry.utcTimeFormat) });
            } else {
                return "";
            }
        });
        
        this.dateFormattedAsLocal = ko.pureComputed(() => {
            const date = moment(model.timestamp());
            return t("dateWithZone", { context: "local", date: date.local().format(editTimeSeriesEntry.localTimeFormat) });
        });

        if (this.timeSeriesName) {
            this.getValuesNames();
        }
        
        this.model().name.subscribe(() => this.getValuesNames());
    }
    
    private getValuesNames(): void {
        const valuesNames = this.valuesNamesProvider(this.model().name());
        this.valuesNames(valuesNames);
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons(".edit-time-series-entry");

        popoverUtils.longWithHover($(".create-incremental"),
            {
                content: editTimeSeriesEntry.incrementalTimeSeriesInfo
            });
    }
    
    getValueName(idx: number) {
        return ko.pureComputed(() => {
            if (this.valuesNames().length) {
                // for an existing timeseries
                return this.valuesNames()[idx];
            } else {
                // for a new timeseries
                const possibleValuesCount = timeSeriesEntryModel.numberOfPossibleValues;
                const possibleValuesNames = typeUtils.range(0, possibleValuesCount).map(idx => t("valueNumber", { index: idx }));
                return possibleValuesNames[idx];
            }
        });
    }

    getValueOnNode(nodeIndex: number, valueIndex: number) {
        return ko.pureComputed(() => this.model().nodesDetails()[nodeIndex].nodeValues[valueIndex]);
    }
    
    save() {
        const model = this.model();
        
        const valid = model.isRollupEntry() ?
            !model.rollupValues().filter(x => !this.isValid(x.validationGroup)).length :
            !model.values().filter(x => !this.isValid(x.validationGroup)).length;
        
        if (!this.isValid(model.validationGroup) || !valid) {
            return false;
        }
        
        this.spinners.save(true);

        new saveTimeSeriesCommand(this.documentId, model.name(), model.toDto(), this.db)
            .execute()
            .done(() => {
                dialog.close(this, model.name());
            })
            .always(() => this.spinners.save(false));
    }

    cancel() {
        dialog.close(this, null);
    }

    deactivate() {
        super.deactivate(null);
    }
}

export = editTimeSeriesEntry;
