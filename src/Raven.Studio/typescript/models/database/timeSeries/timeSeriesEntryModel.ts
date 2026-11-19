import timeSeriesValue = require("models/database/timeSeries/timeSeriesValue");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import typeUtils = require("common/typeUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("timeSeriesModels");

class rollupDataModel {
    first: timeSeriesValue;
    last: timeSeriesValue;
    min: timeSeriesValue;
    max: timeSeriesValue;
    sum: timeSeriesValue;
    count = ko.observable<number>();

    validationGroup: KnockoutValidationGroup;
    
    constructor(...values: number[]) {
        this.first = new timeSeriesValue(values[0]);
        this.last = new timeSeriesValue(values[1]);
        this.min = new timeSeriesValue(values[2]);
        this.max = new timeSeriesValue(values[3]);
        this.sum = new timeSeriesValue(values[4]);
        this.count(values[5] || 0);

        this.count.extend({
            required: true,
            digit: true
        });

        this.validationGroup = ko.validatedObservable({
            first: this.first.value,
            last: this.last.value,
            min: this.min.value,
            max: this.max.value,
            sum: this.sum.value,
            count: this.count
        });
    }
    
    getValues(): number[] {
        return [this.first.value(),
                this.last.value(),
                this.min.value(),
                this.max.value(),
                this.sum.value(),
                this.count()];
    }
}

interface nodeData {
    nodeTag: string;
    databaseID: string;
    nodeValues: number[];
}

class timeSeriesEntryModel {
    
    static aggregationColumns = ["First", "Last", "Min", "Max", "Sum", "Count"];
    static readonly numberOfPossibleValues = 32;
    static readonly numberOfPossibleRollupValues = 5;
    static readonly incrementalPrefix = "INC:";

    static isIncrementalName(timeSeriesName: string): boolean {
        return timeSeriesName.toUpperCase().startsWith(timeSeriesEntryModel.incrementalPrefix);
    }

    static isRollupName(timeSeriesName: string): boolean {
        return timeSeriesName.includes("@");
    }
    
    name = ko.observable<string>();
    tag = ko.observable<string>();
    timestamp = ko.observable<moment.Moment>();

    values = ko.observableArray<timeSeriesValue>([]);
    rollupValues = ko.observableArray<rollupDataModel>([]);
    nodesDetails = ko.observableArray<nodeData>([]);
    
    isCreatingNewTimeSeries = ko.observable<boolean>();
    createIncrementalTimeSeries = ko.observable<boolean>();
       
    isRollupEntry = ko.observable<boolean>();
    isIncrementalEntry: KnockoutComputed<boolean>;

    entryTitle = ko.observable<string>();
    private isEditEntry = false;
    
    existingNumberOfValues = 0;
    maxNumberOfValuesReachedWarning: KnockoutComputed<string>;
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(timeSeriesName: string, dto: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        this.name(timeSeriesName);
        this.tag(dto.Tag);
        this.timestamp(dto.Timestamp ? moment.utc(dto.Timestamp) : null);
        
        this.isRollupEntry(dto.IsRollup);
        this.isCreatingNewTimeSeries(!timeSeriesName);
        
        if (timeSeriesName && timeSeriesEntryModel.isIncrementalName(timeSeriesName)) {
            const details = Object.entries(dto.NodeValues ?? []).map(([nodeDetails, valuesList]) => {
                const [tag, dbId] = nodeDetails.split('-');
                return {
                    nodeTag: tag,
                    databaseID: dbId,
                    nodeValues: valuesList
                };
            })
            
            const detailsSorted = typeUtils.sortBy(details, x => x.nodeTag);
            this.nodesDetails(detailsSorted);
        }
        
        if (dto.IsRollup) {
            const values = dto.Values;
            
            // Check if rollup data values are Not a multiple of the 6 pre-defined aggregationColumns columns
            // Add null values for the template if needed.
            const currentLength = values.length;
            const remainder = currentLength % timeSeriesEntryModel.aggregationColumns.length;
            const numberOfMissingValues = remainder ? timeSeriesEntryModel.aggregationColumns.length - remainder : 0;
            
            for (let i = 0; i < numberOfMissingValues; i++) {
                values[currentLength + i] = null;
            }
            
            for (let i = 0; i < values.length; i += timeSeriesEntryModel.aggregationColumns.length) {
                const rollup = new rollupDataModel(values[i], values[i+1], values[i+2], values[i+3], values[i+4], values[i+5]);
                this.rollupValues().push(rollup);
            }
        } else {
            this.values(dto.Values.map(x => new timeSeriesValue(x)));
            this.existingNumberOfValues = this.values().length;
        }
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables(): void {
        this.maxNumberOfValuesReachedWarning = ko.pureComputed(() => {
            if (this.isRollupEntry && this.rollupValues().length === timeSeriesEntryModel.numberOfPossibleRollupValues) {
                return t("entry.maxRollupValuesReached", { limit: timeSeriesEntryModel.numberOfPossibleRollupValues });
            }

            if (!this.isRollupEntry() && this.values().length === timeSeriesEntryModel.numberOfPossibleValues) {
                return t("entry.maxValuesReached", { limit: timeSeriesEntryModel.numberOfPossibleValues });
            }

            return "";
        });

        this.isIncrementalEntry  = ko.pureComputed(() => 
            !this.isCreatingNewTimeSeries() && timeSeriesEntryModel.isIncrementalName(this.name()));

        this.isEditEntry = !(this.isCreatingNewTimeSeries() || !this.timestamp());
        const kind = this.isEditEntry ? "edit" : "new";
        const isIncremental = this.createIncrementalTimeSeries() || this.isIncrementalEntry();
        const isRollup = this.isRollupEntry();
        const context = isIncremental
            ? isRollup
                ? (`${kind}IncrementalRollup` as const)
                : (`${kind}Incremental` as const)
            : isRollup
              ? (`${kind}Rollup` as const)
              : kind;
        this.entryTitle(t("entry.title", { context }));
    }

    addValue(): void {
        if (this.isRollupEntry()) {
            const newRollupData = new rollupDataModel();
            this.rollupValues.push(newRollupData);
        } else {
            const newValue = new timeSeriesValue();
            this.values.push(newValue);
        }
    }
    
    removeValue(value: timeSeriesValue) {
        this.values.remove(value);
    }

    removeRollupData(rollup: rollupDataModel): void {
        this.rollupValues.remove(rollup);
    }
    
    removeValueData(value: timeSeriesValue): void {
        this.values.remove(value);
    }
    
    private initValidation(): void {
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: () => !this.isCreatingNewTimeSeries() ||
                                     !timeSeriesEntryModel.isRollupName(this.name()),
                    message: t("validation.nameContainsAt")
                },
                {
                    validator: () => !this.isCreatingNewTimeSeries() ||
                                     !this.createIncrementalTimeSeries() ||
                                      timeSeriesEntryModel.isIncrementalName(this.name()),
                    message: t("validation.incrementalPrefixRequired", { prefix: timeSeriesEntryModel.incrementalPrefix })
                },
                {
                    validator: () => !this.isCreatingNewTimeSeries() ||
                                      this.createIncrementalTimeSeries() ||
                                     !timeSeriesEntryModel.isIncrementalName(this.name()),
                    message: t("validation.regularPrefixNotAllowed", { prefix: timeSeriesEntryModel.incrementalPrefix })
                }
            ]
        });
        
        this.timestamp.extend({
            required: true,
            validation: [
                {
                    validator: () => this.timestamp().isValid(),
                    message: t("validation.invalidDate")
                }
            ]
        });

        this.tag.extend({
            maxLength: 255
        });
        
        this.values.extend({
            validation: [
                {
                    validator: () => this.isRollupEntry() || this.values().length > 0,
                    message: t("validation.valueRequired")
                }
            ]
        });
        
        this.rollupValues.extend({
            validation: [
                {
                    validator: () => !this.isRollupEntry() || this.rollupValues().length > 0,
                    message: t("validation.rollupValueRequired")
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            name: this.name,
            tag: this.tag,
            timestamp: this.timestamp,
            values: this.values,
            rollupValues: this.rollupValues
        });
    }
    
    private flattenRollupValues() {
        return this.rollupValues().reduce((result: number[], next: rollupDataModel) => {
            result.push(...next.getValues());
            return result;
        }, []);
    }

    public toDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation {
        return {
            Tag: this.tag(),
            Timestamp: this.timestamp().utc().format(generalUtils.utcFullDateFormat),
            Values: this.isRollupEntry() ? this.flattenRollupValues() :
                this.values().map(x => this.isIncrementalEntry() && this.isEditEntry ? x.delta() : x.value())
        }
    }
    
    static empty(timeSeriesName: string): timeSeriesEntryModel {
        return new timeSeriesEntryModel(timeSeriesName, {
            Timestamp: null,
            Tag: null,
            Values: [],
            IsRollup: timeSeriesName && timeSeriesName.includes("@"),
            NodeValues: null
        });
    }
}

export = timeSeriesEntryModel;
