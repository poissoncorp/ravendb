/// <reference path="../../../../typings/tsd.d.ts"/>
import timeValueEntry = require("models/database/documents/timeValueEntry");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsModels");

class timeSeriesPolicy {

    static allTimeUnits: Array<valueAndLabelItem<timeUnit | "custom", string>> = [
        {
            get label() {
                return t("policy.timeUnits.seconds");
            },
            value: "second"
        }, {
            get label() {
                return t("policy.timeUnits.minutes");
            },
            value: "minute",
        }, {
            get label() {
                return t("policy.timeUnits.hours");
            },
            value: "hour"
        }, {
            get label() {
                return t("policy.timeUnits.days");
            },
            value: "day"
        }, {
            get label() {
                return t("policy.timeUnits.months");
            },
            value: "month"
        }, {
            get label() {
                return t("policy.timeUnits.years");
            },
            value: "year"
        }, {
            get label() {
                return t("policy.timeUnits.custom");
            },
            value: "custom"
        }
    ];
    
    name = ko.observable<string>();
    
    previous = ko.observable<timeSeriesPolicy>();

    hasRetention = ko.observable<boolean>();
    retention = ko.observable<timeValueEntry>();
    retentionFormatted: KnockoutComputed<string>;
    retentionLabel: KnockoutComputed<string>;
    
    hasAggregation: boolean; 
    aggregation = ko.observable<timeValueEntry>();
    aggregationFormatted: KnockoutComputed<string>;
    aggregationLabel: KnockoutComputed<string>;
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        name: this.name,
        retention: this.retention,
        aggregation: this.aggregation
    });
    
    constructor(dto: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesPolicy) {
        this.name(dto.Name);
        
        this.hasAggregation = !!dto.AggregationTime;
        this.hasRetention(!!dto.RetentionTime && !timeValueEntry.isMax(dto.RetentionTime));
        
        this.aggregation(timeValueEntry.from(dto.AggregationTime));
        this.retention(timeValueEntry.from(dto.RetentionTime));
        
        this.retentionFormatted = ko.pureComputed(() => this.retention().format());
        this.aggregationFormatted = ko.pureComputed(() => this.aggregation().format());
        
        this.retentionLabel = ko.pureComputed(
            () => timeSeriesPolicy.allTimeUnits.find(x => this.retention().unit() === x.value).label);
        this.aggregationLabel = ko.pureComputed(
            () => timeSeriesPolicy.allTimeUnits.find(x => this.aggregation().unit() === x.value).label);

        this.hasRetention.subscribe((toggledOn) => {
            if (toggledOn) {
                this.retention().unit("day");
            }
        });
        
        this.initPolicyValidation();
    }
    
    private initPolicyValidation() {
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => {
                        if (!val) {
                            return true;
                        }
                        
                        const previousNames = this.getPreviousItems().map(x => x.name().toLocaleLowerCase());
                        return !_.includes(previousNames, val.toLocaleLowerCase());
                    },
                    message: t("policy.nameNotUnique")
                }]
        });
        
        this.aggregation.extend({
            validation: [
                { 
                    validator: (time: timeValueEntry) => !this.hasAggregation ||
                                                         (this.aggregation().unit() !== 'second' && this.aggregation().unit() !== 'month') ||
                                                         time.amount() >= 1,
                    message: t("policy.minimumValue")
                }, 
                {
                    validator: (time: timeValueEntry) => !this.hasAggregation || time.isPositive(),
                    message: t("policy.aggregationNotPositive")
                },
                {
                    validator: (time: timeValueEntry) => !this.hasAggregation ||
                                                         (this.aggregation().unit() !== 'second' && this.aggregation().unit() !== 'month') ||
                                                         time.amount() % 1 === 0,
                    message: t("policy.wholeNumber"),
                },
                {
                    validator: (time: timeValueEntry) => {
                        const previous = this.previous();
                        if (!previous || !previous.hasAggregation) {
                            return true;
                        }
                        
                        try {
                            return time.compare(previous.aggregation()) > 0;
                        } catch (e) {
                            // when unable to compare
                            return false;
                        }
                    },
                    message: () => t("policy.aggregationNotGreaterThanPrevious", { time: this.previous().aggregationFormatted() })
                },
                {
                    validator: (time: timeValueEntry) => {
                        const previous = this.previous();
                        if (previous != null) {
                            previous.retention(); // register dependency
                        }
                        
                        if (!previous || !previous.hasRetention()) {
                            return true;
                        }

                        try {
                            return previous.retention().compare(time) > 0;
                        } catch (e) {
                            // when unable to compare
                            return false;
                        }
                    },
                    message: () => t("policy.aggregationNotLessThanPreviousRetention", { time: this.previous().retentionFormatted() })
                },
                {
                    validator: (time: timeValueEntry) => {
                        const previous = this.previous();
                        if (!previous || !previous.hasAggregation) {
                            return true;
                        }

                        return time.isMultipleOf(previous.aggregation());
                    },
                    message: t("policy.aggregationNotMultiple")
                }
            ]
        });
        
        this.retention.extend({
            validation: [
                {
                    validator: (time: timeValueEntry) => !this.hasRetention ||
                                                         (this.retention().unit() !== 'second' && this.retention().unit() !== 'month') ||
                                                         time.amount() >= 1,
                    message: t("policy.minimumValue"),
                },
                {
                    validator: (time: timeValueEntry) => !this.hasRetention() || time.isPositive(),
                    message: t("policy.retentionNotPositive")
                },
                {
                    validator: (time: timeValueEntry) => !this.hasRetention ||
                        (this.retention().unit() !== 'second' && this.retention().unit() !== 'month') ||
                        time.amount() % 1 === 0,
                    message: t("policy.wholeNumber"),
                },
            ]
        })
    }
    
    private getPreviousItems() {
        const result: timeSeriesPolicy[] = [];
        
        let previous = this.previous();
        while (previous != null) {
            result.push(previous);
            previous = previous.previous();
        }
        
        return result;
    }
    
    copyFrom(incoming: timeSeriesPolicy): this {
        if (incoming) {
            this.name(incoming.name());

            this.hasRetention(incoming.hasRetention());
            this.retention(incoming.retention().clone());

            this.aggregation(incoming.aggregation().clone());
        }
                
        return this;
    }

    toDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesPolicy {
        return {
            Name: this.name(),
            RetentionTime: this.hasRetention() ? this.retention().toDto() : {
                Value: generalUtils.integerMaxValue,
                Unit: "None"
            },
            AggregationTime: this.hasAggregation ? this.aggregation().toDto() : null
        }
    }

    static empty() {
        return new timeSeriesPolicy({
            RetentionTime: null,
            AggregationTime: {
                Unit: "Second",
                Value: 3600 // 1 hr
            },
            Name: null
        });
    }
}

export = timeSeriesPolicy;
