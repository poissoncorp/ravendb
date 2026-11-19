/// <reference path="../../../../typings/tsd.d.ts"/>
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsModels");

class timeSeriesNamedItem {
    name = ko.observable<string>("");
    
    hasFocus = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        name: this.name
    });
    
    constructor(name: string) {
        this.name(name);

        this.initValidation();
    }
    
    initValidation() {
        this.name.extend({
            required: true,
            
        });
    }
    
    copyFrom(incoming: timeSeriesNamedItem): this {
        this.name(incoming.name());
        return this;
    }

    static empty() {
        return new timeSeriesNamedItem("");
    }
}

class timeSeriesNamedValues {
    timeSeriesName = ko.observable<string>("");
    
    namedValues = ko.observableArray<timeSeriesNamedItem>([]);

    namedValuesAsText: KnockoutComputed<string>;
    
    hasFocus = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        timeSeriesName: this.timeSeriesName,
        namedValues: this.namedValues
    });
    
    constructor(timeSeriesName: string, namedValues: string[]) {
        this.timeSeriesName(timeSeriesName);
        this.namedValues(namedValues.map(x => new timeSeriesNamedItem(x)));
        
        this.initValidation();
        
        this.namedValues.isModified(false);
        
        this.namedValuesAsText = ko.pureComputed(() => this.namedValues().map(x => x.name()).join(", "));
        
        _.bindAll(this, "addMapping", "removeMapping");
    }
    
    private initValidation() {
        this.timeSeriesName.extend({
            required: true
        });
        
        this.namedValues.extend({
            validation: [
                {
                    validator: () => this.namedValues().length > 0,
                    message: t("namedValues.valueRequired")
                },
                {
                    validator: () => {
                        const nonEmptyItems = this.namedValues()
                            .map(x => x.name())
                            .filter(x => x);
                        
                        return nonEmptyItems.length === 0 || _.uniq(nonEmptyItems).length === nonEmptyItems.length;
                    },
                    message: t("namedValues.namesNotUnique")
                }
            ]
        })
    }
    
    addMapping() {
        const item = timeSeriesNamedItem.empty();
        item.hasFocus(true);
        this.namedValues.push(item);
    }

    removeMapping(item: timeSeriesNamedItem) {
        this.namedValues.remove(item);
    }

    copyFrom(incoming: timeSeriesNamedValues): this {
        this.timeSeriesName(incoming.timeSeriesName());
        
        this.namedValues(incoming.namedValues().map(x => timeSeriesNamedItem.empty().copyFrom(x)));
        
        return this;
    }
    
    static empty() {
        return new timeSeriesNamedValues("", []);
    }
}

export = timeSeriesNamedValues;
