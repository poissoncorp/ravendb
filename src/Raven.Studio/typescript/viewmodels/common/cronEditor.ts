/// <reference path="../../../typings/tsd.d.ts" />

import getCronExpressionOccurrenceCommand = require("commands/database/tasks/getCronExpressionOccurrenceCommand");
import generalUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import moment = require("moment");
import cronstrue = require("cronstrue");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("cronEditor");

type cronPeriod = "minute" | "hour" | "day" | "week" | "month" | "year" | "custom";

class cronEditor {
    
    view = require("views/common/cronEditor.html");
    
    getView() {
        return this.view;
    }
    
    targetField: KnockoutObservable<string>;
    
    static periods: cronPeriod[] = ["minute", "hour", "day", "week", "month", "year", "custom"];
    
    static hours:  valueAndLabelItem<string, string>[] = [];
    static minutes: number[] = [];
    static daysOfWeek: valueAndLabelItem<string, string>[] = [];
    static daysOfMonth: number[] = [];
    static months: valueAndLabelItem<string, string>[] = [];
    
    period = ko.observable<cronPeriod>("minute");
    minutes = ko.observable<string>("0");
    hour = ko.observable<string>("0");
    dayOfWeek = ko.observable<string>("0");
    dayOfMonth = ko.observable<string>("1");
    month = ko.observable<string>("1");
    
    minutesVisible: KnockoutComputed<boolean>;
    timeVisible: KnockoutComputed<boolean>;
    dayOfWeekVisible: KnockoutComputed<boolean>;
    dayOfMonthVisible: KnockoutComputed<boolean>;
    monthVisible: KnockoutComputed<boolean>;

    humanReadableVisible: KnockoutComputed<boolean>;
    textInputVisible: KnockoutComputed<boolean>;
    
    monthLabel = ko.pureComputed(() => cronEditor.months.find(x => x.value === this.month()).label);
    dayOfWeekLabel = ko.pureComputed(() => cronEditor.daysOfWeek.find(x => x.value === this.dayOfWeek()).label);
    hourLabel = ko.pureComputed(() => cronEditor.hours.find(x => x.value === this.hour()).label);
    periodLabel = ko.pureComputed(() => this.getPeriodLabel(this.period()));
    
    humanReadable: KnockoutComputed<string>;
    parsingError = ko.observable<string>();
    notAvailable = t("notAvailable");
    nextOccurrenceServerTime = ko.observable<string>(this.notAvailable);
    nextOccurrenceLocalTime = ko.observable<string>();
    nextInterval = ko.observable<string>();
    canDisplayNextOccurrenceLocalTime: KnockoutComputed<boolean>;

    private readonly databaseName: string;
    private readonly taskId: number;
    private readonly isFull: boolean;

    constructor(targetField: KnockoutObservable<string>);
    constructor(targetField: KnockoutObservable<string>, databaseName: string, taskId: number, isFull: boolean);
    
    constructor(
        targetField: KnockoutObservable<string>,
        databaseName?: string,
        taskId?: number,
        isFull?: boolean
    ) {
        this.targetField = targetField;
        this.databaseName = databaseName;
        this.taskId = taskId;
        this.isFull = isFull;
        
        this.populateForm();
        
        this.initObservables();
    }
    
    getPeriodLabel(period: cronPeriod) {
        return t("period", { context: period });
    }

    attached(view: HTMLElement) {
        this.initTooltip(view);
    }
    
    private populateForm() {
        const value = this.targetField();
     
        if (value) {
            const parts = value.split(" ");
            if (parts.length !== 5) {
                this.period("custom");
            } else {
                if (/^(\*\s){4}\*$/.test(value)) { // "* * * * *"
                    this.period("minute");
                } else if (/^\d{1,2}\s(\*\s){3}\*$/.test(value)) { // "? * * * *"
                    this.period("hour");
                    this.minutes(parts[0]);
                } else if (/^(\d{1,2}\s){2}(\*\s){2}\*$/.test(value)) {// "? ? * * *"
                    this.period("day");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                } else if (/^(\d{1,2}\s){2}(\*\s){2}\d{1,2}$/.test(value)) { // "? ? * * ?"
                    this.period("week");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                    const dayOfWeek = parts[4] === "7" ? "0" : parts[4]; // normalize week day
                    this.dayOfWeek(dayOfWeek);
                } else if (/^(\d{1,2}\s){3}\*\s\*$/.test(value)) { // "? ? ? * *"
                    this.period("month");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                    this.dayOfMonth(parts[2]);
                } else if (/^(\d{1,2}\s){4}\*$/.test(value)) { // "? ? ? ? *"
                    this.period("year");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                    this.dayOfMonth(parts[2]);
                    this.month(parts[3]);
                } else {
                    this.period("custom");
                }
            }
        } else {
            this.period("minute");
        }
    }
    
    private initObservables() {
        this.minutesVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "hour";
        });

        this.timeVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "day" || period === "week" || period === "month" || period === "year";
        });

        this.dayOfWeekVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "week";
        });

        this.dayOfMonthVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "month" || period === "year";
        });

        this.monthVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "year";
        });
        
        this.period.subscribe(() => this.update());
        this.minutes.subscribe(() => this.update());
        this.hour.subscribe(() => this.update());
        this.dayOfWeek.subscribe(() => this.update());
        this.dayOfMonth.subscribe(() => this.update());
        this.month.subscribe(() => this.update());

        this.humanReadable = ko.pureComputed(() => cronEditor.getHumanReadable(this.targetField(), this.parsingError));
        this.humanReadableVisible = ko.pureComputed(() => this.period() === "custom");
        this.textInputVisible = ko.pureComputed(() => this.period() === "custom");
        
        this.targetField.throttle(500).subscribe(() => this.updateNextOccurence());
        
        if (this.targetField()) {
            this.updateNextOccurence();
        }

        this.canDisplayNextOccurrenceLocalTime = ko.pureComputed(() => this.nextOccurrenceLocalTime() !== this.nextOccurrenceServerTime());
        
        this.targetField.extend({
            validation: [
                {
                    validator: () => !this.parsingError(),
                    message: `{0}`,
                    params: this.parsingError
                }
            ]
        })
    }
    
    initTooltip(container: HTMLElement) {
        const $container = $(container);
        popoverUtils.longWithHover($(".js-schedule-info", $container),
            {
                content:
                    "<div class='schedule-info-text'>" +
                    t("scheduleInfo.definition") + "<br/>" +
                    t("scheduleInfo.fields")
                        .replace("<fields>", "<span style='color: #B9F4B7'>")
                        .replace("</fields>", "</span>") + "<br/>" +
                    t("scheduleInfo.values")
                        .replace("<values>", "<span style='color: #f9d291'>")
                        .replace("</values>", "</span>")
                        .replace("<special>", "<span style='color: white'>")
                        .replace("</special>", "</span>") + "<br/>" +
                    "<pre>" +
                    `+----------------> ${t("scheduleInfo.minute")} (<span class='values'>0 - 59</span>) (<span class='special-characters'>, - * /</span>)<br/>` +
                    `|  +-------------> ${t("scheduleInfo.hour")} (<span class='values'>0 - 23</span>) (<span class='special-characters'>, - * /</span>)<br/>` +
                    `|  |  +----------> ${t("scheduleInfo.dayOfMonth")} (<span class='values'>1 - 31</span>) (<span class='special-characters'>, - * ? / L W</span>)<br/>` +
                    `|  |  |  +-------> ${t("scheduleInfo.month")} (<span class='values'>${t("scheduleInfo.monthValues")}</span>) (<span class='special-characters'>, - * /</span>)<br/>` +
                    `|  |  |  |  +----> ${t("scheduleInfo.dayOfWeek")} (<span class='values'>${t("scheduleInfo.dayOfWeekValues")}</span>) (<span class='special-characters'>, - * ? / L #</span>)<br/>` +
                    "|  |  |  |  |<br/>" +
                    "<span style='color: #B9F4B7'>" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>" +
                    "</span></pre><br/>" +
                    t("scheduleInfo.moreInfo").replace("<a>", "<a href='https://ravendb.net/l/X6IBEZ' target='_blank'>") +
                    "</div>"
            });
    }
    
    private updateNextOccurence() {
        const cronExpression = this.targetField();

        if (this.parsingError() || !cronExpression) {
            this.nextOccurrenceServerTime(this.notAvailable);
            this.nextOccurrenceLocalTime("");
            this.nextInterval("");
            return;
        }

        const dateFormat = generalUtils.dateFormat;
        new getCronExpressionOccurrenceCommand(cronExpression, this.databaseName, this.taskId, this.isFull)
            .execute()
            .done((result: Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence) => {
                if (result.IsValid) {
                    const nextOccurrenceUtc = moment.utc(result.Utc);
                    const nowUtc = moment.utc();

                    this.nextOccurrenceServerTime(moment(result.ServerTime).format(dateFormat));
                    this.nextOccurrenceLocalTime(nextOccurrenceUtc.local().format(dateFormat));

                    if (nextOccurrenceUtc.isBefore(nowUtc)) {
                        this.nextInterval(t("willRunOnSave"));
                    } else {
                        const fromDuration = generalUtils.formatDurationByDate(nextOccurrenceUtc, true);
                        this.nextInterval(`${fromDuration}`);
                    }

                    this.parsingError(null);
                } else {
                    this.nextOccurrenceServerTime(this.notAvailable);
                    this.nextOccurrenceLocalTime("");
                    this.nextInterval("");
                    this.parsingError(result.ErrorMessage);
                }
            });
    }

    private static getHumanReadable(frequency: string,
                                    parsingError: KnockoutObservable<string>): string {
        if (!frequency) {
            parsingError(null);
            return t("notAvailable");
        }

        const frequencySplit = frequency.trim().replace(/ +(?= )/g, "").split(" ");
        if (frequencySplit.length < 5) {
            parsingError(t("errors.tooFewParts", { count: frequencySplit.length }));
            return t("notAvailable");
        } else if (frequencySplit.length > 5) {
            parsingError(t("errors.tooManyParts", { count: frequencySplit.length }));
            return t("notAvailable");
        }

        try {
            const result = cronstrue.toString(frequency.toUpperCase());
            if (result.includes("undefined")) {
                parsingError(t("errors.invalidExpression"));
                return t("notAvailable");
            }

            parsingError(null);
            return result;
        } catch (error) {
            parsingError(error);
            return t("notAvailable");
        }
    }
    
    private update() {
        this.targetField(this.getCronExpression());
    }
    
    private getCronExpression() {
        switch (this.period()) {
            case "minute":
                return "* * * * *";
            case "hour":
                return this.minutes() + " * * * *";
            case "day":
                return this.minutes() + " " + this.hour() + " * * *";
            case "week":
                return this.minutes() + " " + this.hour() + " * * " + this.dayOfWeek();
            case "month":
                return this.minutes() + " " + this.hour() + " " + this.dayOfMonth() + " * *";
            case "year":
                return this.minutes() + " " + this.hour() + " " + this.dayOfMonth() + " " + this.month() + " *";
            case "custom":
                return this.targetField();
        }
    }

}

for (let i = 0; i < 60; i++) {
    cronEditor.minutes.push(i);
}

for (let i = 0; i < 24; i++) {
    const hour = i % 12;
    
    cronEditor.hours.push({
        get label() {
            return t("hour", { hour: hour || "12", context: i < 12 ? "am" : "pm" });
        },
        value: i.toString()
    });
}

for (let i = 1; i <= 31; i++) {
    cronEditor.daysOfMonth.push(i);
}

([
    "months.january", "months.february", "months.march", "months.april",
    "months.may", "months.june", "months.july", "months.august",
    "months.september", "months.october", "months.november", "months.december"
] as const).forEach((monthKey, idx) => {
    cronEditor.months.push({
        get label() {
            return t(monthKey);
        },
        value: (idx + 1).toString()
    });  
});

([
    "daysOfWeek.sunday", "daysOfWeek.monday", "daysOfWeek.tuesday", "daysOfWeek.wednesday",
    "daysOfWeek.thursday", "daysOfWeek.friday", "daysOfWeek.saturday"
] as const).forEach((dayOfWeekKey, idx) => {
    cronEditor.daysOfWeek.push({
        get label() {
            return t(dayOfWeekKey);
        },
        value: idx.toString()
    });
})

export = cronEditor;
