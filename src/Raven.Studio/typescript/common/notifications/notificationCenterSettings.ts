
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("notifications");

class notificationCenterSettings {

    static readonly postponeOptions: valueAndLabelItem<number, string>[] = [
        {
            get label() {
                return t("postpone.oneHour");
            },
            value: 3600,
        },
        {
            get label() {
                return t("postpone.sixHours");
            },
            value: 6 * 3600,
        },
        {
            get label() {
                return t("postpone.oneDay");
            },
            value: 24 * 3600,
        },
        {
            get label() {
                return t("postpone.oneWeek");
            },
            value: 7 * 24 * 3600,
        },
    ];
    
}

export = notificationCenterSettings;
