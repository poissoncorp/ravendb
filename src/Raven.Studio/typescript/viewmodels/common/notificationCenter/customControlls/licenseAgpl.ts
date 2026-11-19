/// <reference path="../../../../../typings/tsd.d.ts" />

import registration = require("viewmodels/shell/registration");
import license = require("models/auth/licenseModel");
import moment = require("moment");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licenseAgpl");

class licenseAgpl  {
    
    view = require("views/common/notificationCenter/customControlls/licenseAgpl.html");
    
    getView() {
        return this.view;
    }

    licensedUnderHtml = t("licensedUnder").replace(
        "<a>",
        `<a href="https://www.gnu.org/licenses/agpl-3.0.en.html" target="_blank">`
    );
    
    canUseUntil = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return null;
        }

        const now = moment.utc();
        const firstStart = moment.utc(licenseStatus.FirstServerStartDate);
        const weekAfterFirstStart = firstStart.clone().add("1", "week");
        const dateFormat = "YYYY-MM-DD HH:mm:ss";

        if (!now.isBefore(weekAfterFirstStart)) {
            return null;
        }

        const utcDate = weekAfterFirstStart.format(dateFormat);
        const localDate = weekAfterFirstStart.local().format(dateFormat);
        const tooltip = t("availableUntilTooltip", { utcDate, localDate });

        return t("availableUntil", { date: generalUtils.escapeHtml(utcDate) }).replace(
            "<icon/>",
            `<i class="icon-info text-info" title="${generalUtils.escapeHtml(tooltip)}"></i>`
        );
    });
    
    register() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true);
    }
}

export = licenseAgpl;
