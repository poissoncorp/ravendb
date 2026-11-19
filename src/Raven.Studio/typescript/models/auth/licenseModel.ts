/// <reference path="../../../typings/tsd.d.ts" />

import getLicenseStatusCommand = require("commands/licensing/getLicenseStatusCommand");
import buildInfo = require("models/resources/buildInfo");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import licenseSupportInfoCommand = require("commands/licensing/licenseSupportInfoCommand");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authModels");

class licenseModel {
    static licenseStatus = ko.observable<LicenseStatus>();
    static supportCoverage = ko.observable<Raven.Server.Commercial.LicenseSupportInfo>();

    private static baseUrl = "https://ravendb.net/license/request";

    static licenseTypeText = ko.pureComputed(() => licenseModel.licenseTypeTextProvider(licenseModel.licenseStatus())); 

    static licenseTypeTextProvider(licenseStatus: LicenseStatus) {
        if (!licenseStatus || licenseStatus.Type === "None") {
            return t("license.type.none");
        }

        let licenseType = licenseStatus.Type;
        if (licenseType === "Invalid") {
            return t("license.type.invalid");
        }

        if (licenseStatus.IsCloud) {
            if (licenseType === "Community") {
                return t("license.type.cloudFree");
            }

            if (licenseType === "Developer") {
                return t("license.type.cloudDeveloper");
            }

            if (licenseType === "Enterprise") {
                return t("license.type.cloudProduction");
            }

            return t("license.type.cloud");
        }

        if (licenseType === "EnterpriseAi") {
            return "RavenDB AI";
        }

        if (licenseStatus.IsIsv) {
            licenseType += " (ISV)";
        }

        return licenseType;
    }
    
    static licenseTypeShortText = ko.pureComputed(() => licenseModel.licenseTypeShortTextProvider(licenseModel.licenseStatus()));
    
    static licenseTypeShortTextProvider(licenseStatus: LicenseStatus): string {
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "AGPL";
        }
        
        return licenseModel.licenseTypeTextProvider(licenseStatus);
    }
    
    
    static formattedExpiration = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        if (!licenseStatus || !licenseStatus.SubscriptionExpiration) {
            return null;
        }

        const dateFormat = "YYYY MMMM Do";
        const expiration = moment.utc(licenseStatus.SubscriptionExpiration);
        const now = moment.utc();
        const nextMonth = moment.utc().add(1, 'month');
        if (now.isBefore(expiration)) {
            const relativeDurationClass = nextMonth.isBefore(expiration) ? "" : "text-warning";

            const fromDuration = generalUtils.formatDurationByDate(expiration, true);
            return `${generalUtils.escapeHtml(t("license.expirationDate", { date: expiration.format(dateFormat) }))} ${this.getLicenseInfoIcon({ date: expiration, isExpired: false })}<br /><small class="${relativeDurationClass}">(${fromDuration})</small>`;
        }

        const expiredClass = licenseStatus.Expired ? "text-danger" : "";
        const duration = generalUtils.formatDurationByDate(expiration, true);

        return `${generalUtils.escapeHtml(t("license.expirationDate", { date: expiration.format(dateFormat) }))} ${this.getLicenseInfoIcon({ date: expiration, isExpired: true })}<br /><Small class="${expiredClass}">(${duration})</Small>`;
    });

    static getLicenseInfoIcon({ date, isExpired, isSmall = true }: { date: moment.Moment, isExpired: boolean, isSmall?: boolean }): string {
        const title = t("license.expirationInfo", {
            context: isExpired ? "expired" : "active",
            date: date.format("YYYY-MM-DD"),
            localDate: date.local().format("YYYY-MM-DD HH:mm:ss"),
        });
        return `<i class="icon-info text-info"
            title="${generalUtils.escapeHtml(title)}"
            style="font-size: ${isSmall ? "16px" : undefined}">
        </i>`;
    }
        
    static generateLicenseRequestUrl(limitType: Raven.Client.Exceptions.Commercial.LimitType = null): string {
        let url = `${licenseModel.baseUrl}?`;

        const build = buildInfo.serverBuildVersion();
        if (build) {
            url += `&build=${build.BuildVersion}`;
        }

        const status = this.licenseStatus();
        if (status && status.Id) {
            url += `&id=${btoa(status.Id)}`;
        }

        if (limitType) {
            url += `&limit=${btoa(limitType)}`;
        }

        return url;
    }

    static fetchSupportCoverage(): JQueryPromise<Raven.Server.Commercial.LicenseSupportInfo> {
        return new licenseSupportInfoCommand()
            .execute()
            .done((result: Raven.Server.Commercial.LicenseSupportInfo) => {
                licenseModel.supportCoverage(result);
            });
    }
    
    static fetchLicenseStatus(): JQueryPromise<LicenseStatus> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: LicenseStatus) => {
                if (result.Status.includes("AGPL")) {
                    result.Status = "Development Only";
                }
                licenseModel.licenseStatus(result);
            });
    }

    static licenseShortDescription = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        if (!status || status.Type === "None") {
            return null;
        }
       
        let context: "limited" | "unlimitedMemory" | "unlimitedClusterSize" | "unlimited" = "limited";
        if (status.MaxMemory === 0) {
            context = status.MaxClusterSize === 0 ? "unlimited" : "unlimitedMemory";
        } else if (status.MaxClusterSize === 0) {
            context = "unlimitedClusterSize";
        }
        return t("license.shortDescription", {
            context,
            count: status.MaxCores,
            maxMemory: status.MaxMemory,
            maxClusterSize: status.MaxClusterSize,
        });
    });

    static licenseId = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        if (!status || status.Type === "None") {
            return null;
        }

        return status.Id;
    });

    static licenseType = ko.pureComputed(() => {
        return licenseModel.licenseStatus()?.Type ?? null;
    });

    static getStatusValue<T extends keyof LicenseStatus>(key: T) {
        return licenseModel.licenseStatus()?.[key] ?? null;
    }

    static isEnterpriseOrDeveloper = ko.pureComputed(() => {
        const type = licenseModel.licenseType();

        if (type === "Enterprise" || type === "Developer") {
            return true;
        }

        return false;
    });

    static isProfessionalOrAbove = ko.pureComputed(() => {
        if (licenseModel.isEnterpriseOrDeveloper() || licenseModel.licenseType() === "Professional") {
            return true;
        }

        return false;
    });

    static developerLicense = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        
        if (!licenseStatus || licenseStatus.Type !== "Developer") {
            return false;
        }

        return true;
    });
    
    static cloudLicense = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        
        return licenseStatus && licenseStatus.IsCloud;
    });
    
    static licenseCssClass = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        
        if (!status || status.Type === "None") {
            return 'no-license';
        }
        if (status.Status.includes("Expired")) {
            return 'expired';
        } else if (status.Type === "Invalid") {
            return 'invalid';
        } else {
            return 'valid';
        }
    });
    
    static licenseBgColorClass = ko.pureComputed(() => {
        const licenseType = licenseModel.licenseType();
        
        switch (licenseType) {
            case "Community":
                return "community";
            case "Developer":
                return "developer";
            case "Enterprise":
                return "enterprise";
            case "EnterpriseAi":
                return "enterprise-ai";
            case "Professional":
                return "professional";
            case "Invalid":
                return "danger";
            case "Essential":
            default:
                return "no-background";
        }
    });

    static supportCssClass = ko.pureComputed(() => {
        const support = licenseModel.supportCoverage();
        if (!support) {
            return 'no-support';
        }
        switch (support.Status) {
            case 'ProductionSupport':
                return 'production-support';
            case 'ProfessionalSupport':
                return 'professional-support';
            case 'PartialSupport':
                return 'partial-support';
            default:
                return 'no-support';
        }
    });
    
    static supportLabel = ko.pureComputed(() => {
        switch (licenseModel.supportLabelProvider(licenseModel.licenseStatus(), licenseModel.supportCoverage())) {
            case "Production":
                return t("license.support.production");
            case "Professional":
                return t("license.support.professional");
            case "Partial":
                return t("license.support.partial");
            default:
                return t("license.support.community");
        }
    });
    
    static supportLabelProvider(licenseStatus: LicenseStatus, supportInfo: Raven.Server.Commercial.LicenseSupportInfo) {
        if (!licenseStatus || licenseStatus.Type === "None") {
            return 'Community';
        }

        if (!supportInfo) {
            return 'Community';
        }

        const supportType = supportInfo.Status || "NoSupport";
        switch (supportType) {
            case 'ProductionSupport':
                return 'Production';
            case 'ProfessionalSupport':
                return 'Professional';
            case 'PartialSupport':
                return 'Partial';
            default:
                return 'Community';
        }
    }
    
    static supportTableCssClass = ko.pureComputed(() => {
        const label = licenseModel.supportLabelProvider(licenseModel.licenseStatus(), licenseModel.supportCoverage());
        return label.toLocaleLowerCase();
    });
    
    static licenseStatusTooltip = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        
        if (!status || status.Type === "None") {
            return t("license.statusTooltip.none");
        }
        
        if (status.Type === "Invalid") {
            return t("license.statusTooltip.invalid");
        }
        
        if (status.Status.includes("Expired")) {
            return t("license.statusTooltip.expired");
        }
        
        return t("license.statusTooltip.valid");
    });
}

export = licenseModel;
