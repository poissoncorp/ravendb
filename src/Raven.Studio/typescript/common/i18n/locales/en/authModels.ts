export default {
    license: {
        type: {
            none: "No license - AGPLv3 Restrictions Applied",
            invalid: "Invalid license",
            cloudFree: "Cloud Free",
            cloudDeveloper: "Cloud Developer",
            cloudProduction: "Cloud Production",
            cloud: "Cloud",
        },
        expirationDate: "{{date}} UTC",
        expirationInfo_expired: "Your license has expired on {{date}} UTC, which was {{localDate}} your local time.",
        expirationInfo_active:
            "Your license will expire at the end of {{date}} UTC, which is {{localDate}} your local time.",
        shortDescription_limited_one: "{{count}} Cores, {{maxMemory}} GB RAM, Max cluster size: {{maxClusterSize}}",
        shortDescription_limited_other: "{{count}} Cores, {{maxMemory}} GB RAM, Max cluster size: {{maxClusterSize}}",
        shortDescription_unlimitedMemory_one: "{{count}} Cores, Unlimited, Max cluster size: {{maxClusterSize}}",
        shortDescription_unlimitedMemory_other: "{{count}} Cores, Unlimited, Max cluster size: {{maxClusterSize}}",
        shortDescription_unlimitedClusterSize_one: "{{count}} Cores, {{maxMemory}} GB RAM, Max cluster size: Unlimited",
        shortDescription_unlimitedClusterSize_other: "{{count}} Cores, {{maxMemory}} GB RAM, Max cluster size: Unlimited",
        shortDescription_unlimited_one: "{{count}} Cores, Unlimited, Max cluster size: Unlimited",
        shortDescription_unlimited_other: "{{count}} Cores, Unlimited, Max cluster size: Unlimited",
        support: {
            community: "Community",
            production: "Production",
            professional: "Professional",
            partial: "Partial",
        },
        statusTooltip: {
            none: "No license - AGPLv3 restrictions applied",
            invalid: "Invalid license",
            expired: "License has expired",
            valid: "Valid license",
        },
    },
} as const;
