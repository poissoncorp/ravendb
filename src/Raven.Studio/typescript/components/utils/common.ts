import { MouseEvent, MouseEventHandler } from "react";
import { SelectOption } from "components/common/select/Select";
import { loadableData } from "components/models/common";
import { StoryFn } from "@storybook/react-webpack5";
import recentError = require("common/notifications/models/recentError");
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("infra");

export function withPreventDefault(action: (...args: any[]) => void): MouseEventHandler<HTMLElement> {
    return (e: MouseEvent<HTMLElement>) => {
        e.preventDefault();
        action();
    };
}

export function withNestedSubmit(action: (...args: any[]) => void) {
    return (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        e.stopPropagation();
        action();
    };
}

export function createIdleState<T>(initialData?: T): loadableData<T> {
    return {
        status: "idle",
        data: initialData,
    };
}

export function createSuccessState<T>(data: T): loadableData<T> {
    return {
        status: "success",
        data,
    };
}

export function createLoadingState(): loadableData<undefined> {
    return {
        status: "loading",
    };
}

export function createFailureState(error?: any): loadableData<undefined> {
    return {
        status: "failure",
        error,
    };
}

export function getErrorHeadline(error: string): string {
    const colonIndex = error.indexOf(":");
    if (colonIndex === -1) {
        return error;
    }

    // only split on ": " when the prefix looks like an exception type (e.g. "System.TimeoutException"),
    // not e.g. a URL scheme ("http://...") or a message that merely happens to contain a colon
    const prefix = error.slice(0, colonIndex);
    const isUriScheme = error.slice(colonIndex, colonIndex + 3) === "://";
    const looksLikeExceptionType = prefix.length > 0 && !prefix.includes(" ") && !isUriScheme;

    return looksLikeExceptionType ? prefix.trim() : error;
}

export function getRequestErrorMessage(response: JQueryXHR): string {
    if (response.statusText === "timeout") {
        return t("requestTimedOut");
    }

    const { message, error } = recentError.tryExtractMessageAndException(response.responseText);
    if (!message) {
        return response.statusText;
    }

    return error ? `${message}: ${error}` : message;
}

export async function delay(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

export function databaseLocationComparator(lhs: databaseLocationSpecifier, rhs: databaseLocationSpecifier) {
    return lhs.nodeTag === rhs.nodeTag && (lhs.shardNumber ?? null) === (rhs.shardNumber ?? null);
}

export function boundCopy<TArgs>(story: StoryFn<TArgs>, args?: TArgs): StoryFn<TArgs> {
    const copy = story.bind({});
    copy.args = args;
    return copy;
}

export async function tryHandleSubmit<T>(promise: () => Promise<T>) {
    try {
        return await promise();
    } catch (e) {
        console.error(e);
    }
}

export type TryCatchResult<T> =
    | {
          status: "success";
          data: T;
      }
    | {
          status: "error";
          error: string;
      };

export async function tryCatch<T>(promiseFn: () => Promise<T>): Promise<TryCatchResult<T>> {
    try {
        const response = await promiseFn();
        return { status: "success", data: response };
    } catch (error) {
        return {
            status: "error",
            error: error instanceof Error ? error.message : t("unknownError"),
        };
    }
}

// source: https://stackoverflow.com/a/55266531
type AtLeastOne<T> = [T, ...T[]];

export const exhaustiveStringTuple =
    <T extends string>() =>
    <L extends AtLeastOne<T>>(
        ...x: L extends any ? (Exclude<T, L[number]> extends never ? L : Exclude<T, L[number]>[]) : never
    ) =>
        x;
// ---

export const milliSecondsInWeek = 1000 * 3600 * 24 * 7;

function translatedOption<T>(value: T, getLabel: () => string): SelectOption<T> {
    return {
        value,
        get label() {
            return getLabel();
        },
    };
}

export const availableGlacierRegions: SelectOption<string>[] = [
    translatedOption("af-south-1", () => t("awsRegions.afSouth1")),
    translatedOption("ap-east-1", () => t("awsRegions.apEast1")),
    translatedOption("ap-southeast-3", () => t("awsRegions.apSoutheast3")),
    translatedOption("ap-south-1", () => t("awsRegions.apSouth1")),
    translatedOption("ap-northeast-3", () => t("awsRegions.apNortheast3")),
    translatedOption("ap-northeast-2", () => t("awsRegions.apNortheast2")),
    translatedOption("ap-southeast-1", () => t("awsRegions.apSoutheast1")),
    translatedOption("ap-southeast-2", () => t("awsRegions.apSoutheast2")),
    translatedOption("ap-northeast-1", () => t("awsRegions.apNortheast1")),
    translatedOption("us-gov-east-1", () => t("awsRegions.usGovEast1")),
    translatedOption("us-gov-west-1", () => t("awsRegions.usGovWest1")),
    translatedOption("ca-central-1", () => t("awsRegions.caCentral1")),
    translatedOption("cn-north-1", () => t("awsRegions.cnNorth1")),
    translatedOption("cn-northwest-1", () => t("awsRegions.cnNorthwest1")),
    translatedOption("eu-central-1", () => t("awsRegions.euCentral1")),
    translatedOption("eu-west-1", () => t("awsRegions.euWest1")),
    translatedOption("eu-west-2", () => t("awsRegions.euWest2")),
    translatedOption("eu-south-1", () => t("awsRegions.euSouth1")),
    translatedOption("eu-west-3", () => t("awsRegions.euWest3")),
    translatedOption("eu-north-1", () => t("awsRegions.euNorth1")),
    translatedOption("il-central-1", () => t("awsRegions.ilCentral1")),
    translatedOption("me-south-1", () => t("awsRegions.meSouth1")),
    translatedOption("sa-east-1", () => t("awsRegions.saEast1")),
    translatedOption("us-east-1", () => t("awsRegions.usEast1")),
    translatedOption("us-east-2", () => t("awsRegions.usEast2")),
    translatedOption("us-west-1", () => t("awsRegions.usWest1")),
    translatedOption("us-west-2", () => t("awsRegions.usWest2")),
];

export const availableS3Regions: SelectOption<string>[] = [
    translatedOption("af-south-1", () => t("awsRegions.afSouth1")),
    translatedOption("ap-east-1", () => t("awsRegions.apEast1")),
    translatedOption("ap-south-2", () => t("awsRegions.apSouth2")),
    translatedOption("ap-southeast-3", () => t("awsRegions.apSoutheast3")),
    translatedOption("ap-southeast-4", () => t("awsRegions.apSoutheast4")),
    translatedOption("ap-south-1", () => t("awsRegions.apSouth1")),
    translatedOption("ap-northeast-3", () => t("awsRegions.apNortheast3")),
    translatedOption("ap-northeast-2", () => t("awsRegions.apNortheast2")),
    translatedOption("ap-southeast-1", () => t("awsRegions.apSoutheast1")),
    translatedOption("ap-southeast-2", () => t("awsRegions.apSoutheast2")),
    translatedOption("ap-northeast-1", () => t("awsRegions.apNortheast1")),
    translatedOption("us-gov-east-1", () => t("awsRegions.usGovEast1")),
    translatedOption("us-gov-west-1", () => t("awsRegions.usGovWest1")),
    translatedOption("ca-central-1", () => t("awsRegions.caCentral1")),
    translatedOption("cn-north-1", () => t("awsRegions.cnNorth1")),
    translatedOption("cn-northwest-1", () => t("awsRegions.cnNorthwest1")),
    translatedOption("eu-central-1", () => t("awsRegions.euCentral1")),
    translatedOption("eu-west-1", () => t("awsRegions.euWest1")),
    translatedOption("eu-west-2", () => t("awsRegions.euWest2")),
    translatedOption("eu-south-1", () => t("awsRegions.euSouth1")),
    translatedOption("eu-west-3", () => t("awsRegions.euWest3")),
    translatedOption("eu-south-2", () => t("awsRegions.euSouth2")),
    translatedOption("eu-north-1", () => t("awsRegions.euNorth1")),
    translatedOption("eu-central-2", () => t("awsRegions.euCentral2")),
    translatedOption("il-central-1", () => t("awsRegions.ilCentral1")),
    translatedOption("me-south-1", () => t("awsRegions.meSouth1")),
    translatedOption("me-central-1", () => t("awsRegions.meCentral1")),
    translatedOption("sa-east-1", () => t("awsRegions.saEast1")),
    translatedOption("us-east-1", () => t("awsRegions.usEast1")),
    translatedOption("us-east-2", () => t("awsRegions.usEast2")),
    translatedOption("us-west-1", () => t("awsRegions.usWest1")),
    translatedOption("us-west-2", () => t("awsRegions.usWest2")),
];

export type OmitIndexSignature<T> = {
    [K in keyof T as string extends K ? never : K]: T[K];
};

export const storageClassOptions: SelectOption<Raven.Client.Documents.Operations.Backups.S3StorageClass>[] = [
    translatedOption("Standard", () => t("s3StorageClasses.standard")),
    translatedOption("IntelligentTiering", () => t("s3StorageClasses.intelligentTiering")),
    translatedOption("StandardInfrequentAccess", () => t("s3StorageClasses.standardInfrequentAccess")),
    translatedOption("OneZoneInfrequentAccess", () => t("s3StorageClasses.oneZoneInfrequentAccess")),
    translatedOption("GlacierInstantRetrieval", () => t("s3StorageClasses.glacierInstantRetrieval")),
    translatedOption("Glacier", () => t("s3StorageClasses.glacierFlexibleRetrieval")),
    translatedOption("ReducedRedundancy", () => t("s3StorageClasses.reducedRedundancy")),
    translatedOption("DeepArchive", () => t("s3StorageClasses.deepArchive")),
    translatedOption("ExpressOneZone", () => t("s3StorageClasses.expressOneZone")),
];

export type StringWithAutocomplete<T> = T | (string & NonNullable<unknown>);

export const allLogLevels = exhaustiveStringTuple<Sparrow.Logging.LogLevel>()(
    "Trace",
    "Debug",
    "Info",
    "Warn",
    "Error",
    "Fatal",
    "Off"
);

export const logLevelRelevances: Record<Sparrow.Logging.LogLevel, number> = {
    Trace: 0,
    Debug: 1,
    Info: 2,
    Warn: 3,
    Error: 4,
    Fatal: 5,
    Off: 6,
};

export const allLogFilterActions = exhaustiveStringTuple<Sparrow.Logging.LogFilterAction>()(
    "Ignore",
    "IgnoreFinal",
    "Log",
    "LogFinal",
    "Neutral"
);
export const logLevelOptions: SelectOption<Sparrow.Logging.LogLevel>[] = allLogLevels.map((level) => ({
    label: level,
    value: level,
}));

export const logFilterActionOptions: SelectOption<Sparrow.Logging.LogFilterAction>[] = allLogFilterActions.map(
    (action) => ({
        label: action,
        value: action,
    })
);

export type RecursiveRequired<T> = Required<{
    [P in keyof T]: T[P] extends object | undefined ? RecursiveRequired<Required<T[P]>> : T[P];
}>;
