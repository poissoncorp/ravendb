import { exhaustiveStringTuple } from "components/utils/common";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import { SelectOption } from "../select/Select";
import { createTranslator } from "common/i18n/i18n";
import type { TranslationKey } from "common/i18n/resources";

const t = createTranslator("studioConfiguration");

export const allStudioEnvironments = exhaustiveStringTuple<StudioEnvironment>()(
    "None",
    "Development",
    "Testing",
    "Production"
);

const ENVIRONMENT_LABEL_KEYS = {
    None: "environments.none",
    Development: "environments.development",
    Testing: "environments.testing",
    Production: "environments.production",
} as const satisfies Record<StudioEnvironment, TranslationKey<"studioConfiguration">>;

export const studioEnvironmentOptions: SelectOption<StudioEnvironment>[] = allStudioEnvironments.map((environment) => ({
    value: environment,
    get label() {
        return t(ENVIRONMENT_LABEL_KEYS[environment]);
    },
}));

export const allFonts = [
    "JetBrains Mono",
    "Google Sans Code",
    "Fira Code",
    "Source Code Pro",
    "Space Mono",
    "IBM Plex Mono",
];

export const predefinedFontOptions: SelectOption<string>[] = [
    {
        value: "default",
        get label() {
            return t("defaultFont");
        },
    },
    ...allFonts.map((font) => ({ value: font, label: font })),
];
