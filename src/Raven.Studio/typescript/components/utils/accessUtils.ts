import assertUnreachable from "components/utils/assertUnreachable";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("infra");

export function getAccessRequiredMessage(requiredAccessLevel: accessLevel) {
    const accessLevelText = getAccessLevelDisplayName(requiredAccessLevel);
    return t("access.requiredMessage", { accessLevel: accessLevelText });
}

export function getAccessLevelDisplayName(accessLevel: accessLevel) {
    switch (accessLevel) {
        case "DatabaseAdmin":
            return t("access.levels.databaseAdmin");
        case "DatabaseReadWrite":
            return t("access.levels.databaseReadWrite");
        case "DatabaseRead":
            return t("access.levels.databaseRead");
        case "ClusterAdmin":
            return t("access.levels.clusterAdmin");
        case "ClusterNode":
            return t("access.levels.clusterNode");
        case "Operator":
            return t("access.levels.operator");
        case "ValidUser":
            return t("access.levels.validUser");
        case "UnauthenticatedClients":
            return t("access.levels.unauthenticatedClients");
        default:
            assertUnreachable(accessLevel);
    }
}
