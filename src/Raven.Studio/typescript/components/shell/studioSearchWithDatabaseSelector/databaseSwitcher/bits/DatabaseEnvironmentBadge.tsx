import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface DatabaseEnvironmentBadgeProps {
    environment: Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
}

export default function DatabaseEnvironmentBadge({ environment }: DatabaseEnvironmentBadgeProps) {
    const t = useStudioTranslation("studioSearchWithDatabaseSelector");

    if (!environment || environment === "None") {
        return null;
    }

    const getColor = () => {
        switch (environment) {
            case "Production":
                return "danger";
            case "Testing":
                return "success";
            case "Development":
                return "info";
            default:
                return assertUnreachable(environment);
        }
    };

    const getText = () => {
        switch (environment) {
            case "Production":
                return t("databaseSwitcher.environment.production");
            case "Testing":
                return t("databaseSwitcher.environment.testing");
            case "Development":
                return t("databaseSwitcher.environment.development");
            default:
                return assertUnreachable(environment);
        }
    };

    return (
        <Badge className="ms-2 text-uppercase text-black" bg={getColor()} pill>
            {getText()}
        </Badge>
    );
}
