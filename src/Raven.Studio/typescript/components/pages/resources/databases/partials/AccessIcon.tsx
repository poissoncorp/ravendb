import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function AccessIcon(props: { dbAccess: databaseAccessLevel }) {
    const { dbAccess } = props;
    const t = useStudioTranslation("databases");
    switch (dbAccess) {
        case "DatabaseAdmin":
            return <Icon icon="access-admin" title={t("accessLevel.admin")} />;
        case "DatabaseReadWrite":
            return <Icon icon="access-read-write" title={t("accessLevel.readWrite")} />;
        case "DatabaseRead":
            return <Icon icon="access-read" title={t("accessLevel.read")} />;
        default:
            assertUnreachable(dbAccess);
    }
}
