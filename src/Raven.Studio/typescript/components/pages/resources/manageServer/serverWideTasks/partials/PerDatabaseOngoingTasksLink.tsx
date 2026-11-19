import React from "react";
import classNames from "classnames";
import appUrl from "common/appUrl";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function PerDatabaseOngoingTasksLink() {
    const t = useStudioTranslation("serverWideTasks");
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <ConditionalPopover
            conditions={{
                isActive: !activeDatabaseName,
                message: t("perDatabaseLink.noDatabase"),
            }}
        >
            <a
                className={classNames("btn btn-link btn-sm", { disabled: !activeDatabaseName })}
                href={activeDatabaseName ? appUrl.forOngoingTasks(activeDatabaseName) : undefined}
                title={t("perDatabaseLink.title")}
            >
                <Icon icon="ongoing-tasks" />
                {t("perDatabaseLink.label")}
            </a>
        </ConditionalPopover>
    );
}
