import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import IntegrationsUserListItem from "components/pages/database/settings/integrations/IntegrationsUserListItem";
import { IntegrationsUser } from "components/pages/database/settings/integrations/useIntegrations";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface IntegrationsUserListProps {
    fetchState: AsyncStateStatus;
    reload: () => void;
    users: IntegrationsUser[];
    removeUser: (index: number) => void;
}

export default function IntegrationsUserList(props: IntegrationsUserListProps) {
    const t = useStudioTranslation("integrations");
    const { fetchState, reload, users, removeUser } = props;

    if (fetchState === "loading") {
        return <LoadingView />;
    }

    if (fetchState === "error") {
        return <LoadError error={t("list.loadError")} refresh={reload} />;
    }

    if (users.length === 0) {
        return <EmptySet>{t("list.empty")}</EmptySet>;
    }

    return users.map((user, idx) => (
        <IntegrationsUserListItem key={user.id} initialUsername={user.username} removeUser={() => removeUser(idx)} />
    ));
}
