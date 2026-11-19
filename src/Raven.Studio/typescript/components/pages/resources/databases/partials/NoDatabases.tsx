import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import { EmptySet } from "components/common/EmptySet";
import { FlexGrow } from "components/common/FlexGrow";
import CreateDatabase, {
    CreateDatabaseMode,
} from "components/pages/resources/databases/partials/create/CreateDatabase";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function NoDatabases() {
    const t = useStudioTranslation("databases");
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);

    return (
        <div className="content-margin">
            <div className="text-center">
                <EmptySet>{t("noDatabases.title")}</EmptySet>

                {isOperatorOrAbove && (
                    <div className="d-flex align-items-center gap-1">
                        <FlexGrow />
                        <StudioTrans
                            ns="databases"
                            i18nKey="noDatabases.actions"
                            components={{
                                createButton: (
                                    <Button
                                        variant="outline-primary"
                                        onClick={() => setCreateDatabaseMode("regular")}
                                    />
                                ),
                                separator: <div />,
                                restoreButton: (
                                    <Button
                                        variant="outline-primary"
                                        onClick={() => setCreateDatabaseMode("fromBackup")}
                                    />
                                ),
                            }}
                        />
                        <FlexGrow />

                        {createDatabaseMode && (
                            <CreateDatabase
                                closeModal={() => setCreateDatabaseMode(null)}
                                initialMode={createDatabaseMode}
                            />
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
