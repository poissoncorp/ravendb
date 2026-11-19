import React from "react";
import Spinner from "react-bootstrap/Spinner";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import { FormLabel, FormPathSelector, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsync, UseAsyncReturn } from "react-async-hook";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import RichAlert from "components/common/RichAlert";
import OverrideConfigurationViaExternalScriptToggle from "components/common/formDestinations/OverrideConfigurationViaExternalScriptToggle";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function Local() {
    const t = useStudioTranslation("formDestinations");
    const { control } = useFormContext<FormDestinations>();
    const {
        destinations: { local: formValues },
    } = useWatch({ control });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const getLocalFolderPathsProvider = (folderPath: string) => {
        return async () => {
            const dto = await tasksService.getLocalFolderPathOptions(folderPath, databaseName);
            return dto?.List || [];
        };
    };

    const asyncGetBackupLocation = useAsync(() => {
        if (!formValues.folderPath) {
            return;
        }
        return tasksService.getBackupLocation(formValues.folderPath, databaseName);
    }, [formValues.folderPath]);

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    {t("local.title")}
                </FormSwitch>
                <Collapse in={formValues?.isEnabled} className="mt-2">
                    <div>
                        <OverrideConfigurationViaExternalScriptToggle fieldBase={fieldBase} />
                        {formValues.config.isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={fieldBase} />
                        ) : (
                            <div className="mt-2">
                                <FormLabel>{t("local.folderPathLabel")}</FormLabel>
                                <FormPathSelector
                                    control={control}
                                    name={getName("folderPath")}
                                    selectorTitle={t("local.folderPathSelectorTitle")}
                                    placeholder={t("local.folderPathPlaceholder")}
                                    getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                                    getPathDependencies={(path: string) => [path, databaseName]}
                                />
                                <PathInfo
                                    asyncGetBackupLocation={asyncGetBackupLocation}
                                    hasValue={!!formValues.folderPath}
                                />
                            </div>
                        )}
                    </div>
                </Collapse>
            </Card.Body>
        </Card>
    );
}

function PathInfo({
    asyncGetBackupLocation,
    hasValue,
}: {
    asyncGetBackupLocation: UseAsyncReturn<Raven.Server.Web.Studio.DataDirectoryResult, string[]>;
    hasValue: boolean;
}) {
    const t = useStudioTranslation("formDestinations");
    if (!hasValue || asyncGetBackupLocation.status === "not-requested" || asyncGetBackupLocation.status === "error") {
        return null;
    }

    if (asyncGetBackupLocation.status === "loading") {
        return (
            <RichAlert variant="info" className="mt-2">
                <Spinner />
            </RichAlert>
        );
    }

    if (asyncGetBackupLocation.status === "success" && asyncGetBackupLocation.result?.List?.length > 0) {
        return (
            <RichAlert variant="info" className="mt-2">
                {asyncGetBackupLocation.result.List.map((location) => (
                    <div key={location.FullPath}>
                        <small>
                            <span>
                                <StudioTrans
                                    ns="formDestinations"
                                    i18nKey="local.nodeTag"
                                    components={{ strong: <strong /> }}
                                    options={{ nodeTag: location.NodeTag }}
                                />
                            </span>
                            <br />
                            <span>
                                <StudioTrans
                                    ns="formDestinations"
                                    i18nKey="local.path"
                                    components={{ strong: <strong /> }}
                                    options={{ path: location.FullPath }}
                                />
                            </span>
                            <br />
                            {location.Error ? (
                                <strong>{location.Error}</strong>
                            ) : (
                                <>
                                    {location.FreeSpaceHumane ? (
                                        <span>
                                            <StudioTrans
                                                ns="formDestinations"
                                                i18nKey="local.freeSpace"
                                                components={{ strong: <strong /> }}
                                                options={{
                                                    context: location.TotalSpaceHumane ? "withTotal" : undefined,
                                                    freeSpace: location.FreeSpaceHumane,
                                                    totalSpace: location.TotalSpaceHumane,
                                                }}
                                            />
                                        </span>
                                    ) : (
                                        <strong>{t("local.pathUnreachable")}</strong>
                                    )}
                                </>
                            )}
                        </small>
                    </div>
                ))}
            </RichAlert>
        );
    }
}

const fieldBase = "destinations.local";

type FormFieldNames = keyof FormDestinations["destinations"]["local"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
