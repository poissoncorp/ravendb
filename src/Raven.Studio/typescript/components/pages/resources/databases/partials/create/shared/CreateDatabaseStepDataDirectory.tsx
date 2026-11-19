import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Spinner from "react-bootstrap/Spinner";
import InputGroup from "react-bootstrap/InputGroup";
import { FormCheckbox, FormPathSelector } from "components/common/Form";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { UseAsyncReturn } from "react-async-hook";
import { CreateDatabaseFromBackupFormData } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { CreateDatabaseRegularFormData } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularValidation";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface CreateDatabaseStepPathProps {
    manualSelectedNodes: string[];
    isBackupFolder: boolean;
}

export default function CreateDatabaseStepPath({ manualSelectedNodes, isBackupFolder }: CreateDatabaseStepPathProps) {
    const t = useStudioTranslation("databases");
    const { control } = useFormContext<CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData>();
    const {
        basicInfoStep: { databaseName },
        dataDirectoryStep,
    } = useWatch({ control });
    const { resourcesService } = useServices();

    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const selectedNodeTags = manualSelectedNodes ?? allNodeTags;

    const getFolderOptionsProvider = (path: string) => {
        return async () => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(path || "", isBackupFolder);
            return dto?.List || [];
        };
    };

    const asyncGetDatabaseLocation = useAsyncDebounce(
        () =>
            resourcesService.getDatabaseLocation(
                databaseName,
                dataDirectoryStep.isDefault ? "" : dataDirectoryStep.directory
            ),
        [databaseName, dataDirectoryStep.directory, dataDirectoryStep.isDefault]
    );

    return (
        <div>
            <h2 className="text-center">{t("createDatabase.steps.dataDirectory")}</h2>
            <InputGroup className="my-4">
                <InputGroup.Text className="rounded-1 me-1">
                    <FormCheckbox control={control} name="dataDirectoryStep.isDefault">
                        {t("createDatabase.dataDirectory.useServerDirectory")}
                    </FormCheckbox>
                </InputGroup.Text>

                <FormPathSelector
                    control={control}
                    name="dataDirectoryStep.directory"
                    selectorTitle={t("createDatabase.dataDirectory.selectorTitle")}
                    placeholder={t("createDatabase.dataDirectory.placeholder")}
                    getPathsProvider={(path: string) => getFolderOptionsProvider(path)}
                    getPathDependencies={(path: string) => [path, isBackupFolder]}
                    disabled={dataDirectoryStep.isDefault}
                />
            </InputGroup>
            <PathInfo asyncGetDatabaseLocation={asyncGetDatabaseLocation} nodeTagsToDisplay={selectedNodeTags} />
        </div>
    );
}

function PathInfo({
    asyncGetDatabaseLocation,
    nodeTagsToDisplay,
}: {
    asyncGetDatabaseLocation: UseAsyncReturn<Raven.Server.Web.Studio.DataDirectoryResult, (string | boolean)[]>;
    nodeTagsToDisplay?: string[];
}) {
    const t = useStudioTranslation("databases");

    if (
        asyncGetDatabaseLocation.status === "not-requested" ||
        asyncGetDatabaseLocation.status === "error" ||
        asyncGetDatabaseLocation.result?.List?.length === 0
    ) {
        return null;
    }

    if (asyncGetDatabaseLocation.status === "loading") {
        return (
            <div className="mt-2 well p-1 rounded-2 d-flex justify-content-center">
                <Spinner />
            </div>
        );
    }

    const filteredLocations = nodeTagsToDisplay
        ? asyncGetDatabaseLocation.result.List.filter((x) => nodeTagsToDisplay.includes(x.NodeTag))
        : asyncGetDatabaseLocation.result.List;

    return (
        <div className="well mt-2 p-1 rounded-2 vstack gap-1">
            {filteredLocations.map((location) => (
                <div
                    key={`${location.NodeTag}-${location.FullPath}`}
                    color="node"
                    className="hstack gap-4 align-items-start card p-2"
                >
                    <div className="text-center flex-shrink-0">
                        <div className="small-label mb-1">{t("createDatabase.dataDirectory.nodeTag")}</div>
                        <strong className="fs-3 text-node">
                            <Icon icon="node" /> {location.NodeTag}
                        </strong>
                    </div>
                    <div className="vstack flex-grow gap-2">
                        <div>
                            <div className="small-label">
                                <Icon icon="path" />
                                {t("createDatabase.dataDirectory.path")}
                            </div>
                            <code className="text-info word-break">{location.FullPath}</code>
                        </div>

                        {location.Error ? (
                            <RichAlert variant="danger">{location.Error}</RichAlert>
                        ) : (
                            <>
                                {location.FreeSpaceHumane ? (
                                    <div className="hstack gap-3">
                                        <div>
                                            <div className="small-label">
                                                <Icon icon="storage" /> {t("createDatabase.dataDirectory.freeSpace")}
                                            </div>
                                            <strong>{location.FreeSpaceHumane}</strong>
                                        </div>

                                        {location.TotalSpaceHumane && (
                                            <div>
                                                <div className="small-label">
                                                    <Icon icon="storage" />{" "}
                                                    {t("createDatabase.dataDirectory.totalSpace")}
                                                </div>
                                                <strong>{location.TotalSpaceHumane}</strong>
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <RichAlert variant="warning">
                                        {t("createDatabase.dataDirectory.pathUnreachable")}
                                    </RichAlert>
                                )}
                            </>
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
}
