import React from "react";
import { Icon } from "components/common/Icon";
import Spinner from "react-bootstrap/Spinner";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import "./CreateSampleData.scss";
import useBoolean from "components/hooks/useBoolean";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import copyToClipboard from "common/copyToClipboard";
import appUrl from "common/appUrl";
import Code from "components/common/Code";
import { LoadError } from "components/common/LoadError";
import SmokeSvg from "./CreateSampleDataSmoke";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

function CreateSampleData() {
    const t = useStudioTranslation("createSampleData");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const { value: isCodeSampleOpen, toggle: toggleCodeSample } = useBoolean(false);

    const docsLink = useRavenLink({ hash: "SUTS29" });

    // Changing the database causes re-mount
    const asyncFetchCollectionsStats = useAsync(() => tasksService.fetchCollectionsStats(databaseName), []);
    const asyncGetSampleDataClasses = useAsync(() => tasksService.getSampleDataClasses(databaseName), []);

    const asyncCreateSampleData = useAsyncCallback(async () => {
        await tasksService.createSampleData(databaseName);

        const db_old = activeDatabaseTracker.default.database();
        if (db_old.hasRevisionsConfiguration()) {
            return;
        }

        const dbInfo = await tasksService.getDatabaseForStudio(databaseName);
        if (!dbInfo.HasRevisionsConfiguration) {
            return;
        }

        db_old.hasRevisionsConfiguration(true);
        collectionsTracker.default.configureRevisions(db_old);
    });

    const canCreateSampleData = asyncFetchCollectionsStats.result
        ? asyncFetchCollectionsStats.result.collections.filter((x) => x.documentCount() > 0).length === 0
        : false;

    return (
        <div className="sample-data absolute-fill">
            <SmokeSvg />
            <div className="scroll">
                <div className="center">
                    <div className="flex-vertical flex-center text-center">
                        <p className="lead text-emphasis margin-bottom">{t("intro")}</p>
                        <p className="small">
                            <StudioTrans
                                ns="createSampleData"
                                i18nKey="docsInfo"
                                components={{ a: <a href={docsLink} target="_blank" /> }}
                            />
                        </p>
                        <div className="margin-top margin-bottom-sm">
                            <ButtonWithSpinner
                                size="lg"
                                className="rounded-pill"
                                variant="primary"
                                onClick={asyncCreateSampleData.execute}
                                isSpinning={asyncCreateSampleData.status === "loading"}
                                icon={asyncCreateSampleData.status === "success" ? "check" : "magic-wand"}
                                disabled={
                                    !canCreateSampleData ||
                                    asyncCreateSampleData.status === "loading" ||
                                    asyncCreateSampleData.status === "success"
                                }
                            >
                                {t("createButton", {
                                    context: asyncCreateSampleData.status === "success" ? "success" : "idle",
                                })}
                            </ButtonWithSpinner>

                            {asyncFetchCollectionsStats.status === "success" && !canCreateSampleData && (
                                <div className="padding padding-xs margin-top-sm text-warning">
                                    <Icon icon="warning" />
                                    {t("requiresEmptyDatabase")}
                                </div>
                            )}

                            {asyncCreateSampleData.status === "success" && (
                                <div className="padding padding-xs margin-top-sm">
                                    <a href={appUrl.forDocuments("", databaseName)}>
                                        <Icon icon="arrow-thin-right" />
                                        {t("goToDocuments")}
                                    </a>
                                </div>
                            )}
                        </div>
                        <Button
                            size="sm"
                            variant="secondary"
                            className="rounded-pill margin-bottom-md"
                            onClick={toggleCodeSample}
                        >
                            <Icon icon="hash" />{" "}
                            {t("codeSample.toggle", { context: isCodeSampleOpen ? "hide" : "show" })}
                        </Button>
                    </div>
                    <Collapse in={isCodeSampleOpen}>
                        <div>
                            <Card className="sample-code">
                                <Card.Body>
                                    <div className="sample-code-header">
                                        <h3>{t("codeSample.title")}</h3>
                                        {asyncGetSampleDataClasses.result && (
                                            <Button
                                                variant="secondary"
                                                className="rounded-pill"
                                                onClick={() =>
                                                    copyToClipboard.copy(
                                                        asyncGetSampleDataClasses.result,
                                                        t("codeSample.copied")
                                                    )
                                                }
                                            >
                                                <Icon icon="copy" /> <span>{t("codeSample.copy")}</span>
                                            </Button>
                                        )}
                                    </div>
                                    {asyncGetSampleDataClasses.loading && (
                                        <div className="d-flex justify-content-center">
                                            <Spinner className="spinner-gradient" />
                                        </div>
                                    )}
                                    {asyncGetSampleDataClasses.error && (
                                        <LoadError
                                            error={t("codeSample.loadError")}
                                            refresh={asyncGetSampleDataClasses.execute}
                                        />
                                    )}
                                    {asyncGetSampleDataClasses.result && (
                                        <Code code={asyncGetSampleDataClasses.result} language="csharp" />
                                    )}
                                </Card.Body>
                            </Card>
                        </div>
                    </Collapse>
                </div>
            </div>
        </div>
    );
}

export default CreateSampleData;
