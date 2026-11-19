import { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { Icon } from "components/common/Icon";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import AceEditor from "components/common/ace/AceEditor";
import { SubmitHandler, useForm } from "react-hook-form";
import { tryHandleSubmit } from "components/utils/common";
import { AdminJsConsoleFormData, adminJsConsoleYupResolver } from "./AdminJsConsoleValidation";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { FormAceEditor, FormSelect } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import {
    OptionWithIconAndSeparator,
    SelectOptionWithIconAndSeparator,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import "./AdminJsConsole.scss";
import RunScriptButton from "components/common/RunScriptButton";
import useBoolean from "components/hooks/useBoolean";
import { useRavenLink } from "components/hooks/useRavenLink";
import RichAlert from "components/common/RichAlert";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { DatabaseSharedInfo } from "components/models/databases";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

type TargetKind = "server" | "database" | "shard" | "orchestrator";

type TargetOption = SelectOptionWithIconAndSeparator & { kind: TargetKind };

const serverTargetValue = "$server";

const accessibleVariableByKind: Record<TargetKind, string> = {
    server: "server",
    database: "database",
    shard: "database",
    orchestrator: "orchestratorCtx",
};

function getDatabaseTargets(db: DatabaseSharedInfo, t: StudioTranslate<"adminJsConsole">): TargetOption[] {
    if (!db.isSharded) {
        return db.currentNode.isRelevant
            ? [{ value: db.name, label: db.name, icon: "database", kind: "database" }]
            : [];
    }

    const shardTargets = db.shards
        .filter((x) => x.currentNode.isRelevant)
        .map(
            (x): TargetOption => ({
                value: x.name,
                label: DatabaseUtils.formatName(x.name),
                icon: "shard",
                iconColor: "shard",
                kind: "shard",
            })
        );

    if (!db.currentNode.isRelevant) {
        return shardTargets;
    }

    return [
        {
            value: db.name,
            label: t("targets.orchestrator", { name: db.name }),
            icon: "orchestrator",
            iconColor: "orchestrator",
            kind: "orchestrator",
        },
        ...shardTargets,
    ];
}

// TODO https://issues.hibernatingrhinos.com/issue/RavenDB-7588

export default function AdminJSConsole() {
    const t = useStudioTranslation("adminJsConsole");
    const commonT = useStudioTranslation("common");
    const { value: isScriptValid, setValue: setIsScriptValid } = useBoolean(true);
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();
    const asyncRunAdminJsScript = useAsyncCallback(manageServerService.runAdminJsScript);
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);

    const adminJsConsoleDocsLink = useRavenLink({ hash: "IBUJ7M" });

    const allTargets: TargetOption[] = useMemo(() => {
        const databaseTargets = allDatabases.flatMap((db) => getDatabaseTargets(db, t));

        return [
            {
                value: serverTargetValue,
                label: t("targets.server"),
                icon: "server",
                kind: "server",
                horizontalSeparatorLine: databaseTargets.length > 0,
            },
            ...databaseTargets,
        ];
    }, [allDatabases, t]);

    const { handleSubmit, control, reset, formState, watch } = useForm<AdminJsConsoleFormData>({
        resolver: adminJsConsoleYupResolver,
        mode: "all",
        defaultValues: {
            target: allTargets[0].value,
            scriptText: "",
        },
    });

    useDirtyFlag(formState.isDirty);

    const onSave: SubmitHandler<AdminJsConsoleFormData> = async (formData) => {
        reportEvent("console", "execute");

        tryHandleSubmit(async () => {
            const databaseTarget = formData.target === serverTargetValue ? undefined : formData.target;
            await asyncRunAdminJsScript.execute(formData.scriptText, databaseTarget);

            reset(formData);
        });
    };

    const selectedTarget = watch("target");

    const selectedTargetKind = allTargets.find((x) => x.value === selectedTarget)?.kind ?? "database";

    const accessibleVariable = accessibleVariableByKind[selectedTargetKind];

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title={t("title")} icon="administrator-js-console" />
                        <Col>
                            <RichAlert variant="warning" title={t("warning.title")} className="mb-3">
                                <div>{t("warning.message")}</div>
                            </RichAlert>

                            <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                                <Card>
                                    <Card.Header className="hstack gap-4 flex-wrap">
                                        <h3 className="m-0">{t("scriptTarget")}</h3>
                                        <FormSelect
                                            control={control}
                                            name="target"
                                            options={allTargets}
                                            maxMenuHeight={200}
                                            components={{
                                                Option: OptionWithIconAndSeparator,
                                                SingleValue: SingleValueWithIcon,
                                            }}
                                        />
                                        <div className="text-info">
                                            <StudioTrans
                                                ns="adminJsConsole"
                                                i18nKey="accessibleVariable"
                                                components={{ code: <code /> }}
                                                options={{ variable: accessibleVariable }}
                                            />
                                        </div>
                                    </Card.Header>
                                    <Card.Body>
                                        <div className="admin-js-console-grid">
                                            <div>
                                                <h3>{t("script")}</h3>
                                            </div>
                                            <FormAceEditor
                                                control={control}
                                                name="scriptText"
                                                execute={handleSubmit(onSave)}
                                                mode="javascript"
                                                height="200px"
                                                setIsValid={(x) => setIsScriptValid(x)}
                                                focus
                                            />
                                            <RunScriptButton
                                                type="submit"
                                                isSpinning={asyncRunAdminJsScript.status === "loading"}
                                                disabled={!isScriptValid}
                                            />
                                            <div>
                                                <h3>{t("scriptResult")}</h3>
                                            </div>
                                            <AceEditor
                                                mode="javascript"
                                                readOnly
                                                value={JSON.stringify(asyncRunAdminJsScript.result?.Result, null, 4)}
                                                height="300px"
                                            />
                                        </div>
                                    </Card.Body>
                                </Card>
                            </Form>
                        </Col>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper targetId="1" icon="about" color="info">
                                <p>
                                    <StudioTrans
                                        ns="adminJsConsole"
                                        i18nKey="about.intro"
                                        components={{ strong: <strong /> }}
                                    />
                                </p>
                                <p>{t("about.usage")}</p>
                                <hr />
                                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                                <a href={adminJsConsoleDocsLink} target="_blank">
                                    <Icon icon="newtab" /> {t("about.docsLink")}
                                </a>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
