import "./SampleObjectAndSchemaFields.scss";
import { Icon } from "components/common/Icon";
import AceEditor, { AceEditorProps } from "../ace/AceEditor";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { FormAceEditor, FormErrorIcon, useErrorMessage } from "../Form";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import { Control, FieldPath, FieldValues, useFormContext, UseFormSetValue, useWatch } from "react-hook-form";
import ReactAce from "react-ace";
import { ReactNode, useEffect, useRef, useState } from "react";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import useUniqueId from "components/hooks/useUniqueId";
import classNames from "classnames";
import messagePublisher from "common/messagePublisher";
import { AceEditorSamplesPanelConfig } from "../ace/AceEditorSamplesPanel";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface SampleObjectAndSchemaFieldsProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    setValue: UseFormSetValue<TFieldValues>;
    sampleObjectName: TName;
    sampleObjectLabel?: ReactNode;
    sampleObject: string;
    sampleObjectSyntaxHelp?: React.ReactNode;
    sampleObjectSamplesPanel?: AceEditorSamplesPanelConfig;
    sampleObjectTooltip?: ReactNode;
    sampleObjectPlaceholder?: string;
    jsonSchemaName: TName;
    jsonSchemaLabel?: ReactNode;
    jsonSchema: string;
    jsonSchemaSyntaxHelp?: React.ReactNode;
    jsonSchemaSamplesPanel?: AceEditorSamplesPanelConfig;
    jsonSchemaTooltip?: ReactNode;
    schemaType?: Raven.Server.Web.Studio.StudioTasksHandler.SchemaType;
    helpActionTooltipTitle?: string;
    canRegenerateSchemaName: TName;
}

export default function SampleObjectAndSchemaFields<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
>({
    control,
    setValue,
    sampleObjectName,
    sampleObjectLabel,
    sampleObject,
    sampleObjectSyntaxHelp,
    sampleObjectSamplesPanel,
    sampleObjectTooltip = <DefaultSampleObjectTooltip />,
    sampleObjectPlaceholder,
    jsonSchemaName,
    jsonSchemaLabel,
    jsonSchema,
    jsonSchemaSyntaxHelp,
    jsonSchemaSamplesPanel,
    jsonSchemaTooltip = <DefaultJsonSchemaTooltip />,
    schemaType,
    helpActionTooltipTitle,
    canRegenerateSchemaName,
}: SampleObjectAndSchemaFieldsProps<TFieldValues, TName>) {
    const t = useStudioTranslation("sampleObjectAndSchemaFields");
    const sampleObjectRef = useRef<ReactAce>(null);
    const jsonSchemaRef = useRef<ReactAce>(null);

    const { tasksService } = useServices();
    const sampleObjectValue = useWatch({ control, name: sampleObjectName });
    const { trigger, formState } = useFormContext();

    const sampleObjectError = useErrorMessage({ control, paths: [sampleObjectName] });
    const jsonSchemaError = useErrorMessage({ control, paths: [jsonSchemaName] });

    const [lastSampleObjectForGenerate, setLastSampleObjectForGenerate] = useState<string>(sampleObjectValue);

    const asyncGenerateSchema = useAsyncCallback(async () => {
        try {
            const result = await tasksService.getJsonSchemaFromSampleObject(JSON.parse(sampleObject), schemaType);
            setValue(jsonSchemaName, result.Result as TFieldValues[TName], { shouldValidate: true });
            setLastSampleObjectForGenerate(sampleObject);
        } catch (e) {
            console.error(e);
            messagePublisher.reportError(t("generateError"));
        }
    });

    const canRegenerateSchema = !!sampleObject && !!jsonSchema && lastSampleObjectForGenerate !== sampleObject;

    useEffect(() => {
        if (_.get(formState.dirtyFields, sampleObjectName)) {
            setValue(canRegenerateSchemaName, canRegenerateSchema as TFieldValues[TName], {
                shouldValidate: true,
            });
            trigger(jsonSchemaName);
        }
    }, [canRegenerateSchema]);

    const tabsId = useUniqueId("sample-object-and-schema-fields-tabs");

    const defaultActiveTab = jsonSchema ? "json-schema" : "sample-object";

    const sampleObjectActions = buildEditorActions({
        onLoadFile: (value) => setValue(sampleObjectName, value as TFieldValues[TName], { shouldValidate: true }),
        syntaxHelp: sampleObjectSyntaxHelp,
        samplesPanel: sampleObjectSamplesPanel,
        helpTooltipTitle: helpActionTooltipTitle,
    });

    const jsonSchemaActions = buildEditorActions({
        onLoadFile: (value) => setValue(jsonSchemaName, value as TFieldValues[TName], { shouldValidate: true }),
        syntaxHelp: jsonSchemaSyntaxHelp,
        samplesPanel: jsonSchemaSamplesPanel,
    });

    return (
        <div className="sample-object-and-schema-tabs">
            <Tabs defaultActiveKey={defaultActiveTab} id={tabsId}>
                <Tab
                    eventKey="sample-object"
                    title={
                        <div className="hstack">
                            <div className={classNames({ "text-danger": sampleObjectError.hasErrors })}>
                                {sampleObjectLabel ?? t("sampleObject.label")}
                            </div>
                            <PopoverWithHoverWrapper message={sampleObjectTooltip}>
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                            <FormErrorIcon control={control} paths={[sampleObjectName]} onError={() => {}} />
                        </div>
                    }
                >
                    <div>
                        <FormAceEditor
                            aceRef={sampleObjectRef}
                            control={control}
                            name={sampleObjectName}
                            mode="json"
                            placeholder={sampleObjectPlaceholder}
                            actions={sampleObjectActions}
                            samplesPanel={sampleObjectSamplesPanel}
                            isFullScreenLabelHidden
                        />
                        {!!sampleObject && !jsonSchema && (
                            <div className="mt-2">
                                <Icon icon="info" color="info" />
                                {t("sampleObject.autoGenerateHint")}
                            </div>
                        )}
                    </div>
                </Tab>
                <Tab
                    eventKey="json-schema"
                    title={
                        <div className="hstack">
                            <span className={classNames(jsonSchemaError.hasErrors && "text-danger")}>
                                {jsonSchemaLabel ?? t("jsonSchema.label")}
                            </span>
                            <PopoverWithHoverWrapper message={jsonSchemaTooltip}>
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                            <FormErrorIcon control={control} paths={[jsonSchemaName]} />
                        </div>
                    }
                >
                    <div>
                        <FormAceEditor
                            aceRef={jsonSchemaRef}
                            control={control}
                            name={jsonSchemaName}
                            mode="json"
                            actions={jsonSchemaActions}
                            samplesPanel={jsonSchemaSamplesPanel}
                            isFullScreenLabelHidden
                            overlaySlot={
                                <>
                                    {!!sampleObject && !jsonSchema && (
                                        <ButtonWithSpinner
                                            className="rounded-pill position-absolute top-50 start-50 translate-middle"
                                            variant="primary"
                                            onClick={asyncGenerateSchema.execute}
                                            isSpinning={asyncGenerateSchema.loading}
                                            title={t("jsonSchema.viewTitle")}
                                        >
                                            {t("jsonSchema.view")}
                                        </ButtonWithSpinner>
                                    )}
                                    {canRegenerateSchema && (
                                        <ButtonWithSpinner
                                            className="rounded-pill position-absolute z-1"
                                            style={{
                                                top: "150px",
                                                right: "54px",
                                            }}
                                            variant="primary"
                                            onClick={asyncGenerateSchema.execute}
                                            isSpinning={asyncGenerateSchema.loading}
                                            icon="refresh"
                                            title={t("jsonSchema.regenerateTitle")}
                                        >
                                            {t("jsonSchema.regenerate")}
                                        </ButtonWithSpinner>
                                    )}
                                </>
                            }
                        />
                        {!!jsonSchema && (
                            <div className="mt-2">
                                <Icon icon="info" color="info" />
                                {t("jsonSchema.sentToModelHint")}
                            </div>
                        )}
                    </div>
                </Tab>
            </Tabs>
        </div>
    );
}

interface BuildEditorActionsArgs {
    onLoadFile: (value: string) => void;
    syntaxHelp?: ReactNode;
    samplesPanel?: AceEditorSamplesPanelConfig;
    helpTooltipTitle?: string;
}

function buildEditorActions({
    onLoadFile,
    syntaxHelp,
    samplesPanel,
    helpTooltipTitle,
}: BuildEditorActionsArgs): AceEditorProps["actions"] {
    return [
        { component: <AceEditor.FullScreenAction /> },
        { component: <AceEditor.FormatAction /> },
        { component: <AceEditor.LoadFileAction onLoad={onLoadFile} /> },
        ...(!samplesPanel && syntaxHelp
            ? [
                  {
                      component: <AceEditor.HelpAction message={syntaxHelp} tooltipTitle={helpTooltipTitle} />,
                      position: "bottom" as const,
                  },
              ]
            : []),
    ];
}

function DefaultSampleObjectTooltip() {
    return (
        <StudioTrans
            ns="sampleObjectAndSchemaFields"
            i18nKey="sampleObject.tooltip"
            components={{ br: <br />, strong: <strong /> }}
        />
    );
}

function DefaultJsonSchemaTooltip() {
    return <StudioTrans ns="sampleObjectAndSchemaFields" i18nKey="jsonSchema.tooltip" components={{ br: <br /> }} />;
}
