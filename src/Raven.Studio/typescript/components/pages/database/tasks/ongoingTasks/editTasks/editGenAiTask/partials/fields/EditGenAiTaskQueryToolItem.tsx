import savedQueriesStorage from "common/storage/savedQueriesStorage";
import AceEditor from "components/common/ace/AceEditor";
import { FormInput, FormAceEditor, FormGroup, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useRqlLanguageService from "components/hooks/useRqlLanguageService";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import queryCriteria from "models/database/query/queryCriteria";
import { useRef } from "react";
import ReactAce from "react-ace";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { queryToolParametersSamplesTabs, queryToolParametersSchemaSamplesTabs } from "../../editGenAiTaskSamplesData";
import { SelectOption } from "components/common/select/Select";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EditGenAiTaskQueryToolItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

export default function EditGenAiTaskQueryToolItem({ index, remove, save, edit }: EditGenAiTaskQueryToolItemProps) {
    const t = useStudioTranslation("editGenAiTask");
    const { control, setValue, trigger } = useFormContext<EditGenAiTaskFormData>();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    const queryAceRef = useRef<ReactAce>(null);

    const formValues = useWatch({
        control,
    });

    const handleSave = async () => {
        const isValid = await trigger([`queries.${index}`]);
        if (!isValid) {
            return;
        }
        save();
    };

    const rqlLanguageService = useRqlLanguageService();

    const queryItem = formValues.queries[index];

    if (!queryItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center">
                <div>
                    <h4 className="m-0">{queryItem.name}</h4>
                    <small>{queryItem.description}</small>
                </div>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="chevron-down" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    const linkToQuery = () => {
        const query = queryCriteria.empty();
        let queryText = "";

        const regexToFind$: RegExp = /\$\w+/g;
        const allMatches = queryItem.query.match(regexToFind$) || [];
        const uniqueMatches = [...new Set(allMatches)];

        if (uniqueMatches.length > 0) {
            queryText += uniqueMatches.map((x) => `${x} = null`).join("\n");
            queryText += "\n\n";
        }

        queryText += queryItem.query;

        query.queryText(queryText);
        query.recentQuery(true);
        query.skipRunOnInit(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(dbName, queryDto, {
            newWindow: true,
        });
    };

    const isAdvancedOpenByDefault = queryItem.isAllowModelQueriesOverride || queryItem.isAddToInitialContextOverride;

    const isAllowModelQueriesOptions: SelectOption<boolean>[] = [
        { label: t("queryTool.trueOption"), value: true },
        { label: t("queryTool.falseOption"), value: false },
    ];

    const isAddToInitialContextOptions: SelectOption<boolean>[] = [
        { label: t("queryTool.trueOption"), value: true },
        { label: t("queryTool.falseOption"), value: false },
    ];

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">{t("queryTool.title")}</h4>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={handleSave}>
                        <Icon icon="chevron-up" margin="m-0" />
                    </Button>
                </div>
            </div>
            <hr className="mt-2 mb-3" />
            <FormGroup>
                <FormLabel>{t("queryTool.toolName")}</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`queries.${index}.name`}
                    placeholder={t("queryTool.toolNamePlaceholder")}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>{t("queryTool.description")}</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`queries.${index}.description`}
                    placeholder={t("queryTool.descriptionPlaceholder")}
                    rows={4}
                />
            </FormGroup>
            <FormGroup>
                <div className="d-flex mb-1 justify-content-between align-items-end">
                    <FormLabel className="mb-0">
                        {t("queryTool.query")}
                        <PopoverWithHoverWrapper
                            message={
                                <StudioTrans
                                    ns="editGenAiTask"
                                    i18nKey="queryTool.queryInfo"
                                    components={{ br: <br /> }}
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    {queryItem.query && (
                        <Button
                            variant="info"
                            className="rounded-pill"
                            onClick={linkToQuery}
                            title={t("queryTool.testQueryTitle")}
                        >
                            <Icon icon="rocket" />
                            {t("queryTool.testQuery")}
                        </Button>
                    )}
                </div>
                <FormAceEditor
                    aceRef={queryAceRef}
                    control={control}
                    name={`queries.${index}.query`}
                    mode="rql"
                    languageService={rqlLanguageService}
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                    placeholder={t("queryTool.queryPlaceholder")}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`queries.${index}.parametersSampleObject`}
                sampleObjectLabel={t("queryTool.sampleObjectLabel")}
                sampleObject={queryItem.parametersSampleObject}
                sampleObjectSamplesPanel={{ tabs: queryToolParametersSamplesTabs }}
                sampleObjectTooltip={<QueryFieldSampleObjectTooltip />}
                sampleObjectPlaceholder={t("queryTool.sampleObjectPlaceholder")}
                jsonSchemaName={`queries.${index}.parametersSchema`}
                jsonSchemaLabel={t("queryTool.parametersSchemaLabel")}
                jsonSchema={queryItem.parametersSchema}
                jsonSchemaSamplesPanel={{ tabs: queryToolParametersSchemaSamplesTabs }}
                schemaType="ToolParameters"
                canRegenerateSchemaName={`queries.${index}.canRegenerateSchema`}
            />

            <Accordion defaultActiveKey={isAdvancedOpenByDefault ? "advanced-settings" : null} className="mt-2">
                <Accordion.Item eventKey="advanced-settings" className="border border-secondary rounded-2 panel-bg-2">
                    <Accordion.Header
                        as={() => (
                            <AccordionButton className="rounded-2 panel-bg-2">
                                {t("queryTool.advancedSettings")}
                            </AccordionButton>
                        )}
                    ></Accordion.Header>
                    <Accordion.Body>
                        <hr className="my-0 mb-2" />
                        <FormGroup>
                            <FormLabel>
                                {t("queryTool.allowModelQueries")}
                                <PopoverWithHoverWrapper message={<AllowModelQueriesTooltip />}>
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="d-flex flex-wrap align-items-center">
                                <FormSelect
                                    control={control}
                                    name={`queries.${index}.isAllowModelQueries`}
                                    options={isAllowModelQueriesOptions}
                                    isDisabled={!queryItem.isAllowModelQueriesOverride}
                                    placeholder={
                                        queryItem.isAllowModelQueriesOverride
                                            ? t("queryTool.selectTrueOrFalse")
                                            : t("queryTool.defaultPlaceholder")
                                    }
                                />
                                <FormSwitch
                                    control={control}
                                    name={`queries.${index}.isAllowModelQueriesOverride`}
                                    className="ms-2"
                                    afterChange={(isChecked) => {
                                        if (!isChecked) {
                                            setValue(`queries.${index}.isAllowModelQueries`, null, {
                                                shouldValidate: true,
                                            });
                                        }
                                    }}
                                >
                                    {t("queryTool.override")}
                                </FormSwitch>
                            </div>
                        </FormGroup>
                        <FormGroup className="mb-0">
                            <FormLabel>
                                {t("queryTool.addToInitialContext")}
                                <PopoverWithHoverWrapper message={<AddToInitialContextTooltip />}>
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="d-flex flex-wrap align-items-center">
                                <FormSelect
                                    control={control}
                                    name={`queries.${index}.isAddToInitialContext`}
                                    options={isAddToInitialContextOptions}
                                    isDisabled={!queryItem.isAddToInitialContextOverride}
                                    placeholder={
                                        queryItem.isAddToInitialContextOverride
                                            ? t("queryTool.selectTrueOrFalse")
                                            : t("queryTool.defaultPlaceholder")
                                    }
                                />
                                <FormSwitch
                                    control={control}
                                    name={`queries.${index}.isAddToInitialContextOverride`}
                                    className="ms-2"
                                    afterChange={(isChecked) => {
                                        if (!isChecked) {
                                            setValue(`queries.${index}.isAddToInitialContext`, null, {
                                                shouldValidate: true,
                                            });
                                        }
                                    }}
                                >
                                    {t("queryTool.override")}
                                </FormSwitch>
                            </div>
                        </FormGroup>
                    </Accordion.Body>
                </Accordion.Item>
            </Accordion>
        </div>
    );
}

function AllowModelQueriesTooltip() {
    return <StudioTrans ns="editGenAiTask" i18nKey="queryTool.allowModelQueriesInfo" components={{ br: <br /> }} />;
}

function AddToInitialContextTooltip() {
    return <StudioTrans ns="editGenAiTask" i18nKey="queryTool.addToInitialContextInfo" components={{ br: <br /> }} />;
}

function QueryFieldSampleObjectTooltip() {
    return <StudioTrans ns="editGenAiTask" i18nKey="queryTool.sampleObjectInfo" components={{ br: <br /> }} />;
}
