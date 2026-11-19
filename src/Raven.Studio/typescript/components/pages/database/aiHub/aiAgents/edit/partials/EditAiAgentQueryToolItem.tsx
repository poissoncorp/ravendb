import savedQueriesStorage from "common/storage/savedQueriesStorage";
import AceEditor from "components/common/ace/AceEditor";
import Code from "components/common/Code";
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
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { SelectOption } from "components/common/select/Select";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";

interface EditAiAgentQueryToolItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

export default function EditAiAgentQueryToolItem({ index, remove, save, edit }: EditAiAgentQueryToolItemProps) {
    const t = useStudioTranslation("editAiAgent");
    const { control, setValue, trigger } = useFormContext<EditAiAgentFormData>();
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
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center gap-3">
                <div className="tool-info">
                    <h4 className="m-0">{queryItem.name}</h4>
                    <small className="tool-description">{queryItem.description}</small>
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
    const booleanOptions = getBooleanOptions(t);

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">{t("queryTool.configure")}</h4>
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
                <FormLabel>{t("tools.toolNameLabel")}</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`queries.${index}.name`}
                    placeholder={t("queryTool.namePlaceholder")}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>{t("shared.description")}</FormLabel>
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
                    <FormLabel className="mb-0">{t("queryTool.queryLabel")}</FormLabel>
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
                sampleObjectLabel={t("tools.sampleParametersLabel")}
                sampleObject={queryItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<QueryFieldQuerySyntaxHelp />}
                sampleObjectTooltip={<QueryFieldSampleObjectTooltip />}
                sampleObjectPlaceholder={t("queryTool.sampleObjectPlaceholder")}
                jsonSchemaName={`queries.${index}.parametersSchema`}
                jsonSchemaLabel={t("tools.parametersSchemaLabel")}
                jsonSchema={queryItem.parametersSchema}
                jsonSchemaSyntaxHelp={<QueryFieldJsonSchemaSyntaxHelp />}
                schemaType="ToolParameters"
                helpActionTooltipTitle={t("shared.syntaxExample")}
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
                                <PopoverWithHoverWrapper
                                    message={
                                        <StudioTrans
                                            ns="editAiAgent"
                                            i18nKey="queryTool.allowModelQueriesInfo"
                                            components={{ br: <br /> }}
                                        />
                                    }
                                >
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="d-flex flex-wrap align-items-center">
                                <FormSelect
                                    control={control}
                                    name={`queries.${index}.isAllowModelQueries`}
                                    options={booleanOptions}
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
                                <PopoverWithHoverWrapper
                                    message={
                                        <StudioTrans
                                            ns="editAiAgent"
                                            i18nKey="queryTool.addToInitialContextInfo"
                                            components={{ br: <br /> }}
                                        />
                                    }
                                >
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="d-flex flex-wrap align-items-center">
                                <FormSelect
                                    control={control}
                                    name={`queries.${index}.isAddToInitialContext`}
                                    options={booleanOptions}
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

function QueryFieldSampleObjectTooltip() {
    return <StudioTrans ns="editAiAgent" i18nKey="queryTool.sampleObjectInfo" components={{ br: <br /> }} />;
}

const QueryFieldQuerySyntaxHelp = () => {
    const t = useStudioTranslation("editAiAgent");

    const exampleCode1 = `{
    "country": "Provide the country to which the order was shipped.",
    "company": "Provide the company that placed this order."         
}`;

    const exampleCode2 = `{
    "searchTerm": "Provide the name of a product to search for similar items.",
    "similarityLevel": "Provide the similarity level to apply in the search."
}`;

    const exampleCode3 = `{}`;

    return (
        <div>
            <div>{t("shared.exampleNumbered", { number: 1 })}</div>
            <Code code={exampleCode1} language="json" />
            <div className="mt-2">{t("shared.exampleNumbered", { number: 2 })}</div>
            <Code code={exampleCode2} language="json" />
            <div className="mt-2">{t("shared.exampleNoParameters", { number: 3 })}</div>
            <Code code={exampleCode3} language="json" />
        </div>
    );
};

const QueryFieldJsonSchemaSyntaxHelp = () => {
    const t = useStudioTranslation("editAiAgent");

    const exampleCode = `{
    "type": "object",
    "properties": {
        "country": {
            "type": "string",
            "description": "Provide the country to which the order was shipped."
        },
        "company": {
            "type": "string",
            "description": "Provide the company that placed this order."
        }
    },
    "required": [
        "country",
        "company"
    ],
    "additionalProperties": false
}`;

    return (
        <div>
            <div>{t("queryTool.jsonSchemaExample")}</div>
            <Code code={exampleCode} language="json" />
        </div>
    );
};

function getBooleanOptions(t: StudioTranslate<"editAiAgent">): SelectOption<boolean>[] {
    return [
        { label: t("queryTool.valueTrue"), value: true },
        { label: t("queryTool.valueFalse"), value: false },
    ];
}
