import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import {
    EmbeddedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import AceEditor from "components/common/ace/AceEditor";
import Code from "components/common/Code";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditCdcSinkTaskPatchAdvancedField({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <FormGroup marginClass="mb-0">
            <FormLabel>
                {t("patch.label")}
                <PopoverWithHoverWrapper
                    message={
                        <>
                            <p>
                                <StudioTrans
                                    ns="editCdcSinkTask"
                                    i18nKey="patch.info"
                                    components={{ strong: <strong /> }}
                                />
                            </p>
                            <p>{t("patch.usage")}</p>
                        </>
                    }
                >
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormAceEditor
                control={control}
                name={`${path}.patch`}
                mode="javascript"
                height="150px"
                placeholder={t("patch.placeholder")}
                actions={[
                    { component: <AceEditor.FullScreenAction /> },
                    { component: <AceEditor.FormatAction /> },
                    {
                        component: <AceEditor.HelpAction message={patchSyntaxHelp} />,
                        position: "bottom",
                    },
                ]}
                isFullScreenLabelHidden
            />
        </FormGroup>
    );
}

const ComputeFieldSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `// $row contains the raw CDC row with all columns as-is from the source database
this.FullName = ($row.first_name + ' ' + $row.last_name).trim();`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("patch.computeFieldExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const UnmappedColumnSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `// (base_price and tax_rate are not in the column mapping)
this.FinalPrice = $row.base_price * (1 + $row.tax_rate);`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("patch.unmappedColumnExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const DeltaComputationSyntaxHelp = () => {
    const code = `// Use $old to update a running total or compute a value based on the previous state of the document. 
const oldAmount = $old?.Amount || 0;
const newAmount = $row.amount || 0;
this.RunningTotal = (this.RunningTotal || 0) + (newAmount - oldAmount);`;

    return (
        <div>
            <div className="mb-1">
                <strong>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="patch.deltaComputationExample"
                        components={{ code: <code /> }}
                    />
                </strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const ConditionalEnrichmentSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `if ($row.status === 'VIP') {
    this.Priority = 'High';
    this.Discount = 0.15;
}`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("patch.conditionalEnrichmentExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const LoadRelatedDocumentSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `const customer = load('Customers/' + $row.customer_id);
if (customer) {
    this.CustomerName = customer.Name;
    this.CustomerEmail = customer.Email;
}`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("patch.loadRelatedDocumentExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const VariablesLegend = () => {
    const t = useStudioTranslation("editCdcSinkTask");

    return (
        <div className="border border-secondary rounded-2 panel-bg-2 p-3">
            <div className="mb-2">
                <strong>{t("syntaxHelp.availableVariables")}</strong>
            </div>
            <ul className="mb-0">
                <li>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="patch.variableThis"
                        components={{ code: <code />, br: <br /> }}
                    />
                </li>
                <li>
                    <StudioTrans ns="editCdcSinkTask" i18nKey="patch.variableRow" components={{ code: <code /> }} />
                </li>
                <li>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="patch.variableOld"
                        components={{ code: <code />, br: <br />, strong: <strong /> }}
                    />
                </li>
            </ul>
        </div>
    );
};

const patchSyntaxHelp = (
    <div className="vstack gap-2">
        <div className="mb-2">
            <VariablesLegend />
        </div>
        <ComputeFieldSyntaxHelp />
        <UnmappedColumnSyntaxHelp />
        <DeltaComputationSyntaxHelp />
        <ConditionalEnrichmentSyntaxHelp />
        <LoadRelatedDocumentSyntaxHelp />
    </div>
);
