import { FormAceEditor, FormGroup, FormLabel, FormSwitch } from "components/common/Form";
import {
    EmbeddedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext } from "react-hook-form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Code from "components/common/Code";
import AceEditor from "components/common/ace/AceEditor";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditCdcSinkTaskOnDeleteFields({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <div>
            <FormGroup>
                <FormSwitch control={control} name={`${path}.onDelete.ignoreDeletes`}>
                    {t("onDelete.skipDeletion")}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="onDelete.skipDeletionDefault"
                                        components={{ strong: <strong />, br: <br />, code: <code /> }}
                                    />
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="onDelete.skipDeletionEnabled"
                                        components={{ strong: <strong />, br: <br />, code: <code /> }}
                                    />
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="onDelete.skipDeletionPatch"
                                        components={{ em: <em />, br: <br />, code: <code /> }}
                                    />
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info-new" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormSwitch>
            </FormGroup>
            <FormGroup marginClass="mb-0">
                <FormLabel>
                    {t("onDelete.deletePatch")}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <p>
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="onDelete.deletePatchInfo"
                                        components={{ strong: <strong /> }}
                                    />
                                </p>
                                <ul>
                                    <li>
                                        <StudioTrans
                                            ns="editCdcSinkTask"
                                            i18nKey="onDelete.deletePatchRootTables"
                                            components={{ strong: <strong />, code: <code />, br: <br /> }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="editCdcSinkTask"
                                            i18nKey="onDelete.deletePatchEmbeddedTables"
                                            components={{ strong: <strong />, code: <code /> }}
                                        />
                                    </li>
                                </ul>
                                <p>
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="onDelete.deletePatchSkipDeletion"
                                        components={{ em: <em /> }}
                                    />
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="onDelete.deletePatchUsage"
                                        components={{ code: <code /> }}
                                    />
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info-new" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    control={control}
                    name={`${path}.onDelete.patch`}
                    mode="javascript"
                    height="150px"
                    placeholder={t("onDelete.placeholder")}
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: <AceEditor.HelpAction message={deletePatchSyntaxHelp} />,
                            position: "bottom",
                        },
                    ]}
                    isFullScreenLabelHidden
                />
            </FormGroup>
        </div>
    );
}

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
                        i18nKey="onDelete.variableThis"
                        components={{ code: <code />, br: <br />, strong: <strong />, em: <em /> }}
                    />
                </li>
                <li>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="onDelete.variableRow"
                        components={{ code: <code />, br: <br /> }}
                    />
                </li>
                <li>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="onDelete.variableOld"
                        components={{ code: <code />, br: <br />, strong: <strong /> }}
                    />
                </li>
            </ul>
        </div>
    );
};

const ArchiveSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `// Combine with "Skip deletion" to keep the document instead of removing it.
this.Archived = true;
this.ArchivedAt = new Date().toISOString();`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("onDelete.archiveExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const AuditTrailSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `// Use put() to persist an audit record before the document is deleted.
put('DeletedOrders/' + id(this), {
    OriginalId: id(this),
    Customer: this.Customer,
    Total: this.Total,
    DeletedAt: new Date().toISOString()
});`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("onDelete.auditTrailExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const RecomputeAggregateSyntaxHelp = () => {
    const t = useStudioTranslation("editCdcSinkTask");
    const code = `// For embedded deletes (without "Skip deletion"): 
// the item is already removed from this.Lines,
// so re-summing gives the correct post-deletion total.
this.TotalQuantity = (this.Lines || [])
    .reduce((sum, line) => sum + (line.Quantity || 0), 0);`;

    return (
        <div>
            <div className="mb-1">
                <strong>{t("onDelete.recomputeAggregateExample")}</strong>
            </div>
            <Code code={code} language="javascript" />
        </div>
    );
};

const deletePatchSyntaxHelp = (
    <div className="vstack gap-2">
        <div className="mb-2">
            <VariablesLegend />
        </div>
        <ArchiveSyntaxHelp />
        <AuditTrailSyntaxHelp />
        <RecomputeAggregateSyntaxHelp />
    </div>
);
