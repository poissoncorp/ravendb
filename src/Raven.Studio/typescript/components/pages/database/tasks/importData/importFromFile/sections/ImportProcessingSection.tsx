import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormAceEditor, FormGroup, FormInput, FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import Code from "components/common/Code";
import Card from "react-bootstrap/Card";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function ImportProcessingSection() {
    const t = useStudioTranslation("importFromFile");
    const { control } = useFormContext<ImportFromFileFormData>();

    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });
    const isSetMaxReadOps = useWatch({ control, name: "processing.isSetMaxReadOpsPerSecond" });
    const isEncrypted = useWatch({ control, name: "processing.isEncrypted" });

    return (
        <ImportSection
            id="import-processing"
            title={t("sections.importProcessing")}
            errorPaths={["processing.transformScript", "processing.maxReadOpsPerSecond", "processing.encryptionKey"]}
        >
            <h4 className="mb-2">{t("processing.transformation")}</h4>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="processing.isUseTransformScript">
                    {t("processing.useTransformScript")}{" "}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <div className="mb-1 text-center">{t("processing.transformScriptInfo")}</div>
                                <Code code={codeSample} language="javascript" />
                            </>
                        }
                    >
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon color="info" icon="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <Collapse in={isUseTransformScript}>
                    <div className="mt-3">
                        <FormAceEditor control={control} name="processing.transformScript" mode="javascript" />
                    </div>
                </Collapse>
            </div>

            <h4 className="mb-2">{t("processing.optimization")}</h4>
            <Card className="p-4">
                <FormGroup marginClass="mb-0">
                    <FormSwitch control={control} name="processing.isSetMaxReadOpsPerSecond">
                        {t("processing.setMaxReadOps")}
                    </FormSwitch>
                    <Collapse in={isSetMaxReadOps}>
                        <div className="mt-3">
                            <FormInput
                                control={control}
                                name="processing.maxReadOpsPerSecond"
                                type="number"
                                placeholder={t("processing.maxReadOpsPlaceholder")}
                            />
                        </div>
                    </Collapse>
                </FormGroup>
                <hr className="my-1" />
                <FormGroup marginClass="mb-0">
                    <FormSwitch control={control} name="processing.isEncrypted">
                        {t("processing.isEncrypted")}
                    </FormSwitch>
                    <Collapse in={isEncrypted}>
                        <div className="mt-3">
                            <FormInput
                                control={control}
                                name="processing.encryptionKey"
                                type="password"
                                passwordPreview
                                placeholder={t("processing.keyPlaceholder")}
                                autoComplete="off"
                            />
                            <div className="small text-muted mt-1">{t("processing.keyHint")}</div>
                        </div>
                    </Collapse>
                </FormGroup>
            </Card>
        </ImportSection>
    );
}

const codeSample = `const name = this.FirstName;

if (name === "Bob")
    throw 'skip'; // filter-out

this.Freight = 15.3;
    `;
