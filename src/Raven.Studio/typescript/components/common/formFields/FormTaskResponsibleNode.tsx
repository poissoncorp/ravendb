import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { FieldValues, FieldPath, useWatch, UseControllerProps } from "react-hook-form";

interface FormTaskResponsibleNodeProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: UseControllerProps<TFieldValues>["control"];
    isSetName: TName;
    nodeName: TName;
    isPinName: TName;
}

export function FormTaskResponsibleNode<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    isSetName,
    nodeName,
    isPinName,
}: FormTaskResponsibleNodeProps<TFieldValues, TName>) {
    const t = useStudioTranslation("formFields");
    const isSet = useWatch({ control, name: isSetName });
    const responsibleNode = useWatch({ control, name: nodeName });
    const isPin = useWatch({ control, name: isPinName });

    const isDatabaseSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded ?? false;
    const allNodes = useAppSelector(clusterSelectors.allNodes);

    const possibleMentorOptions: SelectOption[] = allNodes
        .filter((x) => x.type === "Member")
        .map((x) => ({ value: x.nodeTag, label: t("taskResponsibleNode.nodeOption", { nodeTag: x.nodeTag }) }));

    const hasNoNodes = possibleMentorOptions.length === 0;

    return (
        <FormGroup>
            <FormGroup>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: isDatabaseSharded,
                            message: t("taskResponsibleNode.shardedNotRespected"),
                        },
                        {
                            isActive: hasNoNodes,
                            message: t("taskResponsibleNode.noNodesAvailable"),
                        },
                    ]}
                >
                    <FormSwitch control={control} name={isSetName} disabled={isDatabaseSharded || hasNoNodes}>
                        {t("taskResponsibleNode.setResponsibleNode")}
                    </FormSwitch>
                </ConditionalPopover>
            </FormGroup>
            {isSet && (
                <>
                    <FormGroup>
                        <FormSelect control={control} name={nodeName} options={possibleMentorOptions} />
                    </FormGroup>
                    {responsibleNode && (
                        <FormGroup>
                            <FormSwitch
                                control={control}
                                name={isPinName}
                                title={t("taskResponsibleNode.pinNodeTitle")}
                            >
                                {t("taskResponsibleNode.pinNode")}
                            </FormSwitch>
                            <RichAlert variant="info" className="mt-2">
                                <StudioTrans
                                    ns="formFields"
                                    i18nKey="taskResponsibleNode.info"
                                    components={{ br: <br /> }}
                                    options={{ context: isPin ? "pinned" : "preferred" }}
                                />
                            </RichAlert>
                        </FormGroup>
                    )}
                </>
            )}
        </FormGroup>
    );
}
