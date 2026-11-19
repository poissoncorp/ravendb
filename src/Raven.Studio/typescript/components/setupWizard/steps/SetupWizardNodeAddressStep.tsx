import { Control, useFieldArray, useForm, useFormContext, UseFormReturn, useWatch } from "react-hook-form";
import { SetupWizardFormData, SetupWizardSecurityOption } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import Collapse from "react-bootstrap/Collapse";
import React, { useEffect, useMemo } from "react";
import Form from "react-bootstrap/Form";
import {
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
    FormSwitch,
    OptionalLabel,
} from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import useConfirm from "components/common/ConfirmDialog";
import InputGroup from "react-bootstrap/InputGroup";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import classNames from "classnames";
import genUtils from "common/generalUtils";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { SelectOption } from "components/common/select/Select";
import { isEmpty } from "common/typeUtils";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { getMaxClusterSize } from "components/setupWizard/utils/setupWizardUtils";
import useBoolean from "hooks/useBoolean";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { components, OptionProps, SingleValueProps } from "react-select";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { createTranslator } from "common/i18n/i18n";

const translate = createTranslator("setupWizard");

export function SetupWizardNodeAddressStep() {
    const t = useStudioTranslation("setupWizard");
    const { control } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();
    const { setupWizardService } = useServices();
    const { fields, append, remove } = useFieldArray({
        control,
        name: "nodeAddressStep.nodes",
    });

    const asyncGetSetupParameters = useAsync(async () => setupWizardService.getSetupParameters(), []);

    const {
        domainStep,
        selfSignedCertificateStep: { cns, isWildcardCertificate },
        securityStep: { securityOption },
    } = useWatch({ control });

    const fullDomain = `a.${domainStep.domain.toLocaleLowerCase()}.${domainStep.rootDomain}`;

    const getDomainForWildcard = (tag: string | null): string => {
        if (cns.length === 0) {
            return null;
        }

        const cn = cns[0];

        if (!tag) {
            return cn.replace("*.", "");
        }
        return cn.replace("*", tag);
    };

    const getDefaultDnsName = (tag: string): string => {
        if (securityOption !== "ownCertificate" || isWildcardCertificate) {
            return undefined;
        }

        const tagPrefix = `${tag.toLowerCase()}.`;
        return cns.find((cn) => cn.toLowerCase().startsWith(tagPrefix)) ?? cns[0];
    };

    const handleDefaultNodeUrlConfiguration = () => {
        if (securityOption === "letsEncrypt") {
            return fullDomain;
        }

        if (securityOption === "ownCertificate") {
            if (isWildcardCertificate) {
                return getDomainForWildcard(null);
            } else {
                return cns[0];
            }
        }

        return null;
    };

    const addNewNode = () => {
        const existingTags = fields.map((field) => field.nodeTag);
        const nodeTags = generateAlphabeticTags(2); // validation allows us to contain 4 letters, but generated tags are around +- 500k length, so we limit it to 2. (700 tags)
        const availableNodeTag = nodeTags.find((tag) => !existingTags.includes(tag)) || "A";

        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "add-node", availableNodeTag);

        append({
            nodeTag: availableNodeTag,
            ipAddress: [
                {
                    ipAddress: asyncGetSetupParameters.result?.IsDocker ? "" : "127.0.0.1",
                },
            ],
            dnsName: getDefaultDnsName(availableNodeTag),
            isEditing: true,
            isNewlyAdded: true,
        });
    };

    // add default node
    useEffect(() => {
        if (fields.length === 0) {
            append({
                nodeTag: "A",
                ipAddress: [
                    {
                        ipAddress: asyncGetSetupParameters.result?.IsDocker ? "" : "127.0.0.1",
                    },
                ],
                dnsName: getDefaultDnsName("A"),
                isEditing: true, // the first node should be added with default values and in editing mode
                isNewlyAdded: false,
                isPassive: false,
                nodeUrl: handleDefaultNodeUrlConfiguration(),
                httpPort: securityOption === "none" ? 8080 : 443,
                tcpPort: 38888,
                hasExternalConfig: false,
            });
        }
    }, []); // eslint-disable-line react-hooks/exhaustive-deps

    useRevalidatePersistedNodesOnEntry();

    return (
        <div>
            <div className="mb-4">
                <h2 className="mb-1">{t("nodeAddressStep.title")}</h2>
                <p className="mb-4 text-muted">
                    {t("nodeAddressStep.description")}
                    <br />
                    {t("nodeAddressStep.descriptionDetails")}
                </p>
            </div>
            <div className="vstack">
                {fields.map((field, index) => (
                    <NodeDetailsPanel key={field.id} control={control} index={index} onRemove={() => remove(index)} />
                ))}
                <AddAnotherNode onAddNode={addNewNode} />
            </div>
        </div>
    );
}

function generateAlphabeticTags(maxLength: number = 4): string[] {
    const tags: string[] = [];
    let currentTag = "";

    while (currentTag.length <= maxLength) {
        tags.push(getNextTag(currentTag));
        currentTag = getNextTag(currentTag);
    }

    return tags;
}

function getNextTag(current: string): string {
    if (!current) {
        return "A";
    }

    const chars = current.split("");
    let i = chars.length - 1;

    while (i >= 0) {
        if (chars[i] !== "Z") {
            chars[i] = String.fromCharCode(chars[i].charCodeAt(0) + 1);
            break;
        }
        chars[i] = "A";
        i--;
    }

    if (i < 0) {
        chars.unshift("A");
    }

    return chars.join("");
}

interface NodeDetailsPanelProps {
    control: Control<SetupWizardFormData>;
    index: number;
    onRemove: () => void;
}

function NodeDetailsPanel({ control, index, onRemove }: NodeDetailsPanelProps) {
    const { getValues } = useFormContext<SetupWizardFormData>();
    const nodeData = useWatch({
        control,
        name: `nodeAddressStep.nodes.${index}`,
    });

    const {
        securityStep: { securityOption },
        selfSignedCertificateStep: { cns, isWildcardCertificate },
    } = useWatch({
        control,
    });
    const { setupWizardService } = useServices();

    const asyncGetSetupParameters = useAsync(async () => setupWizardService.getSetupParameters(), []);

    const nodeAddressStep = getValues().nodeAddressStep;

    const editNodeForm = useForm<NodeEditFormData>({
        defaultValues: nodeData,
        mode: "onChange",
        resolver: yupResolver(nodeEditFormSchema),
        context: {
            nodeAddressStep,
            securityOption,
            currentIndex: index,
            isDocker: asyncGetSetupParameters.result?.IsDocker,
            cns,
            isWildcardCertificate,
        },
    });

    return (
        <RichPanel hover>
            <NodeDetailsPanelHeader control={control} editNodeForm={editNodeForm} index={index} onRemove={onRemove} />
            {!nodeData.isEditing ? (
                <NodeDetailsPanelView index={index} control={control} />
            ) : (
                <NodeDetailsPanelEdit parentControl={control} editNodeForm={editNodeForm} />
            )}
        </RichPanel>
    );
}

interface NodeDetailsPanelHeaderProps {
    control: Control<SetupWizardFormData>;
    index: number;
    onRemove: () => void;
    editNodeForm: UseFormReturn<NodeEditFormData>;
}

function NodeDetailsPanelHeader({ control, index, onRemove, editNodeForm }: NodeDetailsPanelHeaderProps) {
    const t = useStudioTranslation("setupWizard");
    const tCommon = useStudioTranslation("common");
    const { setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();
    const nodeData = useWatch({
        control,
        name: `nodeAddressStep.nodes.${index}`,
    });

    const {
        domainStep,
        selfSignedCertificateStep: { isWildcardCertificate, cns },
        setupMethodStep: { method },
        nodeAddressStep: { nodes },
        securityStep: { securityOption },
    } = useWatch({
        control,
    });

    const { handleSubmit, reset, formState } = editNodeForm;

    const nodeName = t("nodeAddressStep.panel.nodeName", { tag: nodeData.nodeTag ?? "?" });

    const handleDiscardEdit = () => {
        reportEvent(
            setupWizardGA4Prefixes.nodeAddressStep,
            "discard-edit",
            nodeData.isNewlyAdded ? "new" : (nodeData.nodeTag ?? "?")
        );
        if (nodeData.isNewlyAdded) {
            onRemove();
        } else {
            reset(nodeData);
            setValue(`nodeAddressStep.nodes.${index}`, {
                ...nodeData,
                isEditing: false,
            });
        }
    };

    const handleNodeUrl = (formData: NodeEditFormData) => {
        if (securityOption === "letsEncrypt") {
            return `${formData.nodeTag.toLowerCase()}.${domainStep.domain.toLocaleLowerCase()}.${domainStep.rootDomain}`;
        }

        if (securityOption === "ownCertificate") {
            let nodeUrl: string;

            if (isWildcardCertificate) {
                // For wildcard certificates, construct domain from CN and node tag
                if (cns.length > 0 && cns[0].includes("*")) {
                    nodeUrl = cns[0].replace("*", formData.nodeTag.toLowerCase());
                } else {
                    // Fallback to dnsName if available
                    nodeUrl = formData.dnsName || "";
                }
            } else {
                // For non-wildcard certificates, use the selected dnsName
                nodeUrl = formData.dnsName || "";
            }

            if (formData.httpPort !== 443 && formData.httpPort != null) {
                nodeUrl += ":" + formData.httpPort;
            }
            return nodeUrl;
        }

        return null;
    };

    const handleSaveEdit = handleSubmit(async (formData: NodeEditFormData) => {
        setValue(`nodeAddressStep.nodes.${index}`, {
            ...formData,
            dnsName: securityOption === "ownCertificate" ? formData.dnsName : null,
            nodeUrl: handleNodeUrl(formData),
            httpPort: formData.httpPort == null ? (securityOption === "none" ? 8080 : 443) : formData.httpPort,
            isEditing: false,
            isNewlyAdded: false,
        });
        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "save-node", formData.nodeTag ?? "");
    });

    const confirm = useConfirm();

    const handleDeleteNode = async () => {
        const isConfirmed = await confirm({
            title: (
                <StudioTrans
                    ns="setupWizard"
                    i18nKey="nodeAddressStep.deleteConfirm.title"
                    options={{ tag: nodeData.nodeTag ?? "" }}
                    components={{ b: <b /> }}
                />
            ),
            message: (
                <div className="d-flex w-100 justify-content-center">{t("nodeAddressStep.deleteConfirm.message")}</div>
            ),
            icon: "trash",
            confirmText: tCommon("delete"),
            actionColor: "danger",
            size: "lg",
        });

        if (isConfirmed) {
            reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "remove-node", nodeData.nodeTag ?? "?");
            onRemove();
        }
    };

    const isPassiveVisible = method !== "createPackage" && nodes.length === 1;

    return (
        <RichPanelHeader>
            <RichPanelInfo>
                <RichPanelName size="sm">
                    {nodeData.isNewlyAdded ? (
                        <>{t("nodeAddressStep.panel.creatingNode")}</>
                    ) : nodeData.isEditing ? (
                        <>{t("nodeAddressStep.panel.editingNode")}</>
                    ) : (
                        <>
                            <Icon color="node" icon="node" />
                            {nodeName}{" "}
                            {index === 0 && (
                                <small className="text-muted">
                                    {nodeData.isPassive && isPassiveVisible ? (
                                        <span className="text-info">{t("nodeAddressStep.panel.passive")}</span>
                                    ) : (
                                        t("nodeAddressStep.panel.currentNode")
                                    )}
                                </small>
                            )}
                        </>
                    )}
                </RichPanelName>
            </RichPanelInfo>
            <RichPanelActions>
                {nodeData.isEditing ? (
                    <>
                        <ConditionalPopover
                            conditions={{
                                isActive: !isEmpty(formState.errors),
                                message: t("nodeAddressStep.panel.fixErrors"),
                            }}
                        >
                            <Button disabled={!isEmpty(formState.errors)} onClick={handleSaveEdit} variant="success">
                                <Icon icon="save" />
                                {tCommon("save")}
                            </Button>
                        </ConditionalPopover>
                        <Button variant="secondary" onClick={handleDiscardEdit}>
                            {nodeData.isNewlyAdded ? (
                                <>
                                    <Icon icon="trash" />
                                    {t("nodeAddressStep.panel.remove")}
                                </>
                            ) : (
                                <>
                                    <Icon icon="close" />
                                    {t("nodeAddressStep.panel.discard")}
                                </>
                            )}
                        </Button>
                    </>
                ) : (
                    <>
                        <Button
                            variant="secondary"
                            onClick={() => {
                                reportEvent(
                                    setupWizardGA4Prefixes.nodeAddressStep,
                                    "edit-node",
                                    nodeData.nodeTag ?? "?"
                                );
                                setValue(`nodeAddressStep.nodes.${index}.isEditing`, true);
                            }}
                        >
                            <Icon icon="edit" margin="m-0" />
                        </Button>
                        {nodes.filter((node) => !node.isNewlyAdded).length > 1 && (
                            <Button variant="danger" onClick={handleDeleteNode}>
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </>
                )}
            </RichPanelActions>
        </RichPanelHeader>
    );
}

interface NodeDetailsPanelViewProps {
    index: number;
    control: Control<SetupWizardFormData>;
}

function NodeDetailsPanelView({ index, control }: NodeDetailsPanelViewProps) {
    const t = useStudioTranslation("setupWizard");
    const setupWizardData = useWatch({
        control,
    });

    const nodeData = setupWizardData.nodeAddressStep.nodes[index];
    const httpProtocol = setupWizardData.securityStep.securityOption === "none" ? "http" : "https";

    const localIpPortAddress = `${nodeData.ipAddress[0].ipAddress}:${nodeData.httpPort}`;
    return (
        <RichPanelDetails>
            <RichPanelDetailItem>
                <div className="d-flex flex-column w-100">
                    <span className="hstack">
                        <span className="md-label mb-0">{t("nodeAddressStep.view.nodeUrl")}</span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={t("nodeAddressStep.view.addressInfo")}
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    <div className="text-truncate" title={nodeData.nodeUrl || localIpPortAddress}>
                        {nodeData.nodeUrl || localIpPortAddress}
                    </div>
                </div>
            </RichPanelDetailItem>

            {nodeData.dnsName && (
                <RichPanelDetailItem>
                    <div className="d-flex flex-column w-100">
                        <span className="hstack">
                            <span className="md-label mb-0">{t("nodeAddressStep.fields.dnsName")}</span>
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description={t("nodeAddressStep.view.addressInfo")}
                                        ravenLinkHash="MGIZZM"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </span>
                        <div className="text-truncate" title={nodeData.dnsName}>
                            {nodeData.dnsName}
                        </div>
                    </div>
                </RichPanelDetailItem>
            )}

            <RichPanelDetailItem>
                <div className="d-flex flex-column">
                    <span className="hstack">
                        <span className="md-label mb-0">
                            {t("nodeAddressStep.fields.httpPort", { context: httpProtocol })}
                        </span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={t("nodeAddressStep.view.httpPortInfo", { context: httpProtocol })}
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    <div>
                        {nodeData.hasExternalConfig && nodeData.externalHttpPort
                            ? nodeData.externalHttpPort
                            : nodeData.httpPort}
                    </div>
                </div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <div className="d-flex flex-column">
                    <span className="hstack">
                        <span className="md-label mb-0">{t("nodeAddressStep.view.tcpPort")}</span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={t("nodeAddressStep.view.tcpPortInfo")}
                                    ravenLinkHash="9D6HG1"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    <div>
                        {nodeData.hasExternalConfig && nodeData.externalTcpPort
                            ? nodeData.externalTcpPort
                            : nodeData.tcpPort}
                    </div>
                </div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <div className="d-flex flex-column">
                    <span className="hstack">
                        <span className="md-label mb-0">{t("nodeAddressStep.fields.ipAddress")}</span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={t("nodeAddressStep.view.ipAddressInfo")}
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    {nodeData.hasExternalConfig && nodeData.externalIpAddress ? (
                        <div>{nodeData.externalIpAddress}</div>
                    ) : (
                        <div>{nodeData.ipAddress.map((x) => x.ipAddress).join(", ")}</div>
                    )}
                </div>
            </RichPanelDetailItem>
        </RichPanelDetails>
    );
}

function NodeDetailsPanelEdit({
    editNodeForm,
    parentControl,
}: {
    editNodeForm: UseFormReturn<NodeEditFormData>;
    parentControl: Control<SetupWizardFormData>;
}) {
    const t = useStudioTranslation("setupWizard");
    const { control } = editNodeForm;
    const nodeData = useWatch({
        control,
    });

    const {
        domainStep,
        securityStep: { securityOption },
        setupMethodStep: { method: setupMethod },
        selfSignedCertificateStep: { cns, isWildcardCertificate },
        nodeAddressStep: { nodes },
    } = useWatch({
        control: parentControl,
    });

    const { isExternalRequired } = useHostnameDetectionSideEffects({ editNodeForm, parentControl });

    const isDNSVisible = securityOption === "ownCertificate" && !isWildcardCertificate;
    const isPassiveVisible = setupMethod !== "createPackage" && nodes.length === 1;

    const canCustomizeExternalIpsAndPorts = securityOption === "letsEncrypt";
    const canCustomizeExternalTcpPorts = securityOption === "ownCertificate";

    const colWidth = isDNSVisible ? 6 : 4;
    const httpProtocol = securityOption === "none" ? "http" : "https";

    const shouldDisplayUnsafeMode = nodeData.ipAddress.some(
        ({ ipAddress }) => !genUtils.isLocalhostIpAddress(ipAddress)
    );

    return (
        <RichPanelDetails>
            <Form className="w-100 p-2">
                {isPassiveVisible && (
                    <FormGroup>
                        <FormSwitch name="isPassive" control={control}>
                            <span className="hstack">
                                {t("nodeAddressStep.edit.passive")}
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={t("nodeAddressStep.edit.passiveInfo")}
                                            ravenLinkHash="2WV7N1"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </FormSwitch>
                    </FormGroup>
                )}
                <Row>
                    <Col md={colWidth}>
                        <FormGroup>
                            <FormLabel className="d-flex">
                                {t("nodeAddressStep.edit.nodeTag")}
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={t("nodeAddressStep.edit.nodeTagInfo")}
                                            alert={
                                                <RichAlert variant="info" icon="info" className="mt-1">
                                                    {t("nodeAddressStep.edit.nodeTagFormat")}
                                                </RichAlert>
                                            }
                                            ravenLinkHash="WJJHFY"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput
                                disabled={nodeData.isPassive && isPassiveVisible}
                                placeholder={
                                    nodeData.isPassive && isPassiveVisible
                                        ? t("nodeAddressStep.edit.nodeTagPassivePlaceholder")
                                        : t("nodeAddressStep.edit.nodeTagPlaceholder")
                                }
                                type="text"
                                name="nodeTag"
                                control={control}
                            />
                        </FormGroup>
                    </Col>
                    {isDNSVisible && (
                        <Col md={colWidth}>
                            <FormGroup>
                                <FormLabel className="d-flex">
                                    {t("nodeAddressStep.fields.dnsName")}
                                    <PopoverWithHoverWrapper
                                        message={
                                            <SetupWizardInfoPopover
                                                description={
                                                    <>
                                                        {t("nodeAddressStep.edit.dnsNameInfo")}
                                                        <br />
                                                        {t("nodeAddressStep.edit.dnsNameNote")}
                                                    </>
                                                }
                                            />
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </FormLabel>
                                <FormSelect
                                    name="dnsName"
                                    control={control}
                                    placeholder={t("nodeAddressStep.edit.dnsNamePlaceholder")}
                                    options={cns.map((x) => ({ value: x, label: x }))}
                                />
                            </FormGroup>
                        </Col>
                    )}
                    <Col md={colWidth}>
                        <FormGroup>
                            <FormLabel className="d-flex">
                                {t("nodeAddressStep.fields.httpPort", { context: httpProtocol })}
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={t("nodeAddressStep.edit.httpPortInfo", {
                                                context: httpProtocol,
                                            })}
                                            ravenLinkHash="MGIZZM"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput
                                type="number"
                                name="httpPort"
                                placeholder={t("nodeAddressStep.edit.httpPortPlaceholder", { context: httpProtocol })}
                                control={control}
                            />
                        </FormGroup>
                    </Col>
                    <Col md={colWidth}>
                        <FormGroup>
                            <FormLabel className="d-flex">
                                {t("nodeAddressStep.edit.tcpPort")}
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={t("nodeAddressStep.edit.tcpPortInfo")}
                                            ravenLinkHash="9D6HG1"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput
                                type="number"
                                name="tcpPort"
                                placeholder={t("nodeAddressStep.edit.tcpPortPlaceholder")}
                                control={control}
                            />
                        </FormGroup>
                    </Col>
                </Row>
                <IpAddressList parentControl={parentControl} control={control} />
                <div className="d-flex flex-column gap-1">
                    {securityOption === "letsEncrypt" && nodeData.ipAddress.length > 0 && (
                        <RichAlert variant="info" icon="info">
                            <StudioTrans
                                ns="setupWizard"
                                i18nKey="nodeAddressStep.edit.dnsRecordAlert"
                                options={{
                                    context:
                                        nodeData.ipAddress.length > 1 && !nodeData.externalIpAddress
                                            ? "multiple"
                                            : "single",
                                    domain: `${nodeData.nodeTag.toLowerCase()}.${domainStep.domain.toLowerCase()}.${domainStep.rootDomain}`,
                                    ipAddresses:
                                        nodeData.externalIpAddress && nodeData.hasExternalConfig
                                            ? nodeData.externalIpAddress
                                            : nodeData.ipAddress.length > 0
                                              ? nodeData.ipAddress.map((x) => x.ipAddress).join(", ")
                                              : t("nodeAddressStep.edit.insertIpAddresses"),
                                }}
                                components={{ u: <span className="text-decoration-underline" /> }}
                            />
                        </RichAlert>
                    )}
                    {securityOption === "none" && shouldDisplayUnsafeMode && (
                        <RichAlert variant="warning" icon="warning">
                            <StudioTrans
                                ns="setupWizard"
                                i18nKey="nodeAddressStep.edit.unsecureWarning"
                                components={{ i: <i /> }}
                            />
                        </RichAlert>
                    )}
                </div>

                {(canCustomizeExternalIpsAndPorts || canCustomizeExternalTcpPorts) && (
                    <FormSwitch
                        name="hasExternalConfig"
                        color="primary"
                        disabled={isExternalRequired}
                        control={control}
                        className="mt-3 mb-2"
                    >
                        <span className="d-flex">
                            {t("nodeAddressStep.edit.customizeExternal")}
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description={t("nodeAddressStep.edit.customizeExternalInfo")}
                                        ravenLinkHash="Z112DU"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormSwitch>
                )}
                <Collapse in={nodeData.hasExternalConfig}>
                    <Row>
                        <EditFormExternalAddressInputs
                            control={control}
                            canCustomizeExternalIpsAndPorts={canCustomizeExternalIpsAndPorts}
                            canCustomizeExternalTcpPorts={canCustomizeExternalTcpPorts}
                        />
                    </Row>
                </Collapse>
            </Form>
        </RichPanelDetails>
    );
}

function EditFormExternalAddressInputs({
    control,
    canCustomizeExternalIpsAndPorts,
    canCustomizeExternalTcpPorts,
}: {
    control: Control<NodeEditFormData>;
    canCustomizeExternalIpsAndPorts: boolean;
    canCustomizeExternalTcpPorts: boolean;
}) {
    const t = useStudioTranslation("setupWizard");

    return (
        <>
            <Col>
                <FormGroup className="vstack w-100">
                    <FormLabel>
                        <span className="d-flex">
                            {t("nodeAddressStep.edit.externalIpAddress")}
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description={t("nodeAddressStep.edit.externalIpAddressInfo")}
                                        ravenLinkHash="Z112DU"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormLabel>
                    <FormInput
                        type="text"
                        name="externalIpAddress"
                        placeholder={t("nodeAddressStep.edit.externalIpAddressPlaceholder")}
                        control={control}
                    />
                </FormGroup>
            </Col>
            {canCustomizeExternalIpsAndPorts && (
                <Col>
                    <FormGroup className="vstack w-100">
                        <FormLabel>
                            <span className="d-flex align-items-baseline">
                                <span>{t("nodeAddressStep.edit.externalHttpPort")}&nbsp;</span> <OptionalLabel />
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={t("nodeAddressStep.edit.externalHttpPortInfo")}
                                            ravenLinkHash="Z112DU"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </FormLabel>
                        <FormInput
                            type="number"
                            name="externalHttpPort"
                            placeholder={t("nodeAddressStep.edit.externalHttpPortPlaceholder")}
                            control={control}
                        />
                    </FormGroup>
                </Col>
            )}
            {(canCustomizeExternalIpsAndPorts || canCustomizeExternalTcpPorts) && (
                <Col>
                    <FormGroup className="vstack w-100">
                        <FormLabel>
                            <span className="d-flex align-items-baseline">
                                <span>{t("nodeAddressStep.edit.externalTcpPort")}&nbsp;</span>
                                <OptionalLabel />
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={t("nodeAddressStep.edit.externalTcpPortInfo")}
                                            ravenLinkHash="Z112DU"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </FormLabel>
                        <FormInput
                            type="number"
                            name="externalTcpPort"
                            placeholder={t("nodeAddressStep.edit.externalTcpPortPlaceholder")}
                            control={control}
                        />
                    </FormGroup>
                </Col>
            )}
        </>
    );
}

interface AddAnotherNodeProps {
    onAddNode: () => void;
}

export function AddAnotherNode({ onAddNode }: AddAnotherNodeProps) {
    const t = useStudioTranslation("setupWizard");
    const { getValues } = useFormContext<SetupWizardFormData>();

    const licenseKeyStep = getValues("licenseKeyStep");
    const nodeData = getValues("nodeAddressStep.nodes");

    const hasLicense = !!licenseKeyStep?.licenseInfo;
    const maxClusterSize = getMaxClusterSize(licenseKeyStep?.licenseInfo);
    const isMaxClusterNodes = nodeData?.length >= maxClusterSize;

    return (
        <div
            className={classNames(
                "mt-3 w-100 d-flex rounded justify-content-center mb-2 align-items-center border-dashed border-2",
                isMaxClusterNodes ? "border-secondary" : "border-node"
            )}
        >
            <ConditionalPopover
                conditions={[
                    {
                        isActive: !hasLicense && isMaxClusterNodes,
                        message: (
                            <>
                                <p className="mb-0">
                                    <StudioTrans
                                        ns="setupWizard"
                                        i18nKey="nodeAddressStep.addNode.noLicenseLimit"
                                        components={{ b: <b /> }}
                                    />
                                </p>
                                <hr className="my-2" />
                                <span className="md-label">
                                    <Icon icon="link" /> <SeeLicensesComparison />
                                </span>
                            </>
                        ),
                    },
                    {
                        isActive: isMaxClusterNodes,
                        message: (
                            <>
                                <p className="mb-0">{t("nodeAddressStep.addNode.licenseLimit")}</p>
                                <hr className="my-2" />
                                <span className="md-label">
                                    <Icon icon="link" /> <SeeLicensesComparison />
                                </span>
                            </>
                        ),
                    },
                ]}
            >
                <Button
                    disabled={isMaxClusterNodes}
                    variant={isMaxClusterNodes ? "outline-secondary" : "outline-node"}
                    className={classNames("rounded-pill my-4", {
                        "item-disabled": isMaxClusterNodes,
                    })}
                    onClick={onAddNode}
                >
                    <Icon icon="node-add" />
                    {t("nodeAddressStep.addNode.add")}
                </Button>
            </ConditionalPopover>
        </div>
    );
}

interface UseHostnameDetectionSideEffectsProps {
    editNodeForm: UseFormReturn<NodeEditFormData>;
    parentControl: Control<SetupWizardFormData>;
}

function useHostnameDetectionSideEffects({ editNodeForm, parentControl }: UseHostnameDetectionSideEffectsProps) {
    const { setupWizardService } = useServices();
    const asyncGetSetupParameters = useAsync(async () => setupWizardService.getSetupParameters(), []);
    const { setValue, control, watch, clearErrors } = editNodeForm;
    const { value: wasAutoEnabled, setValue: setWasAutoEnabled } = useBoolean(false);
    const { value: prevHadBindAllIp, setValue: setPrevHadBindAllIp } = useBoolean(false);

    const nodeData = useWatch({ control });
    const {
        securityStep: { securityOption },
        nodeAddressStep: { nodes },
    } = useWatch({ control: parentControl });

    const hasBindAllIp = useMemo(
        () => nodeData.ipAddress.some((ip) => genUtils.isBindAllIpAddress(ip.ipAddress)),
        [nodeData.ipAddress]
    );

    const ipsContainHostname = useMemo(
        () => nodeData.ipAddress.some((ip) => genUtils.isHostname(ip.ipAddress)),
        [nodeData.ipAddress]
    );

    const isHostname = useMemo(
        () => ipsContainHostname && securityOption === "ownCertificate",
        [ipsContainHostname, securityOption]
    );

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            const { hasExternalConfig, ipAddress, isPassive, nodeTag } = values;

            const containsHostname = ipAddress.some((ip) => genUtils.isHostname(ip.ipAddress));
            const containsBindAllIp = ipAddress.some((ip) => genUtils.isBindAllIpAddress(ip.ipAddress));

            if (name === "hasExternalConfig") {
                setWasAutoEnabled(false);

                if (!hasExternalConfig) {
                    setValue("externalIpAddress", "");
                    clearErrors("externalIpAddress");
                }

                return;
            }

            if (isPassive && nodes.length > 1) {
                setValue("isPassive", false);
            }

            if (isPassive && nodes.length > 1 && nodeTag) {
                setValue("nodeTag", "", { shouldValidate: false });
            }

            const shouldAutoDisable =
                !containsBindAllIp &&
                hasExternalConfig &&
                !containsHostname &&
                !asyncGetSetupParameters.result?.IsDocker &&
                wasAutoEnabled &&
                prevHadBindAllIp &&
                securityOption === "letsEncrypt";

            if (shouldAutoDisable) {
                setValue("hasExternalConfig", false, { shouldValidate: true });
                clearErrors("externalIpAddress");
                setWasAutoEnabled(false);
            }

            const shouldAutoEnable =
                securityOption === "letsEncrypt" && (containsHostname || containsBindAllIp) && !hasExternalConfig;

            if (shouldAutoEnable) {
                setValue("hasExternalConfig", true, { shouldValidate: true });
                setWasAutoEnabled(true);
            }

            setPrevHadBindAllIp(containsBindAllIp);
        });

        return () => unsubscribe();
    }, [watch, setValue, securityOption]); // eslint-disable-line react-hooks/exhaustive-deps

    return {
        isHostname,
        isExternalRequired: securityOption !== "ownCertificate" && (isHostname || hasBindAllIp || ipsContainHostname),
    };
}

function IpAddressList({
    control,
    parentControl,
}: {
    control: Control<NodeEditFormData>;
    parentControl: Control<SetupWizardFormData>;
}) {
    const t = useStudioTranslation("setupWizard");
    const { setupWizardService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { append, remove, fields } = useFieldArray<NodeEditFormData>({
        control,
        name: "ipAddress",
    });

    const {
        securityStep: { securityOption },
    } = useWatch({ control: parentControl });

    const addIpAddress = () => {
        append({ ipAddress: "" });
        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "add-ip");
    };

    const asyncGetSetupLocalNodeIps = useAsync(async () => setupWizardService.getSetupLocalNodeIps(), []);

    const localIpAddresses: SelectOption[] = Array.from(new Set(asyncGetSetupLocalNodeIps.result || [])).map((ip) => ({
        label: ip,
        value: ip,
    }));

    const ipAddressesOptions: SelectOption[] = [{ label: "0.0.0.0", value: "0.0.0.0" }, ...localIpAddresses];

    return (
        <FormGroup className="w-100">
            <FormLabel className="w-100">
                <div className="hstack justify-content-between">
                    <span className="d-flex">
                        {t("nodeAddressStep.fields.ipAddress")}
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={t("nodeAddressStep.edit.ipAddressInfo")}
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    {securityOption !== "none" && (
                        <Button
                            variant="link"
                            size="xs"
                            onClick={addIpAddress}
                            title={t("nodeAddressStep.edit.addIpAddressTitle")}
                            className="px-0"
                        >
                            <Icon icon="plus" />
                            {t("nodeAddressStep.edit.addIpAddress")}
                        </Button>
                    )}
                </div>
            </FormLabel>
            <div className="vstack gap-2">
                {fields.map((field, ipIndex) => (
                    <InputGroup key={field.id}>
                        <FormSelectAutocomplete
                            control={control}
                            placeholder={t("nodeAddressStep.edit.ipAddressPlaceholder")}
                            isLoading={asyncGetSetupLocalNodeIps.loading}
                            name={`ipAddress.${ipIndex}.ipAddress`}
                            options={ipAddressesOptions}
                            components={{
                                Option: IpAddressOptionComponent,
                                SingleValue: IpAddressSingleValueComponent,
                            }}
                        />
                        {ipIndex > 0 && (
                            <Button
                                variant="outline-danger"
                                size="sm"
                                onClick={() => {
                                    reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "remove-ip");
                                    remove(ipIndex);
                                }}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </InputGroup>
                ))}
            </div>
        </FormGroup>
    );
}

function IpAddressOptionComponent(props: OptionProps<SelectOption>) {
    const { data } = props;

    return (
        <components.Option {...props} className={classNames(props.className, "hstack")}>
            {data.label}
            <NoRemoteAccessBadge ipAddress={data.value} />
        </components.Option>
    );
}

function IpAddressSingleValueComponent({ children, ...props }: SingleValueProps<SelectOption>) {
    return (
        <components.SingleValue {...props} className={classNames(props.className, "hstack")}>
            {children}
            <NoRemoteAccessBadge ipAddress={props.data.value} />
        </components.SingleValue>
    );
}

function NoRemoteAccessBadge({ ipAddress }: { ipAddress: string }) {
    const t = useStudioTranslation("setupWizard");
    const isLocalhostIpAddress = genUtils.isLocalhostIpAddress(ipAddress);

    if (!isLocalhostIpAddress) {
        return null;
    }

    return (
        <Badge bg="secondary" className="ms-1" pill>
            {t("nodeAddressStep.edit.noRemoteAccess")}
        </Badge>
    );
}

export function SetupWizardNodeAddressStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { setValue, getValues } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const nodeData = getValues("nodeAddressStep.nodes");
    const licenseKeyStep = getValues("licenseKeyStep");

    const maxClusterSize = getMaxClusterSize(licenseKeyStep?.licenseInfo);
    const hasExceededLicenseLimit = nodeData?.length > maxClusterSize;

    const isEditing = nodeData?.some((node) => node.isEditing);

    const confirm = useConfirm();

    const areAllNodesIdentical = (() => {
        if (!nodeData || nodeData.length <= 1) {
            return false;
        }

        const firstNode = nodeData[0];

        const firstNodeIps = firstNode.ipAddress
            .map((ip) => ip.ipAddress)
            .sort()
            .join(",");
        const firstNodeTcpPort = firstNode.tcpPort;
        const firstNodeHttpPort = firstNode.httpPort;

        return nodeData.every((node) => {
            const nodeIps = node.ipAddress
                .map((ip) => ip.ipAddress)
                .sort()
                .join(",");

            return nodeIps === firstNodeIps && node.tcpPort === firstNodeTcpPort && node.httpPort === firstNodeHttpPort;
        });
    })();

    const hasDuplicateNodeConfigurations = (() => {
        const configurationMap = new Map();

        for (const node of nodeData) {
            for (const ip of node.ipAddress) {
                const configKey = `${ip.ipAddress}-${node.httpPort}-${node.tcpPort}`;

                if (configurationMap.has(configKey)) {
                    return true;
                }

                configurationMap.set(configKey, true);
            }
        }

        return false;
    })();

    const handleContinue = async () => {
        const nodeCount = nodeData.length;

        if (areAllNodesIdentical) {
            const firstNode = nodeData[0];
            const isConfirmed = await confirm({
                title: t("nodeAddressStep.continueConfirm.identicalTitle"),
                message: (
                    <>
                        <span>{t("nodeAddressStep.continueConfirm.identicalIntro")}</span>
                        <ul>
                            <li>
                                {t("nodeAddressStep.continueConfirm.identicalIpAddress", {
                                    ipAddress: firstNode.ipAddress[0]?.ipAddress ?? "",
                                })}
                            </li>
                            <li>
                                {t("nodeAddressStep.continueConfirm.identicalTcpPort", {
                                    tcpPort: firstNode.tcpPort ?? "",
                                })}
                            </li>
                            <li>
                                {t("nodeAddressStep.continueConfirm.identicalHttpsPort", {
                                    httpPort: firstNode.httpPort ?? "",
                                })}
                            </li>
                        </ul>
                        {t("nodeAddressStep.continueConfirm.identicalPortConflict")}
                        <br />
                        {t("nodeAddressStep.continueConfirm.identicalConfirmSettings")}
                    </>
                ),
                icon: "warning",
                confirmText: t("nodeAddressStep.continueConfirm.proceed"),
                actionColor: "warning",
                size: "lg",
            });

            if (!isConfirmed) {
                return;
            }
        } else if (hasDuplicateNodeConfigurations) {
            const isConfirmed = await confirm({
                title: t("nodeAddressStep.continueConfirm.duplicateTitle"),
                message: t("nodeAddressStep.continueConfirm.duplicateMessage"),
                icon: "warning",
                confirmText: t("nodeAddressStep.continueConfirm.proceedAnyway"),
                actionColor: "warning",
                size: "lg",
            });

            if (!isConfirmed) {
                return;
            }
        }

        // Then check for even node count
        if (nodeCount % 2 === 0) {
            const isConfirmed = await confirm({
                title: t("nodeAddressStep.continueConfirm.evenCountTitle"),
                message: t("nodeAddressStep.continueConfirm.evenCountMessage"),
                icon: "warning",
                confirmText: t("nodeAddressStep.continueConfirm.proceed"),
                actionColor: "warning",
                size: "lg",
            });

            if (isConfirmed) {
                reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "continue", nodeCount.toString());
                setValue("currentStep", "Additional settings");
            }
        } else {
            reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "continue", nodeCount.toString());
            setValue("currentStep", "Additional settings");
        }
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "back");
        const setupWizardFormData = getValues();
        switch (setupWizardFormData.securityStep.securityOption) {
            case "letsEncrypt":
                setValue("currentStep", "Domain");
                break;
            case "ownCertificate":
                setValue("currentStep", "Self-signed certificate");
                break;
            case "none":
                setValue("currentStep", "Security");
                break;
        }
    };

    const isContinueDisabled = isEditing || hasExceededLicenseLimit || nodeData.length === 0;

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> {t("actions.back")}
            </Button>
            <ConditionalPopover
                conditions={[
                    {
                        isActive: hasExceededLicenseLimit,
                        message: <LicenseLimitExceededMessage />,
                    },
                    {
                        isActive: isEditing && !hasExceededLicenseLimit,
                        message: t("nodeAddressStep.unsavedNodes"),
                    },
                ]}
            >
                <Button
                    disabled={isContinueDisabled}
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                >
                    {t("actions.continue")} <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </ConditionalPopover>
        </div>
    );
}

function useRevalidatePersistedNodesOnEntry() {
    const { setValue, getValues } = useFormContext<SetupWizardFormData>();

    useEffect(() => {
        const nodes = getValues("nodeAddressStep.nodes");
        const securityOption = getValues("securityStep.securityOption");
        const cns = getValues("selfSignedCertificateStep.cns");

        nodes.forEach((node, index) => {
            if (securityOption !== "ownCertificate" && node.dnsName) {
                setValue(`nodeAddressStep.nodes.${index}.dnsName`, null);
            }

            if (securityOption === "ownCertificate" && node.dnsName) {
                const isDnsNameInCns = cns?.some((cn) => cn === node.dnsName);
                if (!isDnsNameInCns) {
                    if (cns.length === 1) {
                        // set first CN if only one is available
                        setValue(`nodeAddressStep.nodes.${index}.dnsName`, cns[0]);
                    } else {
                        setValue(`nodeAddressStep.nodes.${index}.dnsName`, null);
                    }
                }
            }

            /*
             * “Discard” restores the node to its pre-edit state. If the user returned here after changing earlier steps, restoring is not allowed because it would cause a validation error, the node must be either saved or removed.
             * When `isNewlyAdded` is true, the “Remove” button is shown instead of “Discard”.
             */
            setValue(`nodeAddressStep.nodes.${index}.isNewlyAdded`, true, {
                shouldValidate: true,
            });
            setValue(`nodeAddressStep.nodes.${index}.isEditing`, true, {
                shouldValidate: true,
            });
        });
    }, []); // eslint-disable-line react-hooks/exhaustive-deps
}

function LicenseLimitExceededMessage() {
    const { getValues } = useFormContext<SetupWizardFormData>();

    const nodeData = getValues("nodeAddressStep.nodes");
    const licenseKeyStep = getValues("licenseKeyStep");

    const maxClusterSize = getMaxClusterSize(licenseKeyStep?.licenseInfo);
    const currentNodeCount = nodeData?.length;

    return (
        <>
            <p className="mb-0">
                <StudioTrans
                    ns="setupWizard"
                    i18nKey="nodeAddressStep.licenseLimitExceeded.configured"
                    options={{ count: maxClusterSize, configuredCount: currentNodeCount }}
                    components={{ strong: <strong /> }}
                />
            </p>
            <p className="mb-0">
                <StudioTrans
                    ns="setupWizard"
                    i18nKey="nodeAddressStep.licenseLimitExceeded.remove"
                    options={{ count: currentNodeCount - maxClusterSize }}
                    components={{ strong: <strong /> }}
                />
            </p>
            <hr className="my-2" />
            <span className="md-label">
                <Icon icon="link" /> <SeeLicensesComparison />
            </span>
        </>
    );
}

function SeeLicensesComparison() {
    return (
        <StudioTrans
            ns="setupWizard"
            i18nKey="nodeAddressStep.seeLicensesComparison"
            components={{ a: <a href="https://ravendb.net/buy" target="_blank" />, icon: <Icon icon="newtab" /> }}
        />
    );
}

export const ipAddressFormSchema = yup.object().shape({
    ipAddress: yup
        .string()
        .test(
            "not-url",
            () => translate("nodeAddressStep.validation.ipAddressNotUrl"),
            (value) => !value?.startsWith("http://") && !value?.startsWith("https://")
        )
        .test(
            "not-localhost-on-docker",
            () => translate("nodeAddressStep.validation.localhostOnDocker"),
            function (value) {
                const { isDocker } = this.options.context || {};
                return (isDocker && !genUtils.isLocalhostIpAddress(value)) || !isDocker;
            }
        )
        .test(
            "valid-ip-in-unsecure-mode",
            () => translate("nodeAddressStep.validation.hostnameInUnsecureMode"),
            function (value) {
                const context = this.options.context as {
                    nodeAddressStep: SetupWizardFormData["nodeAddressStep"];
                    securityOption: SetupWizardSecurityOption;
                    currentIndex: number;
                };

                const isHostname = genUtils.isHostname(value);
                const isUnsecureMode = context?.securityOption === "none";

                return !isHostname || !isUnsecureMode;
            }
        )
        .required(() => translate("nodeAddressStep.validation.ipAddressRequired")),
});

export const nodeEditFormSchema = yup.object({
    isPassive: yup.boolean().default(false),
    nodeTag: yup.string().when("isPassive", {
        is: false,
        then: (schema) =>
            schema
                .required(() => translate("nodeAddressStep.validation.nodeTagRequired"))
                .matches(/^[A-Z]{1,4}$/, () => translate("nodeAddressStep.validation.nodeTagFormat"))
                .test(
                    "unique",
                    () => translate("nodeAddressStep.validation.nodeTagUnique"),
                    function (value) {
                        const { nodeAddressStep, currentIndex } = this.options.context as {
                            nodeAddressStep: SetupWizardFormData["nodeAddressStep"];
                            setupWizardFormData: SetupWizardFormData;
                            currentIndex: number;
                        };

                        if (!nodeAddressStep || !nodeAddressStep.nodes) {
                            return true;
                        }

                        return (
                            nodeAddressStep.nodes.findIndex(
                                (node, idx) => node.nodeTag === value && idx !== currentIndex
                            ) === -1
                        );
                    }
                )
                .test(
                    "reserved-tag",
                    () => translate("nodeAddressStep.validation.nodeTagReserved"),
                    (value) => {
                        return value !== "RAFT";
                    }
                ),
        otherwise: (schema) => schema,
    }),
    dnsName: yup
        .string()
        .nullable()
        .test(
            "required-for-non-wildcard",
            () => translate("nodeAddressStep.validation.dnsNameRequired"),
            function (value) {
                const { securityOption, isWildcardCertificate } = this.options.context as {
                    securityOption: SetupWizardSecurityOption;
                    isWildcardCertificate: boolean;
                    cns: string[];
                };

                if (securityOption === "ownCertificate" && !isWildcardCertificate) {
                    return !!value;
                }

                return true;
            }
        )
        .test(
            "valid-cn",
            () => translate("nodeAddressStep.validation.dnsNameFromCertificate"),
            function (value) {
                const { securityOption, isWildcardCertificate, cns } = this.options.context as {
                    securityOption: SetupWizardSecurityOption;
                    isWildcardCertificate: boolean;
                    cns: string[];
                };

                if (securityOption === "ownCertificate" && !isWildcardCertificate && value) {
                    return cns.includes(value);
                }

                return true;
            }
        )
        .test(
            "unique",
            () => translate("nodeAddressStep.validation.dnsNameUnique"),
            function (value) {
                const { securityOption, isWildcardCertificate, nodeAddressStep, currentIndex } = this.options
                    .context as {
                    securityOption: SetupWizardSecurityOption;
                    isWildcardCertificate: boolean;
                    nodeAddressStep: SetupWizardFormData["nodeAddressStep"];
                    currentIndex: number;
                };

                if (securityOption !== "ownCertificate" || isWildcardCertificate || !value || !nodeAddressStep?.nodes) {
                    return true;
                }

                const httpPort = this.parent.httpPort ?? 443;

                return (
                    nodeAddressStep.nodes.findIndex(
                        (node, idx) =>
                            idx !== currentIndex &&
                            node.dnsName?.toLowerCase() === value.toLowerCase() &&
                            (node.httpPort ?? 443) === httpPort
                    ) === -1
                );
            }
        ),
    httpPort: yup
        .number()
        .nullable()
        .typeError(() => translate("nodeAddressStep.validation.httpPortNumber"))
        .min(1, () => translate("nodeAddressStep.validation.portMin"))
        .max(65535, () => translate("nodeAddressStep.validation.portMax")), // 'required' validation is handled separately when saving
    tcpPort: yup
        .number()
        .default(38888)
        .nullable()
        .min(1, () => translate("nodeAddressStep.validation.portMin"))
        .max(65535, () => translate("nodeAddressStep.validation.portMax"))
        .required(() => translate("nodeAddressStep.validation.tcpPortRequired")),
    ipAddress: yup
        .array()
        .of(ipAddressFormSchema)
        .min(1, () => translate("nodeAddressStep.validation.ipAddressMin")),
    hasExternalConfig: yup.boolean().default(false),
    externalIpAddress: yup
        .string()
        .nullable()
        .when(["hasExternalConfig", "ipAddress", "$securityOption"], {
            is: function (
                hasExtConfig: boolean,
                ipAddresses: NodeEditFormData["ipAddress"],
                securityOption: SetupWizardSecurityOption
            ) {
                if (securityOption !== "letsEncrypt") {
                    return false;
                }

                const hasZeroAddress = ipAddresses?.some((ip) => ip?.ipAddress === "0.0.0.0");

                return hasExtConfig || hasZeroAddress;
            },
            then: (schema: yup.StringSchema) =>
                schema
                    .required(() => translate("nodeAddressStep.validation.externalIpAddressRequired"))
                    .test(
                        "not-url",
                        () => translate("nodeAddressStep.validation.externalIpAddressNotUrl"),
                        (value) => !value?.startsWith("http://") && !value?.startsWith("https://")
                    )
                    .test(
                        "valid-ipv4",
                        () => translate("nodeAddressStep.validation.externalIpAddressIpv4"),
                        (value) => !!value && genUtils.regexIPv4.test(value)
                    ),
            otherwise: (schema: yup.StringSchema) =>
                schema.test(
                    "valid-ipv4-optional",
                    () => translate("nodeAddressStep.validation.externalIpAddressIpv4"),
                    (value) => {
                        if (value == null || value === "") {
                            return true;
                        }
                        return genUtils.regexIPv4.test(value);
                    }
                ),
        }),

    externalHttpPort: yup
        .number()
        .nullable()
        .when("hasExternalConfig", {
            is: true,
            then: (schema) =>
                schema
                    .min(1, () => translate("nodeAddressStep.validation.portMin"))
                    .max(65535, () => translate("nodeAddressStep.validation.portMax")),
        }),
    externalTcpPort: yup
        .number()
        .nullable()
        .when("hasExternalConfig", {
            is: true,
            then: (schema) =>
                schema
                    .min(1, () => translate("nodeAddressStep.validation.portMin"))
                    .max(65535, () => translate("nodeAddressStep.validation.portMax")),
        }),
});

export type NodeEditFormData = yup.InferType<typeof nodeEditFormSchema>;
