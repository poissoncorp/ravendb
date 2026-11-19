import useConfirm from "components/common/ConfirmDialog";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { useAsyncCallback } from "react-async-hook";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import Badge from "react-bootstrap/Badge";
import classNames from "classnames";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import AiAgentGenerateCodeViewSheet from "components/pages/database/aiHub/aiAgents/partials/AiAgentGenerateCodeViewSheet";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface AiAgentCardProps {
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
    reloadAiAgents: () => void;
}

export default function AiAgentCard({ config, reloadAiAgents }: AiAgentCardProps) {
    const t = useStudioTranslation("aiAgents");
    const commonT = useStudioTranslation("common");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const { aiAgentService } = useServices();
    const confirm = useConfirm();

    const { appUrl } = useAppUrls();

    const asyncDeleteAiAgent = useAsyncCallback(() => aiAgentService.deleteAiAgent(databaseName, config.Identifier), {
        onSuccess: reloadAiAgents,
    });

    const asyncToggleAiAgent = useAsyncCallback(
        (isDisabled: boolean) => aiAgentService.saveAiAgent(databaseName, { ...config, Disabled: isDisabled }),
        { onSuccess: reloadAiAgents }
    );

    const codeViewSheet = useViewSheet();

    const handleDelete = async () => {
        const isConfirmed = await confirm({
            title: (
                <StudioTrans
                    ns="aiAgents"
                    i18nKey="card.deleteConfirm.title"
                    components={{ strong: <strong /> }}
                    options={{ name: config.Name }}
                />
            ),
            message: (
                <div className="text-center">
                    <StudioTrans ns="aiAgents" i18nKey="card.deleteConfirm.message" components={{ br: <br /> }} />
                </div>
            ),
            icon: "trash",
            confirmText: t("card.deleteConfirm.confirmText"),
            actionColor: "danger",
        });

        if (isConfirmed) {
            asyncDeleteAiAgent.execute();
        }
    };

    return (
        <div className="panel-bg-1 p-2 rounded-2 border border-secondary g-col-12 g-col-xl-6 g-col-xxl-4">
            <div className="hstack mb-1">
                <h4 className="m-0 text-truncate flex-grow" title={t("card.nameTitle")}>
                    {config.Name}
                </h4>
                {config.Disabled && (
                    <Badge bg="warning" className="ms-2 fs-6" pill>
                        {t("card.disabled")}
                    </Badge>
                )}
            </div>
            <div className="d-flex">
                <div className="text-truncate text-muted fs-5" title={t("card.identifierTitle")}>
                    {config.Identifier}
                </div>
                <Button
                    onClick={() => copyToClipboard.copy(config.Identifier, t("card.identifierCopied"))}
                    size="xs"
                    title={t("card.copyToClipboard")}
                    variant="link"
                    className="p-0"
                >
                    <Icon icon="copy-to-clipboard" margin="ms-1" />
                </Button>
            </div>
            <div className="mt-2 text-truncate" title={config.SystemPrompt}>
                {config.SystemPrompt}
            </div>
            <div className="hstack justify-content-between mt-2">
                {hasDatabaseWriteAccess && (
                    <a
                        href={appUrl.forChatAiAgent(databaseName, config.Identifier)}
                        className={classNames("btn btn-primary", { disabled: config.Disabled })}
                    >
                        <Icon icon="llm" />
                        {t("card.startNewChat")}
                    </a>
                )}
                {hasDatabaseAdminAccess && (
                    <Dropdown>
                        <Dropdown.Toggle as={CustomDropdownToggle} isCaretHidden variant="secondary">
                            <Icon icon="more" margin="m-0" />
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item
                                onClick={() => {
                                    codeViewSheet.open({
                                        component: <AiAgentGenerateCodeViewSheet agentId={config.Identifier} />,
                                    });
                                }}
                            >
                                <Icon icon="magic-wand" />
                                {t("card.generateClientCode")}
                            </Dropdown.Item>
                            <Dropdown.Item href={appUrl.forEditAiAgent(databaseName, config.Identifier)}>
                                <Icon icon="edit" />
                                {t("card.edit")}
                            </Dropdown.Item>
                            <Dropdown.Item href={appUrl.forEditAiAgent(databaseName, config.Identifier, true)}>
                                <Icon icon="copy" />
                                {t("card.clone")}
                            </Dropdown.Item>
                            <Dropdown.Item
                                onClick={() => asyncToggleAiAgent.execute(!config.Disabled)}
                                disabled={asyncToggleAiAgent.loading}
                            >
                                {asyncToggleAiAgent.loading ? (
                                    <Spinner size="sm" className="me-1" />
                                ) : (
                                    <Icon icon={config.Disabled ? "play" : "stop"} />
                                )}
                                {config.Disabled ? t("card.enable") : t("card.disable")}
                            </Dropdown.Item>{" "}
                            <Dropdown.Item
                                className="text-danger"
                                onClick={handleDelete}
                                disabled={asyncDeleteAiAgent.loading}
                            >
                                {asyncDeleteAiAgent.loading ? (
                                    <Spinner size="sm" className="me-1" />
                                ) : (
                                    <Icon icon="trash" />
                                )}
                                {commonT("delete")}
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
                )}
            </div>
        </div>
    );
}
