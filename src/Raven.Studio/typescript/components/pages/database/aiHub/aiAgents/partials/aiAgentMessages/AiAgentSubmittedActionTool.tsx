import { Icon } from "components/common/Icon";
import { AiAgentToolResponseContent } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentToolResponseContent";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface AiAgentSubmittedActionToolProps {
    content: string;
    toolName: string;
}

export function AiAgentSubmittedActionTool({ content, toolName }: AiAgentSubmittedActionToolProps) {
    const t = useStudioTranslation("aiAgents");
    return (
        <div className="bg-faded-primary p-2 border-radius-xs border border-primary w-100">
            <div className="hstack justify-content-between mb-1">
                <div>
                    <StudioTrans
                        ns="aiAgents"
                        i18nKey="messages.actionToolResponse"
                        components={{ strong: <strong /> }}
                        options={{ toolName }}
                    />
                </div>
                <Badge bg="primary" pill>
                    <Icon icon="check" /> {t("messages.submitted")}
                </Badge>
            </div>

            <AiAgentToolResponseContent content={content} />
        </div>
    );
}
