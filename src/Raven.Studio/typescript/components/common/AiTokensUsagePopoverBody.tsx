import genUtils from "common/generalUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface AiTokensUsagePopoverBodyProps {
    prompt: number;
    completion: number;
    reasoning: number;
    cached: number;
    total: number;
}

export default function AiTokensUsagePopoverBody({
    prompt,
    completion,
    cached,
    reasoning,
    total,
}: AiTokensUsagePopoverBodyProps) {
    const t = useStudioTranslation("commonComponents");

    return (
        <div>
            <div className="hstack justify-content-between gap-3">
                <span>{t("aiTokensUsage.prompt")}</span>
                <span>{genUtils.formatAiTokens(prompt)}</span>
            </div>
            <div className="hstack justify-content-between gap-3">
                <span>{t("aiTokensUsage.completion")}</span>
                <span>{genUtils.formatAiTokens(completion)}</span>
            </div>
            <div className="hstack justify-content-between gap-3">
                <span>{t("aiTokensUsage.reasoning")}</span>
                <span>{genUtils.formatAiTokens(reasoning)}</span>
            </div>
            <div className="hstack justify-content-between gap-3">
                <span>{t("aiTokensUsage.cached")}</span>
                <span>{genUtils.formatAiTokens(cached)}</span>
            </div>
            <hr className="my-1" />
            <div className="hstack justify-content-between gap-3">
                <span>{t("aiTokensUsage.total")}</span>
                <span>{genUtils.formatAiTokens(total)}</span>
            </div>
        </div>
    );
}
