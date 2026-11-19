import "./ChatbotAskAiKeywordsSlider.scss";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import { useAppSelector } from "components/store";
import { useState, useEffect } from "react";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

const KEYWORDS: { wordKey: "learn" | "query" | "analyze" | "troubleshoot"; icon?: IconName }[] = [
    { wordKey: "learn", icon: "learn" },
    { wordKey: "query", icon: "query" },
    { wordKey: "analyze", icon: "analyze" },
    { wordKey: "troubleshoot", icon: "hammer-driver" },
] as const;

export default function KeywordsSlider() {
    const [index, setIndex] = useState(0);
    const [isVisible, setIsVisible] = useState(true);
    const isChatbotOpen = useAppSelector(chatbotSelectors.isOpen);
    const t = useStudioTranslation("chatbot");

    useEffect(() => {
        if (!isChatbotOpen) {
            return;
        }

        const id = setInterval(() => {
            setIsVisible(false);

            setTimeout(() => {
                setIndex((prev) => (prev + 1) % KEYWORDS.length);
                setIsVisible(true);
            }, 300);
        }, 3000);

        return () => clearInterval(id);
    }, [isChatbotOpen]);

    const { wordKey, icon } = KEYWORDS[index];

    return (
        <div className="d-flex justify-content-center align-items-center overflow-hidden">
            <h3
                className={classNames(
                    "keyword-pill font-monospace well rounded-pill fw-bold py-1 px-2 mb-0 font-size-16",
                    isVisible ? "is-visible" : "is-hidden"
                )}
            >
                {icon && <Icon icon={icon} className="text-primary" />}
                {t(`welcome.keywords.${wordKey}`)}
            </h3>
        </div>
    );
}
