import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { chatbotActions, chatbotSelectors } from "./store/chatbotSlice";
import classNames from "classnames";
import "./ChatbotNavIcon.scss";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function ChatbotNavIcon() {
    const dispatch = useAppDispatch();
    const isOpen = useAppSelector(chatbotSelectors.isOpen);
    const t = useStudioTranslation("chatbot");

    return (
        <Button
            title={t("knowledgeCenter")}
            variant="link"
            className={classNames("chatbot-nav-icon", { active: isOpen })}
            onClick={() => dispatch(chatbotActions.isOpenToggled())}
        >
            <Icon icon="ai-assistant" margin="m-0" className="icon-chatbot" />
        </Button>
    );
}
