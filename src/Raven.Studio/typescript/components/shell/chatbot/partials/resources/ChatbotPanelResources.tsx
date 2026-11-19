import "./ChatbotPanelResources.scss";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import SeeDocumentationButton from "./partials/SeeDocumentationButton";
import GithubButtonWithStars from "./partials/GithubButtonWithStars";
import { JoinCommunityContent } from "./partials/JoinCommunityContent";
import { FeedbackFormContent } from "./partials/FeedbackFormContent";
import { ContactSupportContent } from "./partials/ContactSupportContent";
import { chatbotActions, chatbotSelectors } from "../../store/chatbotSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function ChatbotPanelResources() {
    const dispatch = useAppDispatch();
    const activeTab = useAppSelector(chatbotSelectors.chatbotResourcesTab);
    const t = useStudioTranslation("chatbot");

    return (
        <div className="chatbot-panel-resources vstack">
            {activeTab === "helpAndResources" && (
                <div className="vstack">
                    <ul className="action-menu__list flex-grow">
                        <p className="mb-0">{t("helpAndResources.intro")}</p>
                        <li
                            className="action-menu__list-item action-menu__list-item--highlight"
                            role="button"
                            title={t("helpAndResources.joinTheCommunity")}
                            onClick={() => dispatch(chatbotActions.chatbotResourcesTabSet("joinTheCommunity"))}
                        >
                            <Icon icon="group" margin="m-0" />
                            {t("helpAndResources.joinTheCommunity")}
                        </li>
                        <li
                            className="action-menu__list-item"
                            role="button"
                            title={t("helpAndResources.contactSupport")}
                            onClick={() => dispatch(chatbotActions.chatbotResourcesTabSet("contactSupport"))}
                        >
                            <Icon icon="support" margin="m-0" />
                            {t("helpAndResources.contactSupport")}
                        </li>
                        <SeeDocumentationButton />
                    </ul>
                    <FlexGrow />
                    <div className="action-menu__footer">
                        <div className="d-flex flex-row align-items-center">
                            <small className="lh-1">{t("helpAndResources.githubProject")}</small>
                            <FlexGrow />
                            <GithubButtonWithStars />
                        </div>
                    </div>
                </div>
            )}
            {activeTab === "joinTheCommunity" && (
                <JoinCommunityContent
                    openFeedbackForm={() => dispatch(chatbotActions.chatbotResourcesTabSet("submitFeedback"))}
                />
            )}
            {activeTab === "submitFeedback" && (
                <FeedbackFormContent
                    goBack={() => dispatch(chatbotActions.chatbotResourcesTabSet("helpAndResources"))}
                />
            )}
            {activeTab === "contactSupport" && <ContactSupportContent />}
        </div>
    );
}
