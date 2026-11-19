import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { useRavenLink } from "components/hooks/useRavenLink";
import { VStack } from "components/common/utilities/VStack";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface JoinCommunityContentProps {
    openFeedbackForm: () => void;
}

export function JoinCommunityContent({ openFeedbackForm }: JoinCommunityContentProps) {
    const gitHubDiscussionsUrl = useRavenLink({ hash: "ITXUEA" });
    const discordServerUrl = useRavenLink({ hash: "ZL8MVM" });
    const t = useStudioTranslation("chatbot");

    return (
        <>
            <ul className="action-menu__list">
                <p className="m-0">{t("joinCommunity.intro")}</p>
                <VStack gap={1} className="mt-1">
                    <li
                        className="action-menu__list-item action-menu__list-item--primary"
                        role="button"
                        title={t("joinCommunity.goToDiscord")}
                        onClick={() => window.open(discordServerUrl, "_blank")}
                    >
                        <Icon icon="discord" margin="m-0" />
                        {t("joinCommunity.discordCommunity")}
                        <FlexGrow />
                        <Icon icon="newtab" margin="m-0" />
                    </li>
                    <li
                        className="action-menu__list-item action-menu__list-item--primary"
                        role="button"
                        title={t("helpAndResources.goToGithubDiscussions")}
                        onClick={() => window.open(gitHubDiscussionsUrl, "_blank")}
                    >
                        <Icon icon="github" margin="m-0" />
                        {t("helpAndResources.githubDiscussions")}
                        <FlexGrow />
                        <Icon icon="newtab" margin="m-0" />
                    </li>
                </VStack>
            </ul>
            <FlexGrow />
            <div className="action-menu__footer">
                <small>
                    <Icon icon="feedback" />
                    <StudioTrans
                        ns="chatbot"
                        i18nKey="joinCommunity.submitFeedback"
                        components={{
                            span: (
                                <span
                                    role="link"
                                    className="text-decoration-underline hover-filter cursor-pointer m-0"
                                    onClick={openFeedbackForm}
                                />
                            ),
                        }}
                    />
                </small>
            </div>
        </>
    );
}
