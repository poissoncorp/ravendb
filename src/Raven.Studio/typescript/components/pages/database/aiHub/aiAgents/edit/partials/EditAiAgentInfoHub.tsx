import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditAiAgentInfoHub() {
    const t = useStudioTranslation("editAiAgent");
    const commonT = useStudioTranslation("common");
    const aiAgentsOverviewDocsLink = useRavenLink({ hash: "XNWAXB" });
    const aiAgentsCreateAgentDocsLink = useRavenLink({ hash: "72VEQI" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <StudioTrans
                        ns="editAiAgent"
                        i18nKey="about.intro"
                        components={{ br: <br />, strong: <strong /> }}
                    />
                    <br />
                    <br />
                    {t("about.chat")}
                    <br />
                    <br />
                    {t("about.configurationIncludes")}
                    <ul className="mt-1">
                        <li>
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="about.connectionString"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="about.responseStructure"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="about.chatStorage"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="mt-1">
                            <StudioTrans ns="editAiAgent" i18nKey="about.tools" components={{ strong: <strong /> }} />
                            <ul>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editAiAgent"
                                        i18nKey="about.queryTools"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editAiAgent"
                                        i18nKey="about.actionTools"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                            </ul>
                        </li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="about.subAgents"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={aiAgentsOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.overviewDocsLink")}
                </a>
                <br />
                <a href={aiAgentsCreateAgentDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.createDocsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
