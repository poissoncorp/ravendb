import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AiTasksInfoHub() {
    const t = useStudioTranslation("aiTasks");
    const commonT = useStudioTranslation("common");
    const genAiTaskOverviewDocsLink = useRavenLink({ hash: "GEHC2I" });
    const embeddingsGenerationOverviewDocsLink = useRavenLink({ hash: "H6GDNY" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <p>
                    <StudioTrans ns="aiTasks" i18nKey="about.intro" components={{ br: <br /> }} />
                </p>
                <div>
                    <StudioTrans ns="aiTasks" i18nKey="about.genAi.heading" components={{ strong: <strong /> }} />
                    <ul>
                        <li>{t("about.genAi.enrichDocuments")}</li>
                        <li>{t("about.genAi.updateDocuments")}</li>
                    </ul>
                </div>
                <div>
                    <StudioTrans ns="aiTasks" i18nKey="about.embeddings.heading" components={{ strong: <strong /> }} />
                    <ul>
                        <li>{t("about.embeddings.extractText")}</li>
                        <li>{t("about.embeddings.generateEmbeddings")}</li>
                        <li>{t("about.embeddings.saveEmbeddings")}</li>
                        <li>{t("about.embeddings.vectorSearch")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={genAiTaskOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.genAiDocsLink")}
                </a>
                <br />
                <a href={embeddingsGenerationOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.embeddingsDocsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
