import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function TasksErrorsAboutView() {
    const t = useStudioTranslation("tasksErrors");
    const commonT = useStudioTranslation("common");
    const taskErrorsOverviewDocsLink = useRavenLink({ hash: "17261M" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info">
                <p>
                    <StudioTrans ns="tasksErrors" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <h4>{t("about.errorTypesHeading")}</h4>
                <ul>
                    <li>
                        <StudioTrans ns="tasksErrors" i18nKey="about.itemError" components={{ strong: <strong /> }} />
                    </li>
                    <li>
                        <StudioTrans
                            ns="tasksErrors"
                            i18nKey="about.processError"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                </ul>
                <h4>{t("about.errorStepsHeading")}</h4>
                <ul>
                    <li>
                        <StudioTrans
                            ns="tasksErrors"
                            i18nKey="about.transformationStep"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans ns="tasksErrors" i18nKey="about.loadStep" components={{ strong: <strong /> }} />
                    </li>
                    <li>
                        <StudioTrans
                            ns="tasksErrors"
                            i18nKey="about.modelInferenceStep"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="tasksErrors"
                            i18nKey="about.persistenceStep"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="tasksErrors"
                            i18nKey="about.configurationStep"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                </ul>
                <h4>{t("about.taskHealthHeading")}</h4>
                <ul>
                    <li className="mb-2">
                        <StudioTrans ns="tasksErrors" i18nKey="about.healthStatus" components={{ br: <br /> }} />
                        <ul>
                            <li>
                                <StudioTrans
                                    ns="tasksErrors"
                                    i18nKey="about.healthy"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                            <li>
                                <StudioTrans
                                    ns="tasksErrors"
                                    i18nKey="about.impaired"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                            <li>
                                <StudioTrans
                                    ns="tasksErrors"
                                    i18nKey="about.failed"
                                    components={{ strong: <strong /> }}
                                />
                            </li>
                        </ul>
                    </li>
                    <li>{t("about.healthRecovery")}</li>
                </ul>
                <h4>{t("about.errorStorageHeading")}</h4>
                <ul>
                    <li className="mb-2">{t("about.storageLimit")}</li>
                    <li>
                        <StudioTrans ns="tasksErrors" i18nKey="about.manualDelete" components={{ br: <br /> }} />
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={taskErrorsOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
