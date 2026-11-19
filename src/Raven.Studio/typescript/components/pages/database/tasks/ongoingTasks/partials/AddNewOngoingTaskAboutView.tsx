import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function AddNewOngoingTaskAboutView() {
    const t = useStudioTranslation("ongoingTasks");

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>
                            <StudioTrans ns="ongoingTasks" i18nKey="addTask.about.intro" components={{ br: <br /> }} />
                        </li>
                        <li className="mt-1">
                            {t("addTask.about.categoriesIntro")}
                            <ul className="mt-1">
                                <li>{t("addTask.about.categories.ai")}</li>
                                <li>{t("addTask.about.categories.replication")}</li>
                                <li>{t("addTask.about.categories.backups")}</li>
                                <li>{t("addTask.about.categories.subscriptions")}</li>
                                <li>{t("addTask.about.categories.etl")}</li>
                                <li>{t("addTask.about.categories.sink")}</li>
                            </ul>
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
