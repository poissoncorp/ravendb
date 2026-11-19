import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function ServerWideTasksInfoHub() {
    const t = useStudioTranslation("serverWideTasks");

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading={t("about.heading")}
                description={t("about.description")}
                targetId="about"
            >
                <p>
                    <StudioTrans ns="serverWideTasks" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="serverWideTasks"
                            i18nKey="about.backupItem"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="serverWideTasks"
                            i18nKey="about.replicationItem"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                </ul>
                <p>{t("about.exclusion")}</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
