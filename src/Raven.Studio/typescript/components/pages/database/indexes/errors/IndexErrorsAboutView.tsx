import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function IndexErrorsAboutView() {
    const t = useStudioTranslation("indexErrors");

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="indexErrors" i18nKey="about.intro" components={{ i: <i /> }} />
                </p>
                <p>{t("about.listIntro")}</p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="indexErrors"
                            i18nKey="about.filterItem"
                            components={{ strong: <strong />, br: <br />, i: <i /> }}
                        />
                    </li>
                    <li className="mt-1">
                        <StudioTrans
                            ns="indexErrors"
                            i18nKey="about.deleteItem"
                            components={{ strong: <strong />, br: <br />, b: <b /> }}
                        />
                    </li>
                </ul>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
