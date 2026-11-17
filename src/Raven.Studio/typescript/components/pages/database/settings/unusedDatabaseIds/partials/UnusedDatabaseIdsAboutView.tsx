import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function UnusedDatabaseIdsAboutView() {
    const t = useStudioTranslation("unusedDatabaseIds");

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <ul>
                    <li className="margin-top-xm">
                        <StudioTrans
                            ns="unusedDatabaseIds"
                            i18nKey="about.usedIdsItem"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li className="margin-top-xs">{t("about.scenariosItem")}</li>
                    <li className="margin-top-xs">
                        <StudioTrans
                            ns="unusedDatabaseIds"
                            i18nKey="about.unusedIdsItem"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li className="margin-top-xs">
                        <StudioTrans ns="unusedDatabaseIds" i18nKey="about.noteItem" components={{ br: <br /> }} />
                    </li>
                </ul>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
