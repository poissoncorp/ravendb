import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function DatabaseRecordAboutView() {
    const databaseRecordDocsLink = useRavenLink({ hash: "LJNTFQ" });
    const t = useStudioTranslation("databaseRecord");
    const commonT = useStudioTranslation("common");

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <ul>
                    <li>{t("about.storesItem")}</li>
                    <li className="margin-top-sm">
                        <StudioTrans ns="databaseRecord" i18nKey="about.editingItem" components={{ br: <br /> }} />
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={databaseRecordDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
