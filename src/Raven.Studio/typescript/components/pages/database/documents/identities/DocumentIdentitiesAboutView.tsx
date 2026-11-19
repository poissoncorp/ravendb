import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { useRavenLink } from "hooks/useRavenLink";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function DocumentIdentitiesAboutView() {
    const t = useStudioTranslation("identities");
    const commonT = useStudioTranslation("common");
    const identitiesDocsLink = useRavenLink({ hash: "KOVX2N" });

    return (
        <AboutViewAnchored className="my-4">
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans ns="identities" i18nKey="about.identity" components={{ strong: <strong /> }} />
                </p>
                <p>{t("about.composition")}</p>
                <p>
                    <StudioTrans
                        ns="identities"
                        i18nKey="about.inThisView"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="identities"
                            i18nKey="about.prefixColumn"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="identities"
                            i18nKey="about.latestValueColumn"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={identitiesDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
