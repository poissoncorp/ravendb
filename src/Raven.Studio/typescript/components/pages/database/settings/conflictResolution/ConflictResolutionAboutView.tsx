import React from "react";
import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ConflictResolutionAboutView() {
    const conflictResolutionDocsLink = useRavenLink({ hash: "QRCNKH" });
    const { forCurrentDatabase } = useAppUrls();
    const t = useStudioTranslation("conflictResolution");
    const commonT = useStudioTranslation("common");

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans ns="conflictResolution" i18nKey="about.intro" components={{ br: <br /> }} />
                </p>
                <p>
                    <StudioTrans
                        ns="conflictResolution"
                        i18nKey="about.behavior"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <ol>
                    <li>
                        <StudioTrans ns="conflictResolution" i18nKey="about.scriptItem" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xs">
                        {t("about.noScriptItem")}
                        <br />
                        <ul>
                            <li className="margin-top-xxs">
                                <StudioTrans
                                    ns="conflictResolution"
                                    i18nKey="about.latestOnItem"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </li>
                            <li className="margin-top-xxs">
                                <StudioTrans
                                    ns="conflictResolution"
                                    i18nKey="about.latestOffItem"
                                    components={{
                                        br: <br />,
                                        a: <a href={forCurrentDatabase.conflicts()} target="_blank" />,
                                    }}
                                />
                            </li>
                        </ul>
                    </li>
                </ol>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={conflictResolutionDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
