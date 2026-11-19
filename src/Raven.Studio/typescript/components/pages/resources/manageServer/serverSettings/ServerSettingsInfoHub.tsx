import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function ServerSettingsInfoHub() {
    const t = useStudioTranslation("serverSettings");
    const commonT = useStudioTranslation("common");
    const configurationOverviewDocsLink = useRavenLink({ hash: "6H3QC3" });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="serverSettings" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <div className="mb-4">
                    <ul>
                        <li className="mb-1">
                            <StudioTrans
                                ns="serverSettings"
                                i18nKey="about.configurationKeyItem"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="mb-1">
                            <StudioTrans
                                ns="serverSettings"
                                i18nKey="about.effectiveValueItem"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="serverSettings"
                                i18nKey="about.originItem"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                    </ul>
                </div>
                <p>
                    <StudioTrans
                        ns="serverSettings"
                        i18nKey="about.readOnly"
                        components={{
                            br: <br />,
                            i: <i />,
                            a: <a href={configurationOverviewDocsLink} target="_blank" />,
                        }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="serverSettings"
                        i18nKey="about.databaseOverride"
                        components={{ br: <br />, strong: <strong /> }}
                    />
                </p>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={configurationOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
