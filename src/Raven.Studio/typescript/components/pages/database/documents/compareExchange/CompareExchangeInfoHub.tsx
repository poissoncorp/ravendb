import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function CompareExchangeInfoHub() {
    const t = useStudioTranslation("compareExchange");
    const commonT = useStudioTranslation("common");
    const docsLink = useRavenLink({ hash: "2BGJN2" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading={t("about.heading")}
                description={t("about.description")}
            >
                <p>
                    <StudioTrans ns="compareExchange" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <p className="mb-0">{t("about.consistency")}</p>
                <hr />
                <p>{t("about.inThisView")}</p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="compareExchange"
                            i18nKey="about.keyColumn"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="compareExchange"
                            i18nKey="about.valueColumn"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="compareExchange"
                            i18nKey="about.metadataColumn"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li>
                        <StudioTrans
                            ns="compareExchange"
                            i18nKey="about.raftIndexColumn"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                </ul>
                <p className="mb-0">
                    <StudioTrans ns="compareExchange" i18nKey="about.actions" components={{ br: <br /> }} />
                </p>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={docsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
