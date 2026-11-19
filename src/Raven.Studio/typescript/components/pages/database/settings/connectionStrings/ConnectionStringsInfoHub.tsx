import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import useConnectionStringsLicense from "./useConnectionStringsLicense";
import appUrl from "common/appUrl";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function ConnectionStringsInfoHub() {
    const t = useStudioTranslation("connectionStrings");
    const commonT = useStudioTranslation("common");
    const { hasAll, featureAvailability } = useConnectionStringsLicense();

    const connectionStringsOverviewDocsLink = useRavenLink({ hash: "P5XJOV" });
    const connectionStringsPerDatabaseDocsLink = useRavenLink({ hash: "BOYDGL" });

    return (
        <AboutViewAnchored defaultOpen={hasAll ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <ul>
                        <li>{t("about.dataStorage")}</li>
                        <li className="margin-top-xxs">{t("about.manage")}</li>
                        <li className="margin-top-xxs">{t("about.createdInTask")}</li>
                        <li className="margin-top-xxs">{t("about.inUse")}</li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="about.serverWide"
                                components={{ a: <a href={appUrl.forServerWideConnectionStrings()} target="_blank" /> }}
                            />
                        </li>
                    </ul>
                    <hr />
                    <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                    <a href={connectionStringsOverviewDocsLink} target="_blank">
                        <Icon icon="newtab" /> {t("about.docsOverview")}
                    </a>
                    <br />
                    <a href={connectionStringsPerDatabaseDocsLink} target="_blank">
                        <Icon icon="newtab" /> {t("about.docsPerDatabase")}
                    </a>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={featureAvailability}
                isUnlimited={hasAll}
                isOpenedByDefault={false}
            />
        </AboutViewAnchored>
    );
}
