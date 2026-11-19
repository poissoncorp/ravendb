import React from "react";
import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import { useAppUrls } from "hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AllRevisionsAboutView() {
    const t = useStudioTranslation("allRevisions");
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="allRevisions"
                        i18nKey="about.intro"
                        components={{ br: <br />, strong: <strong /> }}
                    />
                    <p className="mt-3">
                        <StudioTrans
                            ns="allRevisions"
                            i18nKey="about.regularHeading"
                            components={{ strong: <strong /> }}
                        />
                        <ul>
                            <li>{t("about.regularCreated")}</li>
                            <li>
                                <StudioTrans
                                    ns="allRevisions"
                                    i18nKey="about.regularDeletable"
                                    components={{ br: <br /> }}
                                />
                            </li>
                        </ul>
                    </p>
                    <p>
                        <StudioTrans
                            ns="allRevisions"
                            i18nKey="about.deletedHeading"
                            components={{ strong: <strong /> }}
                        />
                        <ul>
                            <li>{t("about.deletedCreated")}</li>
                            <li>
                                {t("about.deletedDeletableFrom")}
                                <ul className="no-padding-left">
                                    <li>
                                        <StudioTrans
                                            ns="allRevisions"
                                            i18nKey="about.revisionsBinItem"
                                            components={{
                                                a: (
                                                    <a
                                                        href={appUrl.forRevisionsBin(activeDatabaseName)}
                                                        target="_blank"
                                                    />
                                                ),
                                            }}
                                        />
                                    </li>
                                    <li>
                                        <StudioTrans
                                            ns="allRevisions"
                                            i18nKey="about.binCleanerItem"
                                            components={{
                                                a: (
                                                    <a
                                                        href={appUrl.forRevisionsBinCleaner(activeDatabaseName)}
                                                        target="_blank"
                                                    />
                                                ),
                                            }}
                                        />
                                    </li>
                                </ul>
                            </li>
                        </ul>
                    </p>
                </p>
                <p>{t("about.caution")}</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
