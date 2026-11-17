import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { useAppSelector } from "components/store";
import { useAppUrls } from "hooks/useAppUrls";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function RevisionsBinCleanerInfoHub() {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const revisionsBinLink = <a href={appUrl.forRevisionsBin(activeDatabaseName)} target="_blank" />;

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <StudioTrans
                        ns="revisionsBinCleaner"
                        i18nKey="about.automaticDeletion"
                        components={{ strong: <strong />, br: <br />, code: <code />, a: revisionsBinLink }}
                    />
                    <ul className="mt-1">
                        <li className="mb-1">
                            <StudioTrans
                                ns="revisionsBinCleaner"
                                i18nKey="about.minimumAge"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="revisionsBinCleaner"
                                i18nKey="about.cleanerFrequency"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                    </ul>
                </div>
                <div>
                    <StudioTrans
                        ns="revisionsBinCleaner"
                        i18nKey="about.manualDeletion"
                        components={{ strong: <strong />, br: <br />, a: revisionsBinLink }}
                    />
                </div>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
