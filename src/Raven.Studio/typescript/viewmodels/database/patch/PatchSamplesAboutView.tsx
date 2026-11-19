import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function PatchSamplesAboutView() {
    const t = useStudioTranslation("patch");
    const commonT = useStudioTranslation("common");
    const patchViewDocsLink = useRavenLink({ hash: "VXSYQ7" });
    const setBasedPatchOperationsDocsLink = useRavenLink({ hash: "E928D4" });

    return (
        <AboutViewFloating className="me-2">
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading={t("about.heading")}
                description={t("about.description")}
            >
                <p>
                    <StudioTrans ns="patch" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <div>
                    <StudioTrans ns="patch" i18nKey="about.scriptParts" components={{ strong: <strong /> }} />
                    <ul className="mt-1">
                        <li>
                            <StudioTrans
                                ns="patch"
                                i18nKey="about.queryPart"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="patch"
                                i18nKey="about.updatePart"
                                components={{ strong: <strong />, br: <br />, code: <code /> }}
                            />
                        </li>
                    </ul>
                </div>
                <div>
                    <StudioTrans ns="patch" i18nKey="about.basicStructure" components={{ strong: <strong /> }} />
                    <Code code={patchStructureExample} language="rql" className="mt-1 ms-2" isRunQueryHidden />
                </div>
                <div className="mt-3">
                    <StudioTrans ns="patch" i18nKey="about.testingHeading" components={{ strong: <strong /> }} />
                    <ul className="mt-1">
                        <li>
                            <StudioTrans
                                ns="patch"
                                i18nKey="about.testingPreview"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="patch"
                                i18nKey="about.testingQueryIgnored"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                    </ul>
                </div>
                <div>
                    <StudioTrans ns="patch" i18nKey="about.runningHeading" components={{ strong: <strong /> }} />
                    <ul className="mt-1">
                        <li>{t("about.runningExecutes")}</li>
                        <li>
                            <StudioTrans ns="patch" i18nKey="about.runningProgress" components={{ br: <br /> }} />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={patchViewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsPatchView")}
                </a>
                <br />
                <a href={setBasedPatchOperationsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsSetBasedPatchOperations")}
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}

const patchStructureExample = `from CollectionOrIndex
where ...
update {
    // JavaScript changes
}`;
