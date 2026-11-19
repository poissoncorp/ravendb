import { useState } from "react";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import genUtils from "common/generalUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function CertificatesReadyModalBrowserAccess() {
    const t = useStudioTranslation("certificates");
    const [browserTab, setBrowserTab] = useState<Browser>(genUtils.getBrowser());

    return (
        <Tab.Pane eventKey="browser">
            <NumberedList>
                <NumberedListItem stepKey={1}>
                    <h4 className="mb-1">{t("browserAccess.locateHeading")}</h4>
                    <p className="mb-0">
                        <StudioTrans
                            ns="certificates"
                            i18nKey="browserAccess.locateText"
                            components={{ code: <code /> }}
                        />
                    </p>
                </NumberedListItem>
                <NumberedListItem stepKey={2}>
                    <h4 className="mb-2">{t("browserAccess.installHeading")}</h4>
                    <Tab.Container activeKey={browserTab} onSelect={(key: Browser) => setBrowserTab(key)}>
                        <div className="segmented-tab-container">
                            <Nav className="flex-nowrap">
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="Chrome" className="text-center">
                                        <Icon icon="chrome" />
                                        Chrome
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="Firefox" className="text-center">
                                        <Icon icon="firefox" />
                                        Firefox
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="Safari" className="text-center">
                                        <Icon icon="safari" />
                                        Safari
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="Other" className="text-center">
                                        <Icon icon="global" />
                                        {t("browserAccess.otherTab")}
                                    </Nav.Link>
                                </Nav.Item>
                            </Nav>
                            <Tab.Content className="p-2 text-break">
                                <Tab.Pane eventKey="Chrome">{t("browserAccess.chromeInfo")}</Tab.Pane>
                                <Tab.Pane eventKey="Firefox">{t("browserAccess.firefoxInfo")}</Tab.Pane>
                                <Tab.Pane eventKey="Safari">{t("browserAccess.safariInfo")}</Tab.Pane>
                                <Tab.Pane eventKey="Other">{t("browserAccess.otherInfo")}</Tab.Pane>
                            </Tab.Content>
                        </div>
                    </Tab.Container>
                </NumberedListItem>
                <NumberedListItem stepKey={3}>
                    <h4 className="mb-1">{t("browserAccess.reconnectHeading")}</h4>
                    <p className="mb-1">{t("browserAccess.reconnectText")}</p>
                    <ul className="mb-0">
                        <li>{t("browserAccess.closeWindowsItem")}</li>
                        <li>{t("browserAccess.reopenItem")}</li>
                    </ul>
                </NumberedListItem>
            </NumberedList>
            <RichAlert variant="info">{t("browserAccess.incognitoHint")}</RichAlert>
        </Tab.Pane>
    );
}
