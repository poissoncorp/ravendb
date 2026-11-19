import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { useRavenLink } from "components/hooks/useRavenLink";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function CertificatesAuthDisabled() {
    const t = useStudioTranslation("certificates");
    const certificatesDocsLink = useRavenLink({ hash: "RSFSL5" });

    return (
        <Row>
            <Col sm={12} lg={7} className="mx-auto">
                <Card className="p-5 rounded">
                    <h3 className="text-warning mb-1">
                        <Icon icon="unsecure" /> {t("authDisabled.heading")}
                    </h3>
                    <p className="lead mb-0">{t("authDisabled.lead")}</p>
                    <hr />
                    <strong>{t("authDisabled.stepsIntro")}</strong>
                    <NumberedList className="mt-3 mb-4">
                        <NumberedListItem stepKey={1}>
                            <StudioTrans
                                ns="certificates"
                                i18nKey="authDisabled.locateStep"
                                components={{ code: <code /> }}
                            />
                        </NumberedListItem>
                        <NumberedListItem stepKey={2}>
                            <StudioTrans
                                ns="certificates"
                                i18nKey="authDisabled.pathStep"
                                components={{ code: <code /> }}
                            />
                        </NumberedListItem>
                        <NumberedListItem stepKey={3}>
                            <StudioTrans
                                ns="certificates"
                                i18nKey="authDisabled.httpsStep"
                                components={{ code: <code /> }}
                            />
                        </NumberedListItem>
                        <NumberedListItem stepKey={4}>{t("authDisabled.restartStep")}</NumberedListItem>
                    </NumberedList>
                    <RichAlert variant="info">
                        <StudioTrans
                            ns="certificates"
                            i18nKey="authDisabled.httpsInfo"
                            components={{ i: <i />, br: <br /> }}
                        />
                    </RichAlert>
                    <hr />
                    <span>
                        <StudioTrans
                            ns="certificates"
                            i18nKey="authDisabled.moreInfo"
                            components={{
                                a: <a href={certificatesDocsLink} target="_blank" />,
                                icon: <Icon icon="newtab" />,
                            }}
                        />
                    </span>
                </Card>
            </Col>
        </Row>
    );
}
