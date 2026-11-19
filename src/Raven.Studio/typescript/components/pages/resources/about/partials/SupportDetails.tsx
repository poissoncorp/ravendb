import React, { ReactNode } from "react";
import { RichPanelHeader } from "components/common/RichPanel";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import IconName from "../../../../../../typings/server/icons";
import { aboutPageUrls, ConnectivityStatus } from "components/pages/resources/about/partials/common";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import licenseModel from "models/auth/licenseModel";
import { AsyncState } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

const supportImg = require("Content/img/pages/about/support.svg");
const supportCharacterImg = require("Content/img/pages/about/supportCharacter.svg");

interface SupportDetailsProps {
    asyncCheckLicenseServerConnectivity: AsyncState<ConnectivityStatus>;
}

export function SupportDetails(props: SupportDetailsProps) {
    const t = useStudioTranslation("aboutPage");
    const { asyncCheckLicenseServerConnectivity } = props;

    const license = useAppSelector(licenseSelectors.status);
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const support = useAppSelector(licenseSelectors.support);
    const supportType = licenseModel.supportLabelProvider(license, support);
    const isPaidSupport = ["Professional", "Production", "Partial"].includes(supportType);

    const upgradeLink = isCloud ? aboutPageUrls.upgradeSupport.cloud : aboutPageUrls.upgradeSupport.onPremise;

    if (asyncCheckLicenseServerConnectivity.status === "loading") {
        return (
            <LazyLoad active>
                <div>{t("loadingPlaceholder")}</div>
            </LazyLoad>
        );
    }

    if (asyncCheckLicenseServerConnectivity.status === "error") {
        return (
            <div className="m-3">
                <LoadError />
            </div>
        );
    }

    if (!asyncCheckLicenseServerConnectivity.result.connected) {
        return (
            <div className="m-3">
                <LoadError error={t("supportDetails.connectionError")} />
            </div>
        );
    }

    const getTypeHeader = () => {
        if (supportType === "Community") {
            return t("supportDetails.typeHeader", { context: "community" });
        }
        if (isCloud && supportType === "Production") {
            return t("supportDetails.typeHeader", { context: "cloud" });
        }
        if (support.SupportType === "Extended") {
            return t("supportDetails.typeHeader", { context: "extended" });
        }
        if (supportType === "Professional") {
            return t("supportDetails.typeHeader", { context: "professional" });
        }
        if (supportType === "Production") {
            return t("supportDetails.typeHeader", { context: "production" });
        }

        return t("supportDetails.typeHeader", { context: "partial" });
    };

    return (
        <React.Fragment key="support-details">
            <div className="bg-faded-info hstack justify-content-center">
                <img
                    alt={t("supportDetails.supportCharacterAlt")}
                    src={supportCharacterImg}
                    className="support-character-img mt-4"
                />
            </div>
            {supportType === "Community" && (
                <>
                    <RichPanelHeader className="text-center p-4">
                        {t("supportDetails.youAreUsing")}
                        <h2 className="text-info">{getTypeHeader()}</h2>
                        <p>{t("supportDetails.communityForum")}</p>
                        <Button
                            variant="outline-secondary"
                            href={aboutPageUrls.joinCommunity}
                            className="rounded-pill align-self-center px-3"
                            target="_blank"
                        >
                            <Icon icon="group" /> {t("supportDetails.joinCommunity")}{" "}
                            <Icon icon="newtab" margin="ms-2" />
                        </Button>
                    </RichPanelHeader>
                    <div className="text-center p-4 vstack align-items-center">
                        <h2 className="hstack justify-content-center">
                            <img alt={t("supportDetails.supportAlt")} src={supportImg} width={70} className="me-4" />
                            {t("supportDetails.elevateExperience")}
                        </h2>
                        <div>
                            <p className="max-paragraph-width">{t("supportDetails.supportCoverage")}</p>
                            <p>{t("supportDetails.whatYouGet")}</p>
                        </div>
                        <Row className="support-advantages">
                            <SupportAdvantage icon="phone">{t("supportDetails.phoneAndEmail")}</SupportAdvantage>
                            {/* <SupportAdvantage icon="notifications"> TODO we hide this for now*/}
                            {/*    Request support directly from RavenDB Studio*/}
                            {/*</SupportAdvantage>*/}
                            <SupportAdvantage icon="user">{t("supportDetails.coreDevelopersAccess")}</SupportAdvantage>
                            <SupportAdvantage icon="clock">
                                {t("supportDetails.upToTwoHourSla")}
                                <br />
                                {t("supportDetails.availability")}
                            </SupportAdvantage>
                        </Row>
                        <Button
                            variant="success"
                            className="px-4 rounded-pill mt-4"
                            size="lg"
                            href={upgradeLink}
                            target="_blank"
                        >
                            <Icon icon="upgrade-arrow" /> <strong>{t("supportDetails.upgradeYourSupport")}</strong>
                        </Button>
                    </div>
                </>
            )}
            {supportType !== "Community" && (
                <>
                    <RichPanelHeader className="px-4 py-4 vstack gap-4 justify-content-center">
                        <Row className="g-md">
                            <Col xs={12} sm={6} className="text-end">
                                <div className="d-flex align-items-center">
                                    <div className="flex-grow">
                                        {t("supportDetails.youAreUsing")}
                                        <h2
                                            className={classNames(
                                                "text-info m-0",
                                                { "text-professional": supportType === "Professional" },
                                                { "text-enterprise": supportType === "Production" }
                                            )}
                                        >
                                            {getTypeHeader()}
                                        </h2>
                                    </div>
                                    <img
                                        alt={t("supportDetails.supportAlt")}
                                        src={supportImg}
                                        width={70}
                                        className="ms-4"
                                    />
                                </div>
                            </Col>
                            <Col xs={12} sm={6}>
                                <div className="vstack gap-4">
                                    <div className="hstack">
                                        {(supportType === "Professional" || supportType === "Production") && (
                                            <>
                                                <Icon icon="clock" className="fs-2" />

                                                <div className="small me-3">
                                                    {supportType === "Professional" && (
                                                        <>
                                                            <strong>{t("supportDetails.nextBusinessDaySla")}</strong>
                                                            <div>{t("supportDetails.availability")}</div>
                                                        </>
                                                    )}
                                                    {supportType === "Production" && (
                                                        <>
                                                            <strong>{t("supportDetails.twoHourSla")}</strong>
                                                            <div>{t("supportDetails.availability")}</div>
                                                        </>
                                                    )}
                                                </div>
                                            </>
                                        )}
                                        {isPaidSupport && (
                                            <Button
                                                className="rounded-pill"
                                                variant={isCloud ? "cloud" : "primary"}
                                                href={
                                                    isCloud
                                                        ? aboutPageUrls.cloudPortal
                                                        : aboutPageUrls.supportRequest(licenseId)
                                                }
                                                target="_blank"
                                            >
                                                <Icon icon="notifications" /> {t("supportDetails.requestSupport")}
                                            </Button>
                                        )}
                                    </div>
                                    <div>
                                        <a
                                            href={
                                                isCloud ? aboutPageUrls.cloudSupportTerms : aboutPageUrls.supportTerms
                                            }
                                            className="d-inline-flex no-decor"
                                        >
                                            <Icon icon="terms" className="fs-2" />
                                            <div className="small">{t("supportDetails.termsAndConditions")}</div>
                                        </a>
                                    </div>
                                </div>
                            </Col>
                        </Row>
                        <Row className="g-md">
                            <Col className="text-right">{t("supportDetails.communityForum")}</Col>
                            <Col>
                                <Button
                                    variant="outline-secondary"
                                    href={aboutPageUrls.joinCommunity}
                                    className="rounded-pill align-self-center px-3"
                                    target="_blank"
                                >
                                    {t("supportDetails.joinOurCommunity")} <Icon icon="newtab" margin="ms-1" />
                                </Button>
                            </Col>
                        </Row>
                    </RichPanelHeader>
                    {supportType !== "Production" && (
                        <div className="text-center p-4">
                            <div className="bg-faded-success hstack align-items-center rounded-pill p-1">
                                <div className="px-4 flex-grow-1 text-center lh-1">
                                    <strong className="fs-3">{t("supportDetails.getTwoHourSla")}</strong>
                                    <br />
                                    <small>{t("supportDetails.hereForYou")}</small>
                                </div>
                                <Button href={upgradeLink} variant="success" className="px-4 rounded-pill" size="lg">
                                    <Icon icon="upgrade-arrow" />{" "}
                                    <strong>{t("supportDetails.upgradeToProduction")}</strong>
                                </Button>
                            </div>
                        </div>
                    )}
                </>
            )}
        </React.Fragment>
    );
}

interface SupportAdvantageProps {
    icon: IconName;
    children: ReactNode | ReactNode[];
}

function SupportAdvantage(props: SupportAdvantageProps) {
    const { icon, children } = props;
    return (
        <Col>
            <Icon icon={icon} margin="m-0" className="fs-2" />
            <div className="small mt-1">
                <strong>{children}</strong>
            </div>
        </Col>
    );
}
