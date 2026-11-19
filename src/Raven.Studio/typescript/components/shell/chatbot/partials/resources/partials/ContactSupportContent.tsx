import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { useAsync } from "react-async-hook";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import licenseModel from "models/auth/licenseModel";
import classNames from "classnames";
import { useRavenLink } from "hooks/useRavenLink";
import { useServices } from "components/hooks/useServices";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import assertUnreachable from "components/utils/assertUnreachable";

export function ContactSupportContent() {
    const t = useStudioTranslation("chatbot");
    const license = useAppSelector(licenseSelectors.status);
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const support = useAppSelector(licenseSelectors.support);
    const supportType = licenseModel.supportLabelProvider(license, support);
    const isPaidSupport = ["Professional", "Production", "Partial"].includes(supportType);

    const { licenseService } = useServices();
    const asyncLicenseConnectivity = useAsync(licenseService.checkLicenseServerConnectivity, []);
    const isSupportStatusHidden =
        asyncLicenseConnectivity.status === "error" ||
        (asyncLicenseConnectivity.status === "success" && !asyncLicenseConnectivity.result.connected);

    const supportPlansUrl = useRavenLink({ hash: "2DW5F4" });
    const cloudTermsUrl = useRavenLink({ hash: "75DJRY" });
    const onPremiseTermsUrl = useRavenLink({ hash: "R1OFBF" });
    const gitHubDiscussionsUrl = useRavenLink({ hash: "ITXUEA" });
    const communityUrl = useRavenLink({ hash: "FUNILW", isDocs: false });
    const cloudRequestSupportUrl = useRavenLink({ hash: "2YGOL1" });
    const onPremiseRequestSupportUrl = "https://ravendb.net/support/supportrequest?licenseId=" + licenseId;

    const requestSupportUrl = isCloud ? cloudRequestSupportUrl : onPremiseRequestSupportUrl;
    const termsUrl = isCloud ? cloudTermsUrl : onPremiseTermsUrl;

    const getPlanLabel = () => {
        if (isCloud && supportType === "Production") {
            return t("contactSupport.cloudSupport");
        }

        switch (supportType) {
            case "Production":
                return t("contactSupport.plans.production");
            case "Professional":
                return t("contactSupport.plans.professional");
            case "Partial":
                return t("contactSupport.plans.partial");
            case "Community":
                return t("contactSupport.plans.community");
            default:
                return assertUnreachable(supportType);
        }
    };

    return (
        <>
            <ul className="action-menu__list">
                <p className="m-0">
                    {isSupportStatusHidden ? (
                        <span className="text-warning">
                            <Icon icon="warning" />
                            <small>
                                <StudioTrans
                                    ns="chatbot"
                                    i18nKey="contactSupport.licenseServerUnreachable"
                                    components={{ code: <code /> }}
                                />
                            </small>
                        </span>
                    ) : (
                        <span>
                            <StudioTrans
                                ns="chatbot"
                                i18nKey="contactSupport.currentPlan"
                                options={{ plan: getPlanLabel() }}
                                components={{
                                    strong: (
                                        <strong
                                            className={classNames(
                                                { "text-professional": supportType === "Professional" },
                                                { "text-enterprise": supportType === "Production" },
                                                { "text-community": supportType === "Community" }
                                            )}
                                        />
                                    ),
                                }}
                            />
                        </span>
                    )}
                </p>
                {isPaidSupport && !isSupportStatusHidden ? (
                    <>
                        {(supportType === "Professional" || supportType === "Production") && (
                            <div className="d-flex align-items-center gap-1">
                                <Icon icon="clock" />
                                <div className="d-flex flex-column">
                                    <span className="fw-bold lh-base">
                                        {supportType === "Professional"
                                            ? t("contactSupport.nextDaySla")
                                            : t("contactSupport.twoHourSla")}
                                    </span>
                                    <small className="lh-1">
                                        {supportType === "Professional"
                                            ? t("contactSupport.businessHours")
                                            : t("contactSupport.fullAvailability")}
                                    </small>
                                </div>
                            </div>
                        )}
                        <a href={termsUrl} target="_blank" className="d-flex align-items-center gap-1 w-fit-content">
                            <Icon icon="terms" />
                            {t("contactSupport.termsAndConditions")}
                        </a>
                        <li
                            className={classNames(
                                "mt-1 action-menu__list-item",
                                isCloud ? "action-menu__list-item--cloud" : "action-menu__list-item--primary"
                            )}
                            role="button"
                            title={t("contactSupport.requestSupport")}
                            onClick={() => window.open(requestSupportUrl, "_blank")}
                        >
                            <Icon icon="notifications" margin="m-0" />
                            {t("contactSupport.requestSupport")}
                            <FlexGrow />
                            <Icon icon="newtab" margin="m-0" />
                        </li>
                    </>
                ) : (
                    <>
                        <p className="m-0">{t("contactSupport.communityHelp")}</p>
                        <li
                            className="action-menu__list-item action-menu__list-item--primary mt-1"
                            role="button"
                            title={t("helpAndResources.goToGithubDiscussions")}
                            onClick={() => window.open(gitHubDiscussionsUrl, "_blank")}
                        >
                            <Icon icon="github" margin="m-0" />
                            {t("helpAndResources.githubDiscussions")}
                            <FlexGrow />
                            <Icon icon="newtab" margin="m-0" />
                        </li>
                    </>
                )}
            </ul>
            <FlexGrow />
            <div className="action-menu__footer">
                <small className="lh-1">
                    <Icon icon="support" />
                    <StudioTrans
                        ns="chatbot"
                        i18nKey="contactSupport.supportPlans"
                        components={{ a: <a href={supportPlansUrl} target="_blank" /> }}
                    />
                </small>
                {isPaidSupport && !isSupportStatusHidden && (
                    <small className="lh-1 mt-1">
                        <Icon icon="community" />
                        <StudioTrans
                            ns="chatbot"
                            i18nKey="helpAndResources.joinOurCommunity"
                            components={{ a: <a href={communityUrl} target="_blank" /> }}
                        />
                    </small>
                )}
            </div>
        </>
    );
}
