import { useState } from "react";
import Modal from "components/common/Modal";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppDispatch } from "components/store";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import CertificatesReadyModalBrowserAccess from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesReadyModalBrowserAccess";
import CertificatesReadyModalApiAccess from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesReadyModalApiAccess";
import "./CertificatesReadyModal.scss";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type AccessTab = "browser" | "api";

export default function CertificatesReadyModal() {
    const t = useStudioTranslation("certificates");
    const dispatch = useAppDispatch();
    const docsLink = useRavenLink({ hash: "RSFSL5" });

    const [accessTab, setAccessTab] = useState<AccessTab>("browser");

    const close = () => dispatch(certificatesActions.isReadyModalOpenToggled());

    return (
        <Modal
            scrollable
            show
            size="lg"
            centered
            contentClassName="modal-border bulge-primary certificates-ready-modal"
        >
            <Modal.Header onCloseClick={close} className="mb-0">
                <Icon icon="certificate" size="lg" margin="me-3" color="primary" addon="check" />
                <h3 className="mb-0">{t("ready.title")}</h3>
            </Modal.Header>
            <Modal.Body>
                <p className="mb-4">{t("ready.intro")}</p>

                <Tab.Container activeKey={accessTab} onSelect={(key: AccessTab) => setAccessTab(key)}>
                    <div className="access-tab-container mb-4">
                        <Nav className="gap-3">
                            <Nav.Item className="flex-grow mb-0">
                                <Nav.Link
                                    eventKey="browser"
                                    className="d-flex align-items-center justify-content-center w-100 text-center border rounded py-2 px-3"
                                >
                                    <Icon icon="global" />
                                    {t("ready.browserTab")}
                                </Nav.Link>
                            </Nav.Item>
                            <Nav.Item className="flex-grow mb-0">
                                <Nav.Link
                                    eventKey="api"
                                    className="d-flex align-items-center justify-content-center w-100 text-center border rounded py-2 px-3"
                                >
                                    <Icon icon="code" />
                                    {t("ready.apiTab")}
                                </Nav.Link>
                            </Nav.Item>
                        </Nav>
                    </div>

                    <Tab.Content>
                        <CertificatesReadyModalBrowserAccess />
                        <CertificatesReadyModalApiAccess />
                    </Tab.Content>
                </Tab.Container>
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between my-2">
                <a href={docsLink} target="_blank" className="btn btn-info rounded-pill">
                    {t("ready.docsLink")} <Icon icon="newtab" margin="m-0" />
                </a>
                <Button variant="link" onClick={close} className="link-muted">
                    {t("ready.close")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
