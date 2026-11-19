import useBoolean from "hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

import { Icon } from "components/common/Icon";
import { Checkbox, Switch } from "components/common/Checkbox";
import React from "react";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { FormGroup, FormLabel } from "components/common/Form";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface RequestSupportModalProps {
    visible: boolean;
    toggle: () => void;
    supportId: string;
    licenseId: string;
}

export function RequestSupportModal(props: RequestSupportModalProps) {
    const t = useStudioTranslation("aboutPage");
    const { visible, toggle, supportId, licenseId } = props;

    const { value: includeDebugPackage, toggle: toggleIncludeDebugPackage } = useBoolean(false);
    const { value: includeAllDatabases, toggle: toggleIncludeAllDatabases } = useBoolean(true);

    return (
        <Modal show={visible} onHide={toggle} size="lg" contentClassName="modal-border bulge-primary">
            <Modal.Header className="vstack gap-4 " onCloseClick={toggle}>
                <div className="text-center">
                    <Icon icon="support" color="primary" className="fs-1" margin="m-0" />
                </div>

                <div className="text-center lead">{t("requestSupport.title")}</div>
            </Modal.Header>
            <Modal.Body className="vstack">
                <Form className="vstack gap-2">
                    <FormGroup>
                        <FormLabel htmlFor="contactEmail">{t("requestSupport.contactEmail")}</FormLabel>
                        <Form.Control
                            type="email"
                            name="contactEmail"
                            value="defaultEmailAssignedToLicense@client.com"
                            placeholder={t("requestSupport.emailPlaceholder")}
                        />
                    </FormGroup>
                    <Row>
                        <Col>
                            <FormGroup>
                                <FormLabel htmlFor="supportId">{t("requestSupport.supportId")}</FormLabel>
                                <Form.Control type="number" name="supportId" value={supportId} disabled />
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <FormLabel htmlFor="LicenseId">{t("requestSupport.licenseId")}</FormLabel>
                                <Form.Control type="text" name="supportId" value={licenseId} disabled />
                            </FormGroup>
                        </Col>
                    </Row>
                    <FormGroup>
                        <FormLabel htmlFor="messageText">
                            <StudioTrans
                                ns="aboutPage"
                                i18nKey="requestSupport.message"
                                components={{ span: <span className="text-muted" /> }}
                            />
                        </FormLabel>
                        <Form.Control as="textarea" name="text" id="messageText" rows={10} />
                    </FormGroup>
                    <div className="well p-3 rounded-2">
                        <Checkbox size="lg" selected={includeDebugPackage} toggleSelection={toggleIncludeDebugPackage}>
                            {t("requestSupport.includeDebugPackage")}
                        </Checkbox>
                        <Collapse in={includeDebugPackage}>
                            <div className="py-2">
                                <Switch selected={includeAllDatabases} toggleSelection={toggleIncludeAllDatabases}>
                                    {t("requestSupport.includeAllDatabases")}
                                </Switch>
                                <Collapse in={!includeAllDatabases}>
                                    <div className="vstack">
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database1
                                        </Checkbox>
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database2
                                        </Checkbox>
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database3
                                        </Checkbox>
                                    </div>
                                </Collapse>
                                <div className="d-flex gap-4">
                                    <Checkbox selected={null} toggleSelection={null}>
                                        {t("requestSupport.server")}
                                    </Checkbox>
                                    <Checkbox selected={null} toggleSelection={null}>
                                        {t("requestSupport.databases")}
                                    </Checkbox>
                                    <Checkbox selected={null} toggleSelection={null}>
                                        {t("requestSupport.logs")}
                                    </Checkbox>
                                </div>
                            </div>
                        </Collapse>
                    </div>
                </Form>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary" onClick={toggle} className="rounded-pill px-3">
                    {t("requestSupport.close")}
                </Button>
                <Button variant="primary" className="rounded-pill px-3">
                    <Icon icon="support" />
                    {t("requestSupport.submit")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
