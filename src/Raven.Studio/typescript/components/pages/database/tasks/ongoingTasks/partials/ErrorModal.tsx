import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import copyToClipboard from "common/copyToClipboard";
import React from "react";
import Modal from "components/common/Modal";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ErrorModalProps {
    toggleErrorModal: () => void;
    error: string;
}

export function ErrorModal(props: ErrorModalProps) {
    const { toggleErrorModal, error } = props;
    const t = useStudioTranslation("ongoingTasks");
    return (
        <Modal size="xl" show onHide={toggleErrorModal} contentClassName="modal-border bulge-danger">
            <Modal.Header className="hstack gap-1 mb-4" onCloseClick={toggleErrorModal}>
                <Icon icon="warning" color="danger" margin="m-0" />
                <div className="text-center lead">{t("error")}</div>
            </Modal.Header>
            <Modal.Body>
                <Code code={error} language="csharp" />
                <div className="text-end mt-3">
                    <Button
                        className="rounded-pill"
                        variant="primary"
                        onClick={() => copyToClipboard.copy(error, t("errorModal.copied"))}
                    >
                        <Icon icon="copy" /> <span>{t("copyToClipboard")}</span>
                    </Button>
                </div>
            </Modal.Body>
        </Modal>
    );
}
