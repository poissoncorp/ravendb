import copyToClipboard from "common/copyToClipboard";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import Modal from "components/common/Modal";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export interface CopyDocumentsModalData {
    title: string;
    text: string;
}

interface CopyDocumentsModalProps extends CopyDocumentsModalData {
    close: () => void;
}

export default function CopyDocumentsModal({ title, text, close }: CopyDocumentsModalProps) {
    const t = useStudioTranslation("documentsList");

    return (
        <Modal show onHide={close} size="lg">
            <Modal.Header onCloseClick={close} className="pb-0">
                <div>
                    <Icon icon="copy" />
                    <span>{title}</span>
                </div>
            </Modal.Header>
            <Modal.Body className="pb-3">
                <pre style={{ maxHeight: "60vh" }} className="overflow-auto m-0 mt-3">
                    <Code language="javascript" code={text} isActionsHidden />
                </pre>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={close}>
                    <Icon icon="close" />
                    {t("copyModal.close")}
                </Button>
                <Button variant="primary" onClick={() => copyToClipboard.copy(text, t("copyModal.copied"))}>
                    <Icon icon="copy" />
                    {t("copyModal.copyToClipboard")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
