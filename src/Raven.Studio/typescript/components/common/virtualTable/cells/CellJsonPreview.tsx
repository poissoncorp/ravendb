import { Getter } from "@tanstack/react-table";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import CloseButton from "react-bootstrap/CloseButton";
import document from "models/database/documents/document";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface CellJsonPreviewProps {
    json: object;
}

export default function CellJsonPreview({ json }: CellJsonPreviewProps) {
    const t = useStudioTranslation("virtualTable");
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    const jsonText = JSON.stringify(json, null, 4);

    return (
        <>
            <Button type="button" title={t("cells.showPreview")} variant="link" onClick={toggleIsOpen}>
                <Icon icon="preview" margin="m-0" />
            </Button>
            <Modal onHide={toggleIsOpen} show={isOpen} size="lg">
                <Modal.Body className="pb-3">
                    <div className="d-flex justify-content-between">
                        <div>
                            <Icon icon="json" />
                            <span>{t("cells.jsonPreview")}</span>
                        </div>
                        <CloseButton onClick={toggleIsOpen} />
                    </div>
                    <pre style={{ maxHeight: "400px" }} className="overflow-auto m-0 mt-3">
                        <Code language="json" code={jsonText} />
                    </pre>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" type="button" onClick={toggleIsOpen}>
                        <Icon icon="close" />
                        {t("cells.close")}
                    </Button>
                </Modal.Footer>
            </Modal>
        </>
    );
}

export function CellJsonPreviewWrapper({ getValue }: { getValue: Getter<document> }) {
    return <CellJsonPreview json={getValue()} />;
}
