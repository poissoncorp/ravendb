import React from "react";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import Button from "react-bootstrap/Button";
import { Icon } from "../Icon";
import IconName from "typings/server/icons";
import Modal from "components/common/Modal";

interface DeleteCustomAnalyzerConfirmProps {
    name: string;
    toggle: () => void;
    onConfirm: (name: string) => void;
    isServerWide?: boolean;
}

export default function DeleteCustomAnalyzerConfirm(props: DeleteCustomAnalyzerConfirmProps) {
    const { name, onConfirm, toggle, isServerWide } = props;
    const tCommon = useStudioTranslation("common");

    const onSubmit = () => {
        onConfirm(name);
        toggle();
    };

    const iconName: IconName = isServerWide ? "server-wide-custom-analyzers" : "custom-analyzers";

    return (
        <Modal show onHide={toggle} contentClassName="modal-border bulge-danger">
            <Modal.Header className="vstack gap-4" onCloseClick={toggle}>
                <Icon icon={iconName} color="danger" className="fs-1" margin="m-0" />
                <div className="text-center lead">
                    <StudioTrans
                        ns="customAnalyzersCommon"
                        i18nKey="deleteConfirm"
                        components={{ span: <span className="text-danger" /> }}
                        options={{ context: isServerWide ? "serverWide" : undefined }}
                    />
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 position-relative">
                <span className="d-flex align-items-center word-break bg-faded-primary py-1 px-3 w-fit-content rounded-pill mx-auto">
                    <Icon icon={iconName} />
                    {name}
                </span>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    {tCommon("cancel")}
                </Button>
                <Button variant="danger" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="trash" />
                    {tCommon("delete")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
