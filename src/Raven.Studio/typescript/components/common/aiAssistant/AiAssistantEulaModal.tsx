import Modal from "components/common/Modal";
import { Checkbox } from "../Checkbox";
import useBoolean from "components/hooks/useBoolean";
import { useAppDispatch } from "components/store";
import Button from "react-bootstrap/Button";
import { aiAssistantActions } from "../shell/aiAssistantSlice";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
const aiAssistantImg = require("Content/img/ai-assistant-terms-of-use.webp");

interface AiAssistantEulaModalProps {
    close: () => void;
    onConsentGiven?: () => void;
}

export function AiAssistantEulaModal({ close, onConsentGiven }: AiAssistantEulaModalProps) {
    const t = useStudioTranslation("aiAssistant");
    const tCommon = useStudioTranslation("common");
    const dispatch = useAppDispatch();

    const { value: isAccepted, toggle: toggleAccepted } = useBoolean(false);

    const asyncHandleConsent = useAsyncCallback(async () => {
        const result = await dispatch(aiAssistantActions.giveConsent()).unwrap();
        if (result === "Success") {
            onConsentGiven?.();
            close();
        }
    });

    return (
        <Modal show onHide={close} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={close} className="pb-0">
                <h3>{t("eula.title")}</h3>
            </Modal.Header>
            <div className="px-4 rounded overflow-hidden">
                <img src={aiAssistantImg} className="w-100 rounded-2" />
            </div>
            <Modal.Body className="py-1 vstack gap-2">
                <div className="mt-2">{t("eula.description")}</div>
                <Checkbox selected={isAccepted} toggleSelection={toggleAccepted} color="primary">
                    <StudioTrans
                        ns="aiAssistant"
                        i18nKey="eula.accept"
                        components={{
                            a: <a href="https://ravendb.net/legal/ravendb/ai-assistant-terms-of-use" target="_blank" />,
                        }}
                    />
                </Checkbox>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={close} className="link-muted rounded-pill">
                    {tCommon("cancel")}
                </Button>
                <ButtonWithSpinner
                    variant="primary"
                    onClick={asyncHandleConsent.execute}
                    className="rounded-pill"
                    disabled={!isAccepted}
                    isSpinning={asyncHandleConsent.loading}
                >
                    {t("eula.agree")}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
