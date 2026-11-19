import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAceEditorContext } from "../AceEditorContext";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AceEditorFullScreenAction() {
    const { aceRef } = useAceEditorContext();
    const t = useStudioTranslation("aceEditor");

    return (
        <Button
            variant="link"
            onClick={() => {
                aceRef?.current.editor.container.requestFullscreen();
            }}
            className="p-0 text-reset"
            size="sm"
            title={t("actions.fullScreen")}
        >
            <Icon icon="fullscreen" margin="m-0" />
        </Button>
    );
}
