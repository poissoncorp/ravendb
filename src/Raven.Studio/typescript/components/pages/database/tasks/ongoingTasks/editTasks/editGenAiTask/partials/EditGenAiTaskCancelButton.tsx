import Button from "react-bootstrap/Button";
import useEditGenAiCancel from "../hooks/useEditGenAiCancel";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function EditGenAiTaskCancelButton() {
    const t = useStudioTranslation("editGenAiTask");
    const commonT = useStudioTranslation("common");
    const cancel = useEditGenAiCancel();

    return (
        <Button variant="outline-secondary rounded-pill" onClick={cancel} title={t("footer.cancelTitle")}>
            {commonT("cancel")}
        </Button>
    );
}
