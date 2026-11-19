import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface AceEditorDeleteActionProps {
    onDelete: () => void;
}

export default function AceEditorDeleteAction({ onDelete }: AceEditorDeleteActionProps) {
    const tCommon = useStudioTranslation("common");

    return (
        <Button variant="link" onClick={onDelete} className="p-0 text-reset" size="sm" title={tCommon("delete")}>
            <Icon icon="trash" margin="m-0" />
        </Button>
    );
}
