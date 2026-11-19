import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAceEditorContext } from "../AceEditorContext";
import "ace-builds/src-noconflict/ext-beautify";
import { RefObject } from "react";
import ReactAce from "react-ace";
import { useStudioTranslation } from "hooks/useStudioTranslation";

const beautify = ace.require("ace/ext/beautify").beautify;

export default function AceEditorFormatAction() {
    const { aceRef } = useAceEditorContext();
    const t = useStudioTranslation("aceEditor");

    return (
        <Button
            variant="link"
            onClick={() => handleFormat(aceRef)}
            className="p-0 text-reset"
            size="sm"
            title={t("actions.format")}
        >
            <Icon icon="indent" margin="m-0" />
        </Button>
    );
}

export function handleFormat(reactAce: RefObject<ReactAce>) {
    const session = reactAce?.current.editor.session;

    if (!session) {
        console.error("No Ace Editor session found");
        return;
    }

    try {
        const value = session.getValue();
        const parsed = JSON.parse(value);
        const formatted = JSON.stringify(parsed, null, 2);
        session.setValue(formatted);
    } catch {
        beautify(session);
    }
}
