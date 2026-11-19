import React from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "./Icon";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface RunScriptButtonProps extends Button.BtnProps {
    isSpinning?: boolean;
}

export default function RunScriptButton(props: RunScriptButtonProps) {
    const { isSpinning, disabled, ...rest } = props;
    const t = useStudioTranslation("commonComponents");

    return (
        <div className="run-script-button">
            <Button variant="primary" size="lg" className="px-4 py-2" disabled={disabled || isSpinning} {...rest}>
                {isSpinning ? <Spinner /> : <Icon icon="play" className="fs-1 d-inline-block" margin="mb-2" />}
                <div className="kbd">
                    <kbd>{t("runScriptButton.keys.ctrl")}</kbd> <strong>+</strong>{" "}
                    <kbd>{t("runScriptButton.keys.enter")}</kbd>
                </div>
            </Button>
        </div>
    );
}
