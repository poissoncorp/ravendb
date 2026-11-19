import { ReactNode } from "react";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import useBoolean from "components/hooks/useBoolean";
import alert from "common/notifications/models/alert";
import performanceHint from "common/notifications/models/performanceHint";
import notificationCenterSettings from "common/notifications/notificationCenterSettings";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsActions } from "./notificationDetailsProvider";

interface DetailsFooterProps {
    start?: ReactNode;
    children: ReactNode;
}

export function DetailsFooter({ start, children }: DetailsFooterProps) {
    return (
        <Modal.Footer className="justify-content-between">
            <div>{start}</div>
            <div className="hstack gap-2">{children}</div>
        </Modal.Footer>
    );
}

export function CloseButton({ onClick }: { onClick: () => void }) {
    const t = useStudioTranslation("notificationCenter");

    return (
        <Button variant="link" className="link-muted" onClick={onClick}>
            {t("footer.close")}
        </Button>
    );
}

export function DismissButton({ onClick }: { onClick: () => void }) {
    const t = useStudioTranslation("notificationCenter");

    return (
        <Button variant="primary" className="rounded-pill" onClick={onClick} title={t("footer.dismissTitle")}>
            <Icon icon="dismiss-all" />
            {t("footer.dismiss")}
        </Button>
    );
}

export function CloseDetailsFooter({ actions }: { actions: NotificationDetailsActions }) {
    return (
        <DetailsFooter>
            <CloseButton onClick={actions.close} />
        </DetailsFooter>
    );
}

export function AlertDetailsFooter({ alert, actions }: { alert: alert; actions: NotificationDetailsActions }) {
    return (
        <DetailsFooter start={alert.canBePostponed() && <PostponeDropdown actions={actions} />}>
            <CloseButton onClick={actions.close} />
            {alert.canBeDismissed() && <DismissButton onClick={actions.dismiss} />}
        </DetailsFooter>
    );
}

export function PerformanceHintDetailsFooter({
    hint,
    actions,
}: {
    hint: performanceHint;
    actions: NotificationDetailsActions;
}) {
    const t = useStudioTranslation("notificationCenter");
    const dontShowAgain = useKnockoutValue(hint.dontShowAgain);
    const canBeDismissed = hint.canBeDismissed();

    return (
        <DetailsFooter
            start={
                canBeDismissed && (
                    <Checkbox selected={dontShowAgain} toggleSelection={(e) => hint.dontShowAgain(e.target.checked)}>
                        {t("footer.dontShowAgain")}
                    </Checkbox>
                )
            }
        >
            <CloseButton onClick={actions.close} />
            {canBeDismissed && <DismissButton onClick={actions.dismiss} />}
        </DetailsFooter>
    );
}

function PostponeDropdown({ actions }: { actions: NotificationDetailsActions }) {
    const t = useStudioTranslation("notificationCenter");
    const { value: isPostponing, setTrue: startPostponing, setFalse: stopPostponing } = useBoolean(false);

    const postpone = (timeInSeconds: number) => {
        startPostponing();
        actions.postpone(timeInSeconds).catch(stopPostponing);
    };

    return (
        <Dropdown drop="up">
            <Dropdown.Toggle as={CustomDropdownToggle} disabled={isPostponing}>
                {isPostponing ? <Spinner size="sm" className="me-1" /> : <Icon icon="clock" />}
                {t("footer.postpone")}
            </Dropdown.Toggle>
            <Dropdown.Menu renderOnMount popperConfig={{ strategy: "fixed" }}>
                {notificationCenterSettings.postponeOptions.map((option) => (
                    <Dropdown.Item key={option.value} onClick={() => postpone(option.value)}>
                        {option.label}
                    </Dropdown.Item>
                ))}
            </Dropdown.Menu>
        </Dropdown>
    );
}
