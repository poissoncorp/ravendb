import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ReactNode } from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface SetupWizardClickableCardProps {
    icon: IconName;
    addon?: IconName;
    title: string;
    description: string;
    isSelected: boolean;
    popoverMessage?: string | ReactNode;
    onClick: () => void;
    className?: string;
    isDisabled?: boolean;
}

export default function SetupWizardClickableCard({
    className,
    description,
    popoverMessage,
    addon,
    icon,
    onClick,
    title,
    isDisabled,
    isSelected,
}: SetupWizardClickableCardProps) {
    const t = useStudioTranslation("setupWizard");

    return (
        <div
            className={classNames("wizard-card border rounded p-4", className, {
                "bg-faded-primary border-primary active": isSelected,
                "border-secondary": !isSelected,
                "item-disabled pe-none": isDisabled,
            })}
            onClick={onClick}
        >
            <div className="text-emphasis hstack gap-4">
                <div>
                    <Icon icon={icon} addon={addon} margin="m-0" style={{ fontSize: 24 }} />
                </div>
                <div className="flex-grow">
                    <h4 className="mb-0 text-emphasis">{title}</h4>
                    <span className="text-muted">{description}</span>
                </div>
                {popoverMessage && (
                    <PopoverWithHoverWrapper message={popoverMessage}>
                        <div style={{ minWidth: "120px" }} className="md-label text-right mb-0">
                            <Icon icon="info" />
                            {t("clickableCard.whenToUse")}
                        </div>
                    </PopoverWithHoverWrapper>
                )}
            </div>
        </div>
    );
}
