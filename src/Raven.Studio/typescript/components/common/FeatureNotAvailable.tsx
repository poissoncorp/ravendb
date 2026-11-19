import React, { ReactNode } from "react";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "./EmptySet";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface FeatureNotAvailableProps {
    badgeText?: string;
    children: ReactNode | ReactNode[];
}

export default function FeatureNotAvailable(props: FeatureNotAvailableProps) {
    const { badgeText, children } = props;
    const t = useStudioTranslation("commonComponents");

    return (
        <div>
            <EmptySet icon="disabled" color="warning">
                <div className="vstack gap-3">
                    <span>
                        <Badge pill bg="faded-warning">
                            {badgeText ?? t("featureNotAvailable.badge")}
                        </Badge>
                    </span>
                    {children}
                </div>
            </EmptySet>
        </div>
    );
}
