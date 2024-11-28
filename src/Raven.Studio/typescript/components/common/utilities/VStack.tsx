import React, { PropsWithChildren } from "react";
import classNames from "classnames";

interface GapConfig {
    [breakpoint: string]: 1 | 2 | 3 | 4 | 5 | 6 | 7;
}

export interface VStackProps {
    className?: string;
    gap?: 1 | 2 | 3 | 4 | 5 | 6 | 7 | GapConfig;
}

export function VStack({ className, children, gap }: PropsWithChildren<VStackProps>) {
    const gapClasses =
        typeof gap === "object"
            ? Object.entries(gap).map(([bp, val]) => `gap-${bp}-${val}`)
            : gap
              ? [`gap-${gap}`]
              : [];

    return <div className={classNames("vstack", className, ...gapClasses)}>{children}</div>;
}
