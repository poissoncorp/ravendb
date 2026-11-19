import React, { HTMLAttributes, ReactNode, useCallback } from "react";

import classNames from "classnames";
import { NodeInfo } from "components/models/databases";
import app from "durandal/app";
import showDataDialog from "viewmodels/common/showDataDialog";
import Badge from "react-bootstrap/Badge";
import genUtils from "common/generalUtils";
import assertUnreachable from "components/utils/assertUnreachable";

import "./DatabaseGroup.scss";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseGroupProps extends HTMLAttributes<HTMLDivElement> {
    children?: ReactNode | ReactNode[];
    className?: string;
}

export function DatabaseGroup(props: DatabaseGroupProps) {
    const { children, className } = props;
    return <div className={classNames("dbgroup", className)}>{children}</div>;
}

export function DatabaseGroupList(props: { children?: ReactNode | ReactNode[] }) {
    const { children } = props;
    return <div className="dbgroup-list">{children}</div>;
}

interface DatabaseGroupItemProps extends HTMLAttributes<HTMLDivElement> {
    children?: ReactNode | ReactNode[];
    className?: string;
}

export function DatabaseGroupItem(props: DatabaseGroupItemProps) {
    const { children, className } = props;
    return <div className={classNames("dbgroup-item", className)}>{children}</div>;
}

interface DatabaseGroupNodeProps extends HTMLAttributes<HTMLDivElement> {
    children?: ReactNode | ReactNode[];
    icon?: IconName;
    color?: ThemeColor;
}

export function DatabaseGroupNode(props: DatabaseGroupNodeProps) {
    const { children, icon, color } = props;
    const nodeIcon = icon ?? "node";
    const nodeColor = color ?? "node";
    return (
        <div className="dbgroup-node">
            <Icon icon={nodeIcon} color={nodeColor} /> {children ? <strong>{children}</strong> : null}
        </div>
    );
}

interface DatabaseGroupTypeProps extends HTMLAttributes<HTMLDivElement> {
    node?: NodeInfo;
    children?: ReactNode | ReactNode[];
}

export function DatabaseGroupType(props: DatabaseGroupTypeProps) {
    const { node, children } = props;
    const t = useStudioTranslation("commonComponents");

    function nodeBadgeColor(node: NodeInfo) {
        switch (node.lastStatus) {
            case "Ok":
                return "success";
            case "NotResponding":
                return "danger";
            default:
                return "warning";
        }
    }

    function cssIcon(node: NodeInfo): IconName {
        const type = node.type;

        switch (type) {
            case "Member":
                return "dbgroup-member";
            case "Promotable":
                return "dbgroup-promotable";
            case "Rehab":
                return "dbgroup-rehab";
            default:
                assertUnreachable(type);
        }
    }

    function nodeBadgeText(node: NodeInfo) {
        switch (node.lastStatus) {
            case "Ok":
                return t("databaseGroup.status.active");
            case "NotResponding":
                return t("databaseGroup.status.error");
            default:
                return t("databaseGroup.status.catchingUp");
        }
    }

    return (
        <div className="dbgroup-type">
            <div title={node.type} className="mb-1">
                <Icon icon={cssIcon(node)} /> {node.type}
            </div>
            <div>
                <Badge bg={nodeBadgeColor(node)}>
                    {nodeBadgeText(node)}
                    {node.responsibleNode && (
                        <span className="ms-1" title={t("databaseGroup.responsibleNodeTitle")}>
                            <Icon icon="node" margin="m-0" />
                            <strong className="text-reset"> {node.responsibleNode}</strong>
                        </span>
                    )}
                </Badge>
            </div>
            {children}
        </div>
    );
}

export function DatabaseGroupActions(props: { children?: ReactNode | ReactNode[] }) {
    const { children } = props;
    return <div className="dbgroup-actions">{children}</div>;
}

interface DatabaseGroupErrorProps extends HTMLAttributes<HTMLDivElement> {
    node: NodeInfo;
}

export function DatabaseGroupError(props: DatabaseGroupErrorProps) {
    const { node } = props;
    const t = useStudioTranslation("commonComponents");
    const lastErrorShort = node.lastError ? genUtils.trimMessage(node.lastError) : null;

    const showErrorsDetails = useCallback(() => {
        app.showBootstrapDialog(
            new showDataDialog(t("databaseGroup.errorDialogTitle", { nodeTag: node.tag }), node.lastError, "plain")
        );
    }, [node, t]);

    return lastErrorShort ? (
        <div className="dbgroup-error position-relative">
            <div className="text-danger">
                <Icon icon="warning" /> {t("databaseGroup.error")}
            </div>

            <small className="d-flex flex-column">
                {lastErrorShort}
                <a
                    className="link stretched-link ms-2"
                    title={t("databaseGroup.errorDetailsTitle")}
                    onClick={showErrorsDetails}
                >
                    {t("databaseGroup.errorDetails")} <Icon icon="info" margin="m-0" />
                </a>
            </small>
            <div></div>
        </div>
    ) : null;
}
