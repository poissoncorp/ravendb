import Button from "react-bootstrap/Button";
import useUniqueId from "components/hooks/useUniqueId";
import { DatabaseSharedInfo, NodeInfo } from "components/models/databases";
import appUrl from "common/appUrl";
import {
    DatabaseGroupActions,
    DatabaseGroupError,
    DatabaseGroupItem,
    DatabaseGroupNode,
    DatabaseGroupType,
} from "components/common/DatabaseGroup";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import useConfirm from "components/common/ConfirmDialog";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Dropdown from "react-bootstrap/Dropdown";
import { useSortable } from "@dnd-kit/sortable";
import { useSortableStyles } from "hooks/useSortableStyles";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface OrchestratorInfoComponentProps {
    node: NodeInfo;
    deleteFromGroup: (nodeTag: string) => void;
    canDelete: boolean;
}

interface PromoteButtonProps {
    databaseName: string;
    nodeTag: string;
}

function PromoteButton({ databaseName, nodeTag }: PromoteButtonProps) {
    const t = useStudioTranslation("manageDatabaseGroup");
    const { databasesService } = useServices();
    const asyncPromoteImmediately = useAsyncCallback(() => databasesService.promoteDatabaseNode(databaseName, nodeTag));

    const confirm = useConfirm();

    const promote = async () => {
        const isConfirmed = await confirm({
            title: t("node.promoteConfirmTitle", { nodeTag }),
            icon: "promote",
            actionColor: "primary",
            confirmText: t("node.promote"),
        });

        if (isConfirmed) {
            await asyncPromoteImmediately.execute();
        }
    };

    return (
        <ButtonWithSpinner
            variant="secondary"
            className="rounded-pill justify-content-center"
            title={t("node.promoteTitle")}
            icon="promote"
            size="sm"
            onClick={promote}
            isSpinning={asyncPromoteImmediately.status === "loading"}
        >
            {t("node.promote")}
        </ButtonWithSpinner>
    );
}

export function OrchestratorInfoComponent(props: OrchestratorInfoComponentProps) {
    const t = useStudioTranslation("manageDatabaseGroup");
    const { node, deleteFromGroup, canDelete } = props;

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                <Button
                    size="xs"
                    variant="outline-danger"
                    disabled={!canDelete}
                    className="rounded-pill"
                    onClick={() => deleteFromGroup(node.tag)}
                >
                    <Icon icon="cancel" /> {t("orchestrators.remove")}
                </Button>
            </DatabaseGroupActions>
            <DatabaseGroupError node={node} />
        </DatabaseGroupItem>
    );
}

interface NodeInfoComponentProps {
    node: NodeInfo;
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => void;
    db: DatabaseSharedInfo;
}

export function NodeInfoComponent(props: NodeInfoComponentProps) {
    const { node, db, deleteFromGroup } = props;

    const t = useStudioTranslation("manageDatabaseGroup");
    const deleteLockId = useUniqueId("delete-lock");
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const canPromote = isOperatorOrAbove && node.type === "Promotable";
    const canDelete = db.lockMode === "Unlock";

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>

            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                {canPromote && <PromoteButton databaseName={db.name} nodeTag={node.tag} />}
                {canDelete ? (
                    <Dropdown key="can-delete">
                        <Dropdown.Toggle variant="outline-danger" size="xs" className="rounded-pill">
                            <Icon icon="disconnected" />
                            {t("node.deleteFromGroup")}
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item
                                onClick={() => deleteFromGroup(node.tag, false)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="disconnected" />
                                <div className="d-flex flex-column">
                                    <span className="lh-1">{t("node.softDelete")}</span>
                                    <small className="text-muted">{t("node.softDeleteDescription")}</small>
                                </div>
                            </Dropdown.Item>
                            <Dropdown.Item
                                onClick={() => deleteFromGroup(node.tag, true)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="trash" color="danger" />
                                <div className="d-flex flex-column">
                                    <span className="text-danger lh-1">{t("node.hardDelete")}</span>
                                    <small className="text-muted">{t("node.hardDeleteDescription")}</small>
                                </div>
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
                ) : (
                    <OverlayTrigger
                        key="cannot-delete"
                        overlay={<Tooltip id={deleteLockId}>{t("node.deleteLocked")}</Tooltip>}
                    >
                        <Dropdown id={deleteLockId}>
                            <Dropdown.Toggle variant="danger" disabled size="xs" className="rounded-pill">
                                {db.lockMode === "PreventDeletesError" && <Icon icon="trash" addon="exclamation" />}
                                {db.lockMode === "PreventDeletesIgnore" && <Icon icon="trash" addon="cancel" />}
                                {t("node.deleteFromGroup")}
                            </Dropdown.Toggle>
                        </Dropdown>
                    </OverlayTrigger>
                )}
            </DatabaseGroupActions>
            <DatabaseGroupError node={node} />
        </DatabaseGroupItem>
    );
}

interface ShardInfoComponentProps {
    node: NodeInfo;
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => void;
    db: DatabaseSharedInfo;
}

export function ShardInfoComponent(props: ShardInfoComponentProps) {
    const { node, deleteFromGroup, db } = props;

    const t = useStudioTranslation("manageDatabaseGroup");
    const deleteLockId = useUniqueId("delete-lock");
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const canDelete = db.lockMode === "Unlock";

    const documentsUrl = appUrl.forDocuments(null, db.name);
    const debugUrl = appUrl.toExternalUrl(node.nodeUrl, documentsUrl);

    const canPromote = isOperatorOrAbove && node.type === "Promotable";

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                {canPromote && <PromoteButton databaseName={db.name} nodeTag={node.tag} />}
                <Dropdown key="advanced">
                    <Dropdown.Toggle size="xs" variant="outline-secondary" className="rounded-pill">
                        <Icon icon="debug-advanced" />
                        {t("node.advanced")}
                    </Dropdown.Toggle>
                    <Dropdown.Menu>
                        <Dropdown.Item href={debugUrl} target="_blank">
                            {t("node.debugShard")}
                        </Dropdown.Item>
                    </Dropdown.Menu>
                </Dropdown>
                {canDelete ? (
                    <Dropdown key="can-delete">
                        <Dropdown.Toggle variant="outline-danger" size="xs" className="rounded-pill">
                            <Icon icon="disconnected" />
                            {t("node.deleteFromGroup")}
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item
                                onClick={() => deleteFromGroup(node.tag, false)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="trash" />
                                <div className="d-flex flex-column">
                                    <span className="lh-1">{t("node.softDelete")}</span>
                                    <small className="text-muted">{t("node.softDeleteDescription")}</small>
                                </div>
                            </Dropdown.Item>
                            <Dropdown.Item
                                onClick={() => deleteFromGroup(node.tag, true)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="alerts" color="danger" />
                                <div className="d-flex flex-column">
                                    <span className="text-danger lh-1">{t("node.hardDelete")}</span>
                                    <small className="text-muted">{t("node.hardDeleteDescription")}</small>
                                </div>
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
                ) : (
                    <OverlayTrigger
                        key="cannot-delete"
                        overlay={<Tooltip id={deleteLockId}>{t("node.deleteLocked")}</Tooltip>}
                    >
                        <Dropdown id={deleteLockId}>
                            <Dropdown.Toggle variant="outline-danger" disabled size="xs" className="rounded-pill">
                                {db.lockMode === "PreventDeletesError" && <Icon icon="trash" addon="exclamation" />}
                                {db.lockMode === "PreventDeletesIgnore" && <Icon icon="trash" addon="cancel" />}
                                {t("node.deleteFromGroup")}
                            </Dropdown.Toggle>
                        </Dropdown>
                    </OverlayTrigger>
                )}
            </DatabaseGroupActions>

            <DatabaseGroupError node={node} />
        </DatabaseGroupItem>
    );
}

interface NodeInfoReorderComponentProps {
    node: NodeInfo;
}

export function NodeInfoReorderComponent({ node }: NodeInfoReorderComponentProps) {
    const sortable = useSortable({
        id: node.tag,
    });
    const style = useSortableStyles(sortable);

    const { attributes, listeners, setNodeRef } = sortable;

    return (
        <div ref={setNodeRef} style={style} {...attributes} {...listeners}>
            <NodeInfoReorderPreview node={node} />
        </div>
    );
}

export function NodeInfoReorderPreview({ node }: { node: NodeInfo }) {
    return (
        <DatabaseGroupItem className="item-reorder">
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
        </DatabaseGroupItem>
    );
}
