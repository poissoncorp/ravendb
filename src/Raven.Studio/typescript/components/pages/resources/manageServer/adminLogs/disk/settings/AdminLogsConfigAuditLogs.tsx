import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import { adminLogsSelectors } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppSelector } from "components/store";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AdminLogsConfigAuditLogs({ targetId }: { targetId: string }) {
    const t = useStudioTranslation("adminLogs");
    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.AuditLogs;

    return (
        <Accordion.Item eventKey={targetId} className="p-1 rounded-3">
            <Accordion.Header>{t("diskSettings.auditLogs.title")}</Accordion.Header>
            <Accordion.Body>
                <h5 className="text-center text-muted text-uppercase">
                    {t("diskSettings.readOnly")}
                    <PopoverWithHoverWrapper message={t("diskSettings.readOnlyInfo")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </h5>
                <Table>
                    <tbody>
                        <tr>
                            <td>{t("diskSettings.table.path")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.Path} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.level")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.Level} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.archiveAboveSize")}</td>
                            <td>
                                <AdminLogsConfigTableValue
                                    value={`${config.ArchiveAboveSizeInMb?.toLocaleString()} MB`}
                                />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.maxArchivedDays")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveDays} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.maxArchivedFiles")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveFiles} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.archiveFileCompression")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.EnableArchiveFileCompression} />
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </Accordion.Body>
        </Accordion.Item>
    );
}
