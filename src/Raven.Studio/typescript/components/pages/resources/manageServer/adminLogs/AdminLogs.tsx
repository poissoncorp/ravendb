import fileDownloader from "common/fileDownloader";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import { RichPanel, RichPanelDetails, RichPanelHeader } from "components/common/RichPanel";
import Select, { SelectOption } from "components/common/select/Select";
import SizeGetter from "components/common/SizeGetter";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import AdminLogsVirtualList from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsVirtualList";
import AdminLogsDiskDownloadModal from "components/pages/resources/manageServer/adminLogs/disk/AdminLogsDiskDownloadModal";
import AdminLogsDiskSettingsModal from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsDiskSettingsModal";
import AdminLogsDisplaySettingsModal from "components/pages/resources/manageServer/adminLogs/displaySettings/AdminLogsDisplaySettingsModal";
import useAdminLogsFilter from "components/pages/resources/manageServer/adminLogs/hooks/useAdminLogsFilter";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import AdminLogsViewSettingsModal from "components/pages/resources/manageServer/adminLogs/viewSettings/AdminLogsViewSettingsModal";
import { useAppDispatch, useAppSelector } from "components/store";
import { logLevelOptions } from "components/utils/common";
import moment from "moment";
import { useEffect } from "react";
import { StylesConfig } from "react-select";
import { Button, Col, Input, Row } from "reactstrap";

export default function AdminLogs() {
    const dispatch = useAppDispatch();
    const eventsCollector = useEventsCollector();
    const { manageServerService } = useServices();

    const filteredLogs = useAppSelector(adminLogsSelectors.filteredLogs);
    const isPaused = useAppSelector(adminLogsSelectors.isPaused);
    const isMonitorTail = useAppSelector(adminLogsSelectors.isMonitorTail);
    const isDiscSettingOpen = useAppSelector(adminLogsSelectors.isDiscSettingOpen);
    const isViewSettingOpen = useAppSelector(adminLogsSelectors.isViewSettingOpen);
    const isDisplaySettingsOpen = useAppSelector(adminLogsSelectors.isDisplaySettingsOpen);
    const isDownloadDiskLogsOpen = useAppSelector(adminLogsSelectors.isDownloadDiskLogsOpen);
    const isAllExpanded = useAppSelector(adminLogsSelectors.isAllExpanded);
    const configs = useAppSelector(adminLogsSelectors.configs);
    const configsLoadStatus = useAppSelector(adminLogsSelectors.configsLoadStatus);
    const filter = useAppSelector(adminLogsSelectors.filter);

    const { localFilter, handleFilterChange } = useAdminLogsFilter();

    // Fetch configs and start ws client on mount
    useEffect(() => {
        dispatch(adminLogsActions.fetchConfigs());
        dispatch(adminLogsActions.liveClientStarted());

        return () => {
            dispatch(adminLogsActions.liveClientStopped());
            dispatch(adminLogsActions.reset());
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const handlePageMinLevelChange = async ({ value }: SelectOption<Sparrow.Logging.LogLevel>) => {
        if (!configs) {
            return;
        }

        await manageServerService.saveAdminLogsConfiguration({
            AdminLogs: {
                MinLevel: value,
                Filters: configs.adminLogsConfig.AdminLogs.CurrentFilters,
                LogFilterDefaultAction: configs.adminLogsConfig.AdminLogs.CurrentLogFilterDefaultAction,
            },
        });
        await dispatch(adminLogsActions.fetchConfigs());
    };

    const exportToFile = () => {
        const fileName = "admin-log-" + moment().format("YYYY-MM-DD HH-mm") + ".json";

        fileDownloader.downloadAsJson(
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            filteredLogs.map(({ _meta: ignored, ...logWithoutMeta }) => logWithoutMeta),
            fileName
        );
    };

    return (
        <div className="content-padding vstack gap-2 h-100">
            <Row className="gap-sm-1 gap-md-0">
                <Col md={7} sm={12}>
                    <RichPanel color="secondary">
                        <RichPanelHeader className="text-white d-flex justify-content-between">
                            <span>
                                <Icon icon="client" />
                                Logs on this view
                            </span>
                            <div className="d-flex align-items-center">
                                <Icon icon="logs" addon="arrow-filled-up" />
                                Level:{" "}
                                <Select
                                    value={logLevelOptions.find(
                                        (x) => x.value === configs?.adminLogsConfig?.AdminLogs?.CurrentMinLevel
                                    )}
                                    onChange={handlePageMinLevelChange}
                                    options={logLevelOptions}
                                    isLoading={configsLoadStatus === "loading"}
                                    isDisabled={configsLoadStatus !== "success"}
                                    className="ms-1 fs-6"
                                    styles={levelSelectStyles}
                                />
                            </div>
                        </RichPanelHeader>
                        <RichPanelDetails>
                            <div className="d-flex gap-2 flex-wrap">
                                <Button
                                    type="button"
                                    color={isPaused ? "success" : "warning"}
                                    outline
                                    onClick={() => dispatch(adminLogsActions.isPausedToggled())}
                                >
                                    <Icon icon={isPaused ? "play" : "pause"} />
                                    {isPaused ? "Resume" : "Pause"}
                                </Button>
                                <Button
                                    type="button"
                                    color="danger"
                                    outline
                                    onClick={() => {
                                        eventsCollector.reportEvent("admin-logs", "clear");
                                        dispatch(adminLogsActions.logsSet([]));
                                    }}
                                >
                                    <Icon icon="cancel" />
                                    Clear
                                </Button>
                                <Button
                                    type="button"
                                    color="light"
                                    outline
                                    onClick={() => dispatch(adminLogsActions.isMonitorTailToggled())}
                                >
                                    <Icon icon={isMonitorTail ? "pause" : "check"} />
                                    Monitor (tail -f)
                                </Button>
                                <Button
                                    type="button"
                                    color="light"
                                    outline
                                    onClick={() => {
                                        eventsCollector.reportEvent("admin-logs", "export");
                                        exportToFile();
                                    }}
                                >
                                    <Icon icon="export" />
                                    Export
                                </Button>
                                <ButtonWithSpinner
                                    type="button"
                                    color="light"
                                    outline
                                    onClick={() => dispatch(adminLogsActions.isViewSettingOpenToggled())}
                                    isSpinning={configsLoadStatus === "loading"}
                                    icon="settings"
                                    disabled={configsLoadStatus !== "success"}
                                >
                                    Settings
                                </ButtonWithSpinner>
                                {isViewSettingOpen && <AdminLogsViewSettingsModal />}
                            </div>
                        </RichPanelDetails>
                    </RichPanel>
                </Col>
                <Col md={5} sm={12}>
                    <RichPanel color="secondary">
                        <RichPanelHeader className="text-white d-flex justify-content-between">
                            <span>
                                <Icon icon="drive" />
                                Logs on disk
                            </span>
                            <div className="d-flex align-items-center">
                                <Icon icon="logs" addon="arrow-filled-up" />
                                Level:{" "}
                                {configsLoadStatus === "loading" ? (
                                    <LazyLoad active>
                                        <div>?????</div>
                                    </LazyLoad>
                                ) : (
                                    configs?.adminLogsConfig?.Logs?.CurrentMinLevel
                                )}
                            </div>
                        </RichPanelHeader>
                        <RichPanelDetails>
                            <div className="d-flex gap-2 flex-wrap">
                                <Button
                                    type="button"
                                    color="light"
                                    outline
                                    onClick={() => dispatch(adminLogsActions.isDownloadDiskLogsOpenToggled())}
                                >
                                    <Icon icon="download" />
                                    Download
                                </Button>
                                {isDownloadDiskLogsOpen && <AdminLogsDiskDownloadModal />}
                                <ButtonWithSpinner
                                    type="button"
                                    color="light"
                                    outline
                                    onClick={() => dispatch(adminLogsActions.isDiscSettingOpenToggled())}
                                    icon="settings"
                                    isSpinning={configsLoadStatus === "loading"}
                                    disabled={configsLoadStatus !== "success"}
                                >
                                    Settings
                                </ButtonWithSpinner>
                                {isDiscSettingOpen && <AdminLogsDiskSettingsModal />}
                            </div>
                        </RichPanelDetails>
                    </RichPanel>
                </Col>
            </Row>
            <div className="d-flex gap-2">
                <div className="clearable-input flex-grow-1">
                    <Input
                        type="text"
                        placeholder="Search..."
                        value={localFilter}
                        onChange={(e) => handleFilterChange(e.target.value)}
                        className="pe-4"
                    />
                    {filter && (
                        <div className="clear-button">
                            <Button color="secondary" size="sm" onClick={() => handleFilterChange("")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
                <Button
                    type="button"
                    color="link"
                    size="xs"
                    onClick={() => dispatch(adminLogsActions.isAllExpandedToggled())}
                >
                    <Icon icon={isAllExpanded ? "collapse-vertical" : "expand-vertical"} />
                    {isAllExpanded ? "Collapse all" : "Expand all"}
                </Button>
                <Button
                    type="button"
                    color="link"
                    size="xs"
                    onClick={() => dispatch(adminLogsActions.isDisplaySettingsOpenToggled())}
                >
                    <Icon icon="settings" />
                    Display settings
                </Button>
                {isDisplaySettingsOpen && <AdminLogsDisplaySettingsModal />}
            </div>
            <div className="flex-grow-1">
                <SizeGetter
                    isHeighRequired
                    render={(size) => <AdminLogsVirtualList availableHeightInPx={size.height} />}
                />
            </div>
        </div>
    );
}

const levelSelectStyles: StylesConfig = {
    control: (base) => ({
        ...base,
        minHeight: 22,
        height: 22,
        minWidth: 60,
    }),
    placeholder: (base) => ({
        ...base,
        height: 22,
    }),
    singleValue: (base) => ({
        ...base,
        height: 22,
    }),
    indicatorsContainer: () => ({
        display: "none",
    }),
};
