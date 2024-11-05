import { createListenerMiddleware } from "@reduxjs/toolkit";
import { adminLogsActions } from "./adminLogsSlice";
import { AppListenerEffectApi } from "components/store";
import adminLogsWebSocketClient from "common/adminLogsWebSocketClient";
import eventsCollector from "common/eventsCollector";

export const adminLogsMiddleware = createListenerMiddleware();

let liveClient: adminLogsWebSocketClient = null;

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.isPausedToggled,
    effect: (_, listenerApi: AppListenerEffectApi) => {
        const state = listenerApi.getState();

        if (state.adminLogs.isPaused) {
            liveClient?.dispose();
            liveClient = null;
        } else if (!liveClient) {
            liveClient = new adminLogsWebSocketClient((message) =>
                listenerApi.dispatch(adminLogsActions.logsAppended(message))
            );
        }
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.liveClientStopped,
    effect: () => {
        eventsCollector.default.reportEvent("admin-logs", "connect");

        liveClient?.dispose();
        liveClient = null;
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.liveClientStarted,
    effect: (_, listenerApi: AppListenerEffectApi) => {
        if (liveClient) {
            return;
        }

        liveClient = new adminLogsWebSocketClient((message) =>
            listenerApi.dispatch(adminLogsActions.logsAppended(message))
        );
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.maxLogsCountSet,
    effect: (_, listenerApi: AppListenerEffectApi) => {
        const state = listenerApi.getState();
        const logsLength = state.adminLogs.logs.length;
        const maxLogsCount = state.adminLogs.maxLogsCount;

        if (logsLength > maxLogsCount) {
            listenerApi.dispatch(adminLogsActions.logsSet(state.adminLogs.logs.slice(logsLength - maxLogsCount)));
        }
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.isAllExpandedToggled,
    effect: (_, listenerApi: AppListenerEffectApi) => {
        const state = listenerApi.getState();

        listenerApi.dispatch(
            adminLogsActions.logsSet(
                state.adminLogs.logs.map((log) => ({
                    ...log,
                    _meta: { ...log._meta, isExpanded: state.adminLogs.isAllExpanded },
                }))
            )
        );
    },
});
