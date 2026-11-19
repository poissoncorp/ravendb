import { Icon } from "components/common/Icon";
import React from "react";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function IndexGlobalIndexing() {
    const t = useStudioTranslation("indexesList");

    return (
        <div className="pull-right" id="toggleIndexing" data-bind="requiredAccess: 'DatabaseReadWrite'">
            <Button
                variant="secondary"
                className="disable-indexing"
                title={t("globalIndexing.pauseTitle")}
                data-bind="click: stopIndexing, visible: $root.globalIndexingStatus() !== 'Paused', 
                                       attr: { title: 'Pause indexing process for ALL indexes until restart - On node ' + localNodeTag() },
                                       enable: !spinners.globalStartStop() && $root.globalIndexingStatus() !== 'Disabled', css: { 'btn-spinner': spinners.globalStartStop() }"
            >
                <Icon icon="pause" />
                {t("globalIndexing.pause")}
            </Button>
            <Button
                variant="success"
                className="enable-indexing"
                title={t("globalIndexing.resumeTitle")}
                data-bind="click: startIndexing, visible: $root.globalIndexingStatus() === 'Paused', enable: !spinners.globalStartStop(), css: { 'btn-spinner': spinners.globalStartStop() }"
            >
                <Icon icon="play" />
                {t("globalIndexing.resume")}
            </Button>
        </div>
    );
}
