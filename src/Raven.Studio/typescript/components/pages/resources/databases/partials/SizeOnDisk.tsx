import { DatabaseLocalInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import useUniqueId from "components/hooks/useUniqueId";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function SizeOnDisk(props: { info: DatabaseLocalInfo }) {
    const { info } = props;

    const tooltipId = useUniqueId("size-on-disk-tooltip");

    if (!info) {
        return null;
    }
    const tempBufferSize = info.tempBuffersSize?.SizeInBytes ?? 0;
    const totalSize = info.totalSize?.SizeInBytes ?? 0;
    const grandTotalSize = tempBufferSize + totalSize;

    return (
        <div>
            <OverlayTrigger
                overlay={
                    <Tooltip id={tooltipId}>
                        <StudioTrans
                            ns="databases"
                            i18nKey="sizeOnDisk.tooltip"
                            options={{
                                data: genUtils.formatBytesToSize(totalSize),
                                temp: genUtils.formatBytesToSize(tempBufferSize),
                                total: genUtils.formatBytesToSize(grandTotalSize),
                            }}
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </Tooltip>
                }
            >
                <div>{genUtils.formatBytesToSize(grandTotalSize)}</div>
            </OverlayTrigger>
        </div>
    );
}
