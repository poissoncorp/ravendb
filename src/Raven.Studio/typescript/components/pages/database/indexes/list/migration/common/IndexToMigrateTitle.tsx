import "./IndexesImportExport.scss";
import { Icon } from "components/common/Icon";
import useUniqueId from "components/hooks/useUniqueId";
import { IndexSharedInfo } from "components/models/indexes";
import IndexUtils from "components/utils/IndexUtils";
import { FlexGrow } from "components/common/FlexGrow";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface IndexToMigrateTitleProps {
    index: Raven.Client.Documents.Indexes.IndexDefinition | IndexSharedInfo;
    disabledReason?: string;
}

export default function IndexToMigrateTitle({ index, disabledReason }: IndexToMigrateTitleProps) {
    const t = useStudioTranslation("indexesList");
    const name = "Name" in index ? index.Name : index.name;
    const type = "Type" in index ? index.Type : index.type;

    const indexDisabledReasonTooltipId = useUniqueId("indexDisabledReasonId-");
    const indexLanguageTooltipId = useUniqueId("indexLanguageTooltipId-");
    const indexTypeTooltipId = useUniqueId("indexTypeTooltipId-");

    return (
        <>
            <div className="text-truncate" title={name}>
                {name}
            </div>
            <FlexGrow />
            {disabledReason && (
                <OverlayTrigger overlay={<Tooltip id={indexDisabledReasonTooltipId}>{disabledReason}</Tooltip>}>
                    <div id={indexDisabledReasonTooltipId} className="pe-1">
                        <Icon icon="warning" color="warning" margin="m-0" id={indexDisabledReasonTooltipId} />
                    </div>
                </OverlayTrigger>
            )}
            <div className="index-legend">
                <OverlayTrigger
                    overlay={
                        <Tooltip id={indexLanguageTooltipId}>
                            <span>
                                {t("migration.indexLanguage", {
                                    context: IndexUtils.isCsharpIndex(type) ? "csharp" : "javaScript",
                                })}
                            </span>
                        </Tooltip>
                    }
                >
                    <div id={indexLanguageTooltipId}>
                        <Icon icon={IndexUtils.isCsharpIndex(type) ? "csharp-logo" : "javascript"} margin="m-0" />
                    </div>
                </OverlayTrigger>

                <OverlayTrigger overlay={<Tooltip id={indexTypeTooltipId}>{IndexUtils.formatType(type)}</Tooltip>}>
                    <div id={indexTypeTooltipId}>
                        <Icon icon={IndexUtils.indexTypeIcon(type)} margin="m-0" />
                    </div>
                </OverlayTrigger>
            </div>
        </>
    );
}
