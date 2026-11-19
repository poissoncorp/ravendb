import appUrl from "common/appUrl";
import queryUtil from "common/queryUtil";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { InputItem } from "components/models/common";
import { useAppSelector } from "components/store";
import endpoints from "endpoints";
import { useRef, useState } from "react";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ExportDocumentsDropdownProps {
    collectionName: string;
    getVisibleColumnFields: () => string[];
}

type ExportFormat = "csv" | "json";
type ExportColumns = "all" | "visible";

const FORMAT_ITEMS: InputItem<ExportFormat>[] = [
    { label: "CSV", value: "csv" },
    { label: "JSON", value: "json" },
];

export default function ExportDocumentsDropdown({
    collectionName,
    getVisibleColumnFields,
}: ExportDocumentsDropdownProps) {
    const t = useStudioTranslation("documentsList");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { reportEvent } = useEventsCollector();

    const [format, setFormat] = useState<ExportFormat>("csv");
    const [columns, setColumns] = useState<ExportColumns>("all");
    const [isOpen, setIsOpen] = useState(false);

    const formRef = useRef<HTMLFormElement>(null);

    const columnsItems: InputItem<ExportColumns>[] = [
        { label: t("export.columns.all"), value: "all" },
        { label: t("export.columns.visible"), value: "visible" },
    ];

    const handleExport = () => {
        reportEvent("query", "export-csv");

        const args = {
            format,
            field: columns === "all" ? undefined : getVisibleColumnFields(),
        };

        formRef.current.action =
            appUrl.forDatabaseQuery(databaseName) +
            endpoints.databases.streaming.streamsQueries +
            appUrl.urlEncodeArgs(args);
        formRef.current.submit();

        setIsOpen(false);
    };

    return (
        <>
            <Dropdown show={isOpen} onToggle={setIsOpen} autoClose="outside">
                <Dropdown.Toggle as={CustomDropdownToggle} variant="secondary" title={t("export.title")}>
                    <Icon icon="csv-export" />
                    {t("export.button")}
                </Dropdown.Toggle>
                <Dropdown.Menu align="end" className="p-3" style={{ minWidth: "170px" }}>
                    <div className="vstack gap-3">
                        <MultiRadioToggle<ExportFormat>
                            label={t("export.formatLabel")}
                            inputItems={FORMAT_ITEMS}
                            selectedItem={format}
                            setSelectedItem={setFormat}
                        />
                        <MultiRadioToggle<ExportColumns>
                            label={t("export.columnsLabel")}
                            inputItems={columnsItems}
                            selectedItem={columns}
                            setSelectedItem={setColumns}
                        />
                        <Button variant="primary" onClick={handleExport} className="align-self-start">
                            <Icon icon="download" />
                            {t("export.exportButton")}
                        </Button>
                    </div>
                </Dropdown.Menu>
            </Dropdown>
            <form ref={formRef} method="post" target="hidden-form" className="d-none" data-testid="export-form">
                <input
                    type="hidden"
                    name="ExportOptions"
                    value={JSON.stringify({ Query: `from ${queryUtil.escapeName(collectionName)}` })}
                />
            </form>
        </>
    );
}
