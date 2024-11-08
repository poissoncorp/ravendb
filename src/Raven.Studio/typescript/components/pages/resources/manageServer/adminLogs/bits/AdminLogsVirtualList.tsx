import { useVirtualizer } from "@tanstack/react-virtual";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import {
    adminLogsSelectors,
    adminLogsActions,
    AdminLogsMessage,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import { useRef, useEffect } from "react";
import { Collapse, Table } from "reactstrap";

export default function AdminLogsVirtualList(props: { availableHeightInPx: number }) {
    const dispatch = useAppDispatch();

    const filteredLogs = useAppSelector(adminLogsSelectors.filteredLogs);
    const isMonitorTail = useAppSelector(adminLogsSelectors.isMonitorTail);

    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: filteredLogs.length,
        estimateSize: () => 22,
        getScrollElement: () => listRef.current,
        overscan: 5,
        measureElement: (element) => {
            return element.getBoundingClientRect().height;
        },
        getItemKey: (index) => filteredLogs[index]._meta.id,
    });

    // Scroll to bottom if logs are updated and isMonitorTail is true
    useEffect(() => {
        if (isMonitorTail && listRef.current) {
            listRef.current.scrollTo(0, listRef.current?.scrollHeight);
        }
    }, [filteredLogs.length, isMonitorTail]);

    return (
        <div ref={listRef} style={{ overflow: "auto", height: props.availableHeightInPx }}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const log = filteredLogs[virtualRow.index];

                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                padding: "2px 0px",
                            }}
                        >
                            <div
                                style={{
                                    borderLeft: `4px solid ${getTextColor(log.Level)}`,
                                    backgroundColor: `var(--panel-bg-1)`,
                                }}
                            >
                                <div
                                    key={log.Date}
                                    className="d-flex align-items-center cursor-pointer text-truncate"
                                    onClick={() => dispatch(adminLogsActions.isLogExpandedToggled(log))}
                                    style={{ padding: `4.3px` }}
                                >
                                    <div style={{ margin: "0 4.3px 0 0" }}>
                                        <Icon
                                            icon={log._meta.isExpanded ? "chevron-down" : "chevron-right"}
                                            className="fs-6"
                                            margin="m-0"
                                        />
                                    </div>
                                    <span className="text-truncate">
                                        <LogItemTitleFieldValue value={log.Date} />
                                        <LogItemSeparator />
                                        <LogItemTitleFieldValue value={log.Level} />
                                        <LogItemSeparator />
                                        <LogItemTitleFieldValue value={log.Component} />
                                        <LogItemSeparator />
                                        <LogItemTitleFieldValue value={log.Resource} />
                                        <LogItemSeparator />
                                        <LogItemTitleFieldValue value={log.Message} />
                                    </span>
                                </div>
                                <Collapse isOpen={log._meta.isExpanded} className="vstack gap-2 p-2">
                                    <Code
                                        code={log.Message}
                                        elementToCopy={log.Message}
                                        language="plaintext"
                                        codeClassName="wrapped pe-4"
                                    />
                                    <div className="p-2">
                                        <Table size="sm" className="m-0">
                                            <tbody>
                                                {Object.keys(log)
                                                    .filter(
                                                        (key: keyof AdminLogsMessage) =>
                                                            key !== "_meta" && key !== "Message"
                                                    )
                                                    .map((key: keyof AdminLogsMessage) => (
                                                        <tr key={key}>
                                                            <td>{getFormattedFieldName(key)}</td>
                                                            <td>{String(log[key] ?? "-")}</td>
                                                        </tr>
                                                    ))}
                                            </tbody>
                                        </Table>
                                    </div>
                                </Collapse>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

function getTextColor(level: AdminLogsMessage["Level"]): string {
    switch (level) {
        case "DEBUG":
            return "var(--bs-success)";
        case "INFO":
            return "var(--bs-info)";
        case "WARN":
            return "var(--bs-warning)";
        case "ERROR":
            return "var(--bs-orange)";
        case "FATAL":
            return "var(--bs-danger)";
        case "OFF":
        case "TRACE":
            return "var(--panel-bg-3)";
        default:
            return assertUnreachable(level);
    }
}

function LogItemSeparator() {
    return <span style={{ margin: "0px 3px" }}>|</span>;
}

function LogItemTitleFieldValue({ value = "" }: { value: string }) {
    const filter = useAppSelector(adminLogsSelectors.filter);

    const matchedIndices = getMatchedIndices(value, filter);
    const characters = value.split("");

    return (
        <span>
            {characters.map((char, index) => (
                <Char key={index} char={char} index={index} matchedIndices={matchedIndices} />
            ))}
        </span>
    );
}

function getMatchedIndices(value: string, filter: string): number[] {
    if (!filter) {
        return [];
    }

    const filterLower = filter.toLowerCase();
    const valueLower = value.toLowerCase();

    let currentIndex = 0;
    const indices: number[] = [];

    while (currentIndex < valueLower.length) {
        const matchIndex = valueLower.indexOf(filterLower, currentIndex);
        if (matchIndex === -1) {
            break;
        }

        for (let i = 0; i < filterLower.length; i++) {
            indices.push(matchIndex + i);
        }

        currentIndex = matchIndex + 1;
    }

    return indices;
}

interface CharProps {
    char: string;
    index: number;
    matchedIndices: number[];
}

function Char({ char, index, matchedIndices }: CharProps) {
    const isHighlighted = matchedIndices.includes(index);

    if (isHighlighted) {
        return <mark className="bg-faded-warning p-0">{char}</mark>;
    }

    return char;
}

function getFormattedFieldName(fieldName: keyof AdminLogsMessage): string {
    switch (fieldName) {
        case "ThreadID":
            return "Thread ID";
        default:
            return fieldName;
    }
}
