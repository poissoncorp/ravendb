import { Column, Table as TanstackTable, flexRender } from "@tanstack/react-table";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, Input, Label, Button } from "reactstrap";

interface VirtualTableHeadProps<T> {
    table: TanstackTable<T>;
}

export default function VirtualTableHead<T>({ table }: VirtualTableHeadProps<T>) {
    return (
        <thead>
            {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id} className="d-flex">
                    {headerGroup.headers.map((header) => (
                        <th
                            key={header.id}
                            className="position-relative align-content-center"
                            style={{ width: header.getSize() }}
                        >
                            <div
                                className="position-relative d-flex align-items-center justify-content-between"
                                title={getHeaderTitle(header.column)}
                            >
                                {flexRender(header.column.columnDef.header, header.getContext())}

                                <ColumnSettings column={header.column}></ColumnSettings>
                            </div>
                            {header.column.getCanResize() && (
                                <div
                                    className={classNames("resizer", {
                                        "is-resizing": header.column.getIsResizing(),
                                    })}
                                    onMouseDown={header.getResizeHandler()}
                                    onTouchStart={header.getResizeHandler()}
                                ></div>
                            )}
                        </th>
                    ))}
                </tr>
            ))}
        </thead>
    );
}

function getHeaderTitle<T>(column: Column<T, unknown>): string {
    const { columnDef } = column;

    if (typeof columnDef.header === "string") {
        return columnDef.header;
    }

    if ("accessorKey" in columnDef && typeof columnDef.accessorKey === "string") {
        return columnDef.accessorKey;
    }

    return columnDef.id;
}

function ColumnSettings<T>({ column }: { column: Column<T, unknown> }) {
    const [localFilter, setLocalFilter] = useState("");

    const debouncedSetFilter = useMemo(
        () => _.debounce((value: string) => column.setFilterValue(value), 500),
        [column]
    );

    const handleFilterChange = (value: string) => {
        setLocalFilter(value);
        debouncedSetFilter(value);
    };

    if (document.querySelector("#page-host") == null) {
        return null;
    }

    if (!column.getCanSort() && !column.getCanFilter()) {
        return null;
    }

    return (
        <UncontrolledDropdown>
            <DropdownToggle caret title="Column settings" color="link" size="sm" />
            <DropdownMenu container="page-host">
                {column.getCanFilter() && (
                    <div className="px-3 py-1">
                        <Label className="small-label">Filter column</Label>
                        <div className="clearable-input">
                            <Input
                                type="text"
                                placeholder="Search..."
                                value={localFilter}
                                onChange={(e) => handleFilterChange(e.target.value)}
                                className="pe-4"
                            />
                            {localFilter && (
                                <div className="clear-button">
                                    <Button color="secondary" size="sm" onClick={() => handleFilterChange("")}>
                                        <Icon icon="clear" margin="m-0" />
                                    </Button>
                                </div>
                            )}
                        </div>
                    </div>
                )}
                {column.getCanSort() && (
                    <div className="px-3 py-1">
                        <Label className="small-label">Sort</Label>
                        <div className="d-flex gap-1">
                            <Button color="primary" onClick={() => column.toggleSorting(false)} title="Sort A to Z">
                                <Icon icon="corax-sort-az" margin="m-0" />
                            </Button>
                            <Button color="primary" onClick={() => column.toggleSorting(true)} title="Sort Z to A">
                                <Icon icon="corax-sort-za" margin="m-0" />
                            </Button>
                        </div>
                    </div>
                )}
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}
