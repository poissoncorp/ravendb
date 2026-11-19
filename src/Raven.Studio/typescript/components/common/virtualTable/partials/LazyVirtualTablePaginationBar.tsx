import { Icon } from "components/common/Icon";
import Select, { SelectOption } from "components/common/select/Select";
import { ChangeEvent, FocusEvent, ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import "./LazyVirtualTablePaginationBar.scss";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export interface LazyVirtualTablePagination {
    page: number;
    totalPages: number;
    firstRowNumber: number;
    lastRowNumber: number;
    totalCount: number | null;
    pageSize: number;
    pageSizeOptions: number[];
    onPageChange: (page: number) => void;
    onPageSizeChange: (pageSize: number) => void;
}

interface LazyVirtualTablePaginationBarProps {
    pagination: LazyVirtualTablePagination;
}

export default function LazyVirtualTablePaginationBar({ pagination }: LazyVirtualTablePaginationBarProps) {
    const t = useStudioTranslation("virtualTable");
    const { page, totalPages, firstRowNumber, lastRowNumber, totalCount, pageSize, pageSizeOptions } = pagination;

    const totalContext = totalCount ? "withTotal" : undefined;
    const total = totalCount ? totalCount.toLocaleString() : "";

    const rowsRange =
        firstRowNumber === 0
            ? t("pagination.noRows", { context: totalContext, total })
            : t("pagination.rowsRange", {
                  context: totalContext,
                  first: firstRowNumber.toLocaleString(),
                  last: lastRowNumber.toLocaleString(),
                  total,
              });

    return (
        <div className="lazy-virtual-table-pagination d-flex align-items-center justify-content-between gap-3 pt-1">
            <div className="d-flex align-items-center gap-3">
                <small className="text-muted text-nowrap">{rowsRange}</small>
                <div className="vr" />
                <label className="d-flex align-items-center gap-2 mb-0 text-nowrap">
                    <small className="text-muted">{t("pagination.rowsPerPage")}</small>
                    <Select
                        className="page-size-select"
                        aria-label={t("pagination.rowsPerPage")}
                        options={pageSizeOptions.map(toPageSizeOption)}
                        value={toPageSizeOption(pageSize)}
                        onChange={(option) => pagination.onPageSizeChange(option.value)}
                        isSearchable={false}
                        menuPlacement="top"
                    />
                </label>
            </div>
            <div className="d-flex align-items-center gap-1">
                <PageButton
                    title={t("pagination.firstPage")}
                    isDisabled={page <= 1}
                    onClick={() => pagination.onPageChange(1)}
                >
                    <Icon icon="chevron-left" margin="m-0" />
                    <Icon icon="chevron-left" margin="m-0" className="double-chevron" />
                </PageButton>
                <PageButton
                    title={t("pagination.previousPage")}
                    isDisabled={page <= 1}
                    onClick={() => pagination.onPageChange(page - 1)}
                >
                    <Icon icon="chevron-left" margin="m-0" />
                </PageButton>
                <StudioTrans
                    ns="virtualTable"
                    i18nKey="pagination.pageOfTotal"
                    components={{
                        prefix: <small className="text-muted ms-1" />,
                        input: <PageInput page={page} totalPages={totalPages} onPageChange={pagination.onPageChange} />,
                        suffix: <small className="text-muted me-1 text-nowrap" />,
                    }}
                    options={{ total: totalPages.toLocaleString() }}
                />
                <PageButton
                    title={t("pagination.nextPage")}
                    isDisabled={page >= totalPages}
                    onClick={() => pagination.onPageChange(page + 1)}
                >
                    <Icon icon="chevron-right" margin="m-0" />
                </PageButton>
                <PageButton
                    title={t("pagination.lastPage")}
                    isDisabled={page >= totalPages}
                    onClick={() => pagination.onPageChange(totalPages)}
                >
                    <Icon icon="chevron-right" margin="m-0" />
                    <Icon icon="chevron-right" margin="m-0" className="double-chevron" />
                </PageButton>
            </div>
        </div>
    );
}

interface PageButtonProps {
    title: string;
    isDisabled: boolean;
    onClick: () => void;
    children: ReactNode;
}

function PageButton({ title, isDisabled, onClick, children }: PageButtonProps) {
    return (
        <Button
            variant="link"
            size="sm"
            className="page-button d-flex align-items-center p-1"
            title={title}
            aria-label={title}
            disabled={isDisabled}
            onClick={onClick}
        >
            {children}
        </Button>
    );
}

interface PageInputProps {
    page: number;
    totalPages: number;
    onPageChange: (page: number) => void;
}

function PageInput({ page, totalPages, onPageChange }: PageInputProps) {
    const t = useStudioTranslation("virtualTable");
    const [value, setValue] = useState(String(page));
    const [prevPage, setPrevPage] = useState(page);

    if (page !== prevPage) {
        setPrevPage(page);
        setValue(String(page));
    }

    const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
        setValue(event.target.value);

        const parsedPage = Number(event.target.value);
        const isValidPage = Number.isInteger(parsedPage) && parsedPage >= 1 && parsedPage <= totalPages;

        if (isValidPage && parsedPage !== page) {
            onPageChange(parsedPage);
        }
    };

    return (
        <Form.Control
            type="number"
            size="sm"
            className="page-input text-center"
            aria-label={t("pagination.pageInput")}
            min={1}
            max={totalPages}
            style={{ width: `calc(${String(totalPages).length}ch + 2.5em)` }}
            value={value}
            onChange={handleChange}
            onFocus={(event: FocusEvent<HTMLInputElement>) => event.target.select()}
            onBlur={() => setValue(String(page))}
        />
    );
}

function toPageSizeOption(pageSize: number): SelectOption<number> {
    return { value: pageSize, label: pageSize.toLocaleString() };
}
