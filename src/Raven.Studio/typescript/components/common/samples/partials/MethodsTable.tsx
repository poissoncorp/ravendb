import React, { ReactNode, useState } from "react";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Collapse from "react-bootstrap/Collapse";
import Row from "react-bootstrap/Row";
import classNames from "classnames";
import { motion } from "motion/react";
import Code, { CodeLanguage } from "components/common/Code";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { MethodEntry, MethodGroup } from "./samplesTypes";
import InsertButton from "./InsertButton";

interface MethodsTableProps {
    methodGroups: MethodGroup[];
    search: string;
    onInsert: (script: string) => void;
    language?: CodeLanguage;
    /** Rendered above the first group, for caveats that apply to the whole reference. */
    notice?: ReactNode;
}

function matchesSearch(method: MethodEntry, category: string, search: string) {
    if (!search) {
        return true;
    }

    const needle = search.toLowerCase();
    const haystack = [method.signature, category, ...(method.keywords ?? [])];

    return haystack.some((term) => term.toLowerCase().includes(needle));
}

export default function MethodsTable({ methodGroups, search, onInsert, language = "rql", notice }: MethodsTableProps) {
    const filteredGroups = methodGroups
        .map((group) => ({
            ...group,
            methods: group.methods.filter((method) => matchesSearch(method, group.category, search)),
        }))
        .filter((group) => group.methods.length > 0);

    return (
        <div className="methods-table vstack gap-3 px-3 py-2">
            {notice && !search && <div>{notice}</div>}
            {filteredGroups.length === 0 && (
                <EmptySet compact>
                    {search ? "No available methods match your search." : "No methods are available."}
                </EmptySet>
            )}
            {filteredGroups.map((group) => (
                <MethodGroupCard key={group.category} group={group} onInsert={onInsert} language={language} />
            ))}
        </div>
    );
}

interface MethodGroupCardProps {
    group: MethodGroup;
    onInsert: (script: string) => void;
    language: CodeLanguage;
}

function MethodGroupCard({ group, onInsert, language }: MethodGroupCardProps) {
    return (
        <div>
            <h4 className="mb-2 mt-0">{group.category}</h4>
            <Card className="border border-color-light rounded overflow-hidden mb-0">
                <Row className="mx-0 panel-bg-2 fw-regular border-color-light">
                    <Col xs={5} className="py-2 px-3">
                        Method signature
                    </Col>
                    <Col xs={5} className="py-2 px-3 border-start border-color-light">
                        Description
                    </Col>
                    <Col xs={2} className="py-2 px-3 border-start border-color-light">
                        Return type
                    </Col>
                </Row>
                {group.methods.map((method) => (
                    <MethodRow key={method.signature} method={method} onInsert={onInsert} language={language} />
                ))}
            </Card>
        </div>
    );
}

interface MethodRowProps {
    method: MethodEntry;
    onInsert: (script: string) => void;
    language: CodeLanguage;
}

function MethodRow({ method, onInsert, language }: MethodRowProps) {
    const [open, setOpen] = useState(false);
    const hasExample = !!method.sampleScript && !method.isUnavailable;

    const toggle = () => {
        if (hasExample) {
            setOpen((prev) => !prev);
        }
    };

    return (
        <>
            <Row
                className={classNames("mx-0 border-top border-color-light method-row", {
                    "method-row--expandable": hasExample,
                    "method-row--unavailable": method.isUnavailable,
                })}
                onClick={toggle}
            >
                <Col xs={5} className="py-2 px-3">
                    {hasExample && (
                        <motion.span
                            className="method-row__chevron"
                            animate={{ rotate: open ? 90 : 0 }}
                            transition={{ duration: 0.15 }}
                        >
                            <Icon size="sm" icon="chevron-right" margin="m-0" />
                        </motion.span>
                    )}
                    <code className={classNames({ "ms-4": !hasExample })}>{method.signature}</code>
                </Col>
                <Col xs={5} className="py-2 px-3 border-start border-color-light">
                    {method.description}
                </Col>
                <Col xs={2} className="py-2 px-3 border-start border-color-light">
                    <code>{method.returnType}</code>
                </Col>
            </Row>
            {hasExample && (
                <Collapse in={open}>
                    <div>
                        <div className="method-example border-top border-color-light px-3 py-2">
                            <div className="fw-semibold text-muted small mb-1">Example usage</div>
                            <Code
                                code={method.sampleScript}
                                language={language}
                                isRunQueryHidden
                                isTitleHidden
                                extraActions={<InsertButton onInsert={() => onInsert(method.sampleScript)} />}
                            />
                        </div>
                    </div>
                </Collapse>
            )}
        </>
    );
}
