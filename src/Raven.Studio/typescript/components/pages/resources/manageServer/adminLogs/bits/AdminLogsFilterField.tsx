import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormInput, FormLabel, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";
import { AdminLogsConfigLogsFormData } from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigLogs";
import { AdminLogsViewSettingsFormData } from "components/pages/resources/manageServer/adminLogs/view/AdminLogsViewSettingsModal";
import { logFilterActionOptions, logLevelOptions, logLevelRelevances } from "components/utils/common";
import { Control, useWatch } from "react-hook-form";
import { components, OptionProps } from "react-select";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type FormData = AdminLogsViewSettingsFormData | AdminLogsConfigLogsFormData;

interface LevelOptionProps extends SelectOption {
    isDisabled?: boolean;
    level?: "max" | "min";
}

interface AdminLogsFilterFieldProps {
    control: Control<FormData>;
    idx: number;
    remove: () => void;
}

export default function AdminLogsFilterField({ control, idx, remove }: AdminLogsFilterFieldProps) {
    const t = useStudioTranslation("adminLogs");
    const formValues = useWatch({ control });

    const maxLevel = formValues.filters[idx].maxLevel;
    const minLevel = formValues.filters[idx].minLevel;

    const getMinLevelOptions = (): LevelOptionProps[] => {
        if (maxLevel) {
            return logLevelOptions.map((option) => ({
                ...option,
                level: "min",
                isDisabled: logLevelRelevances[option.value] > logLevelRelevances[maxLevel],
            }));
        }

        return logLevelOptions;
    };

    const getMaxLevelOptions = (): LevelOptionProps[] => {
        if (minLevel) {
            return logLevelOptions.map((option) => ({
                ...option,
                level: "max",
                isDisabled: logLevelRelevances[option.value] < logLevelRelevances[minLevel],
            }));
        }

        return logLevelOptions;
    };

    return (
        <Card color="faded-info" className="p-3 rounded">
            <Row>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <FormLabel>{t("filterField.minLevel")}</FormLabel>
                        <FormSelect
                            control={control}
                            name={`filters.${idx}.minLevel`}
                            options={getMinLevelOptions()}
                            components={{ Option: LevelOption }}
                        />
                    </FormGroup>
                </Col>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <FormLabel>{t("filterField.maxLevel")}</FormLabel>
                        <FormSelect
                            control={control}
                            name={`filters.${idx}.maxLevel`}
                            options={getMaxLevelOptions()}
                            components={{ Option: LevelOption }}
                        />
                    </FormGroup>
                </Col>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <FormLabel>
                            {t("filterField.action")}
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        <p>{t("filterField.actionInfo.intro")}</p>
                                        <ul className="mb-1">
                                            <li className="mb-1">
                                                <StudioTrans
                                                    ns="adminLogs"
                                                    i18nKey="filterField.actionInfo.ignore"
                                                    components={{ code: <code /> }}
                                                />
                                            </li>
                                            <li className="mb-1">
                                                <StudioTrans
                                                    ns="adminLogs"
                                                    i18nKey="filterField.actionInfo.ignoreFinal"
                                                    components={{ code: <code />, br: <br /> }}
                                                />
                                            </li>
                                            <li className="mb-1">
                                                <StudioTrans
                                                    ns="adminLogs"
                                                    i18nKey="filterField.actionInfo.log"
                                                    components={{ code: <code /> }}
                                                />
                                            </li>
                                            <li className="mb-1">
                                                <StudioTrans
                                                    ns="adminLogs"
                                                    i18nKey="filterField.actionInfo.logFinal"
                                                    components={{ code: <code />, br: <br /> }}
                                                />
                                            </li>
                                            <li>
                                                <StudioTrans
                                                    ns="adminLogs"
                                                    i18nKey="filterField.actionInfo.neutral"
                                                    components={{ code: <code /> }}
                                                />
                                            </li>
                                        </ul>
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </FormLabel>
                        <FormSelect control={control} name={`filters.${idx}.action`} options={logFilterActionOptions} />
                    </FormGroup>
                </Col>
            </Row>
            <div className="flex-grow-1 mb-0">
                <FormLabel className="d-flex">
                    {t("filterField.condition")}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <hr className="p-0 m-0" />
                                <p>
                                    <StudioTrans
                                        ns="adminLogs"
                                        i18nKey="filterField.conditionInfo.intro"
                                        components={{ br: <br /> }}
                                    />
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="adminLogs"
                                        i18nKey="filterField.conditionInfo.database"
                                        components={{ strong: <strong /> }}
                                    />
                                    <br />
                                    <code>
                                        contains(&apos;&#36;&#123;event-properties:item=Resource&#125;&apos;,
                                        &apos;DB1&apos;)
                                    </code>
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="adminLogs"
                                        i18nKey="filterField.conditionInfo.databaseWithException"
                                        components={{ strong: <strong /> }}
                                    />
                                    <br />
                                    <code>
                                        contains(&apos;&#36;&#123;event-properties:item=Resource&#125;&apos;,
                                        &apos;DB2&apos;) and exception &#33;&#61; null
                                    </code>
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="adminLogs"
                                        i18nKey="filterField.conditionInfo.index"
                                        components={{ strong: <strong /> }}
                                    />
                                    <br />
                                    <code>
                                        contains(&apos;&#36;&#123;event-properties:item=Component&#125;&apos;,
                                        &apos;MyIndex&apos;)
                                    </code>
                                </p>
                                <p>
                                    <StudioTrans
                                        ns="adminLogs"
                                        i18nKey="filterField.conditionInfo.logger"
                                        components={{ strong: <strong /> }}
                                    />
                                    <br />
                                    <code>logger &#61;&#61; &apos;Voron.Impl.Journal.WriteAheadJournal&apos;</code>
                                </p>
                                <p>
                                    {t("filterField.conditionInfo.length")}
                                    <br />
                                    <code>length(message) &gt; 200</code>
                                </p>
                                <hr className="p-0 m-0" />
                                <p className="m-0">
                                    {t("filterField.conditionInfo.learnMore")}
                                    <br />
                                    <a href="https://github.com/NLog/NLog/wiki/When-filter#conditions" target="_blank">
                                        github.com/NLog/NLog/wiki/When-filter#conditions
                                    </a>
                                </p>
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <InputGroup>
                    <FormInput
                        control={control}
                        name={`filters.${idx}.condition`}
                        type="text"
                        className="border-top-right-radius-none border-bottom-right-radius-none"
                    />
                    <Button type="button" variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </InputGroup>
            </div>
        </Card>
    );
}

export function LevelOption(props: OptionProps<LevelOptionProps>) {
    const t = useStudioTranslation("adminLogs");
    const { data } = props;

    const getDisabledReason = (): string => {
        if (!data.isDisabled) {
            return null;
        }
        if (data.level === "min") {
            return t("filterField.minLevelDisabled");
        }
        if (data.level === "max") {
            return t("filterField.maxLevelDisabled");
        }
    };

    return (
        <ConditionalPopover
            conditions={{
                isActive: data.isDisabled,
                message: getDisabledReason(),
            }}
            popoverPlacement="top"
            className="w-100"
        >
            <components.Option {...props}>{data.label}</components.Option>
        </ConditionalPopover>
    );
}
