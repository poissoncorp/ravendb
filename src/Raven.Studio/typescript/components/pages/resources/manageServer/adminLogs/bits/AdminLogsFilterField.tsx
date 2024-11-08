import { FormSelect, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { AdminLogsConfigLogsFormData } from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigLogs";
import { AdminLogsViewSettingsFormData } from "components/pages/resources/manageServer/adminLogs/viewSettings/AdminLogsViewSettingsModal";
import { logLevelOptions, logFilterActionOptions } from "components/utils/common";
import { Control } from "react-hook-form";
import { Row, Col, FormGroup, Label, Button, UncontrolledPopover, Card, InputGroup } from "reactstrap";

interface AdminLogsFilterFieldProps {
    control: Control<AdminLogsViewSettingsFormData | AdminLogsConfigLogsFormData>;
    idx: number;
    remove: () => void;
}

export default function AdminLogsFilterField({ control, idx, remove }: AdminLogsFilterFieldProps) {
    return (
        <Card color="faded-info" className="p-3 rounded">
            <Row>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <Label>Minimum level</Label>
                        <FormSelect control={control} name={`filters.${idx}.minLevel`} options={logLevelOptions} />
                    </FormGroup>
                </Col>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <Label>Maximum level</Label>
                        <FormSelect control={control} name={`filters.${idx}.maxLevel`} options={logLevelOptions} />
                    </FormGroup>
                </Col>
                <Col md={4}>
                    <FormGroup className="flex-grow-1">
                        <Label>Action</Label>
                        <FormSelect control={control} name={`filters.${idx}.action`} options={logFilterActionOptions} />
                    </FormGroup>
                </Col>
            </Row>
            <div className="flex-grow-1 mb-0">
                <Label className="d-flex">
                    Condition
                    <div id="condition-tooltip">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </div>
                    <UncontrolledPopover target="condition-tooltip" trigger="hover" className="bs5">
                        <div className="p-3">
                            More info here:
                            <br />
                            <a href="https://github.com/NLog/NLog/wiki/When-filter#conditions" target="_blank">
                                github.com/NLog/NLog/wiki/When-filter#conditions
                            </a>
                        </div>
                    </UncontrolledPopover>
                </Label>
                <InputGroup>
                    <FormInput
                        control={control}
                        name={`filters.${idx}.condition`}
                        type="text"
                        className="border-top-right-radius-none border-bottom-right-radius-none"
                    />
                    <Button type="button" color="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </InputGroup>
            </div>
        </Card>
    );
}
