import { FormSelect, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { AdminLogsConfigLogsFormData } from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigLogs";
import { AdminLogsViewSettingsFormData } from "components/pages/resources/manageServer/adminLogs/viewSettings/AdminLogsViewSettingsModal";
import { logLevelOptions, logFilterActionOptions } from "components/utils/common";
import { Control } from "react-hook-form";
import { Row, Col, FormGroup, Label, Button } from "reactstrap";

interface AdminLogsFilterFieldProps {
    control: Control<AdminLogsViewSettingsFormData | AdminLogsConfigLogsFormData>;
    idx: number;
    remove: () => void;
}

export default function AdminLogsFilterField({ control, idx, remove }: AdminLogsFilterFieldProps) {
    return (
        <div className="p-1 bg-faded-info rounded">
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
            <FormGroup className="flex-grow-1">
                <Label>Condition</Label>
                <FormInput
                    control={control}
                    name={`filters.${idx}.condition`}
                    type="text"
                    addon={
                        <Button type="button" color="link" className="text-danger" onClick={remove}>
                            <Icon icon="trash" margin="m-0" />
                        </Button>
                    }
                />
            </FormGroup>
        </div>
    );
}
