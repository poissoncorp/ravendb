import { Icon } from "components/common/Icon";
import useId from "components/hooks/useId";
import { UncontrolledPopover } from "reactstrap";

export default function AdminLogsPersistInfoIcon() {
    const id = useId("persist-info-");

    return (
        <>
            <Icon icon="info" color="info" margin="ms-1" id={id} />
            <UncontrolledPopover target={id} trigger="hover" className="bs5">
                <div className="p-3">If not saved, above settings will reset after a server restart</div>
            </UncontrolledPopover>
        </>
    );
}
