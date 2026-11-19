import copyToClipboard from "common/copyToClipboard";
import changeVectorUtils from "common/changeVectorUtils";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import { FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ChangeVectorDetailsProps {
    sourceDatabaseChangeVector: string;
    lastAcceptedChangeVectorFromDestination: string;
}

export function ChangeVectorDetails(props: ChangeVectorDetailsProps) {
    const { sourceDatabaseChangeVector, lastAcceptedChangeVectorFromDestination } = props;
    const t = useStudioTranslation("ongoingTasks");

    const handleCopyToClipboard = (value: string) => {
        copyToClipboard.copy(value, t("changeVectorDetails.itemCopied"));
    };

    const sourceDatabaseChangeVectorFormatted = sourceDatabaseChangeVector
        ? changeVectorUtils.formatChangeVector(sourceDatabaseChangeVector, true)
        : null;
    const lastAcceptedChangeVectorFromDestinationFormatted = lastAcceptedChangeVectorFromDestination
        ? changeVectorUtils.formatChangeVector(lastAcceptedChangeVectorFromDestination, true)
        : null;

    if (!sourceDatabaseChangeVectorFormatted && !lastAcceptedChangeVectorFromDestination) {
        return null;
    }

    return (
        <div>
            {sourceDatabaseChangeVectorFormatted && (
                <div className="mb-2">
                    <FormLabel htmlFor="sourceDatabaseCv" className="mb-0 md-label">
                        {t("changeVectorDetails.sourceDatabaseCv")}
                    </FormLabel>
                    <InputGroup>
                        <Form.Control
                            type="text"
                            className="form-control"
                            id="sourceDatabaseCv"
                            readOnly
                            value={sourceDatabaseChangeVectorFormatted.map((x) => x.fullFormat).join(", ")}
                        />
                        <Button
                            onClick={() =>
                                handleCopyToClipboard(
                                    sourceDatabaseChangeVectorFormatted.map((x) => x.fullFormat).join(",")
                                )
                            }
                            variant="primary"
                            size="sm"
                            title={t("copyToClipboard")}
                        >
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </InputGroup>
                </div>
            )}

            {lastAcceptedChangeVectorFromDestinationFormatted && (
                <div>
                    <FormLabel htmlFor="lastAcceptedCV" className="mb-0 md-label">
                        {t("changeVectorDetails.lastAcceptedCv")}
                    </FormLabel>
                    <InputGroup>
                        <Form.Control
                            type="text"
                            className="form-control"
                            id="lastAcceptedCV"
                            readOnly
                            value={lastAcceptedChangeVectorFromDestinationFormatted.map((x) => x.fullFormat).join(", ")}
                        />
                        <Button
                            onClick={() =>
                                handleCopyToClipboard(
                                    lastAcceptedChangeVectorFromDestinationFormatted.map((x) => x.fullFormat).join(", ")
                                )
                            }
                            variant="primary"
                            size="sm"
                            title={t("copyToClipboard")}
                        >
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </InputGroup>
                </div>
            )}
        </div>
    );
}
