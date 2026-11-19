import { FormInput, FormLabel } from "components/common/Form";
import React from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "../../createDatabaseFromBackupValidation";
import { useFormContext, useWatch } from "react-hook-form";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EncryptionFieldProps {
    sourceType: RestoreSource;
}

export default function EncryptionField({ sourceType }: EncryptionFieldProps) {
    const t = useStudioTranslation("databases");
    const { control } = useFormContext<FormData>();

    const {
        sourceStep: {
            sourceData: { [sourceType]: selectedSourceData },
        },
    } = useWatch({
        control,
    });

    const isEncrypted = selectedSourceData.pointsWithTags[0].restorePoint?.isEncrypted;

    if (!isEncrypted) {
        return null;
    }

    return (
        <Row className="mt-2">
            <Col lg="3">
                <FormLabel className="col-form-label">
                    <StudioTrans
                        ns="databases"
                        i18nKey="createFromBackup.source.encryptionKey"
                        components={{ small: <small className="text-muted" /> }}
                    />
                </FormLabel>
            </Col>
            <Col>
                <FormInput
                    type="text"
                    control={control}
                    name={`sourceStep.sourceData.${sourceType}.encryptionKey`}
                    placeholder={t("createFromBackup.source.encryptionKeyPlaceholder")}
                />
            </Col>
        </Row>
    );
}
