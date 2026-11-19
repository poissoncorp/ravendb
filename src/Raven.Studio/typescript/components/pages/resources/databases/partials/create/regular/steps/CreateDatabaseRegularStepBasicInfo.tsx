import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CreateDatabaseRegularFormData as FormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import AuthenticationOffMessage from "components/pages/resources/databases/partials/create/shared/AuthenticationOffMessage";
import EncryptionUnavailableMessage from "components/pages/resources/databases/partials/create/shared/EncryptionUnavailableMessage";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { createDatabaseRegularDataUtils } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularDataUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const newDatabaseImg = require("Content/img/createDatabase/new-database.svg");

export default function CreateDatabaseRegularStepBasicInfo() {
    const t = useStudioTranslation("databases");
    const { control, setValue, setFocus } = useFormContext<FormData>();
    const nodeTagsCount = useAppSelector(clusterSelectors.allNodeTags).length;

    const {
        basicInfoStep: { isEncrypted },
    } = useWatch({ control });

    const isManualReplicationRequiredForEncryption =
        createDatabaseRegularDataUtils.getIsManualReplicationRequiredForEncryption(nodeTagsCount, isEncrypted);

    useEffect(() => {
        if (isManualReplicationRequiredForEncryption) {
            setValue("replicationAndShardingStep.isManualReplication", true, { shouldValidate: true });
        }
    }, [isManualReplicationRequiredForEncryption, setValue]);

    // Focus the database name input when the step is loaded
    useEffect(() => {
        setFocus("basicInfoStep.databaseName");
    }, [setFocus]);

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={newDatabaseImg} alt="" className="step-img" />
            </div>
            <h2 className="text-center mb-4">{t("createDatabase.title")}</h2>
            <Row>
                <Col lg={{ offset: 2, span: 8 }} className="text-center">
                    <FormInput
                        type="text"
                        control={control}
                        name="basicInfoStep.databaseName"
                        placeholder={t("createDatabase.databaseNamePlaceholder")}
                        id="DbName"
                        autoComplete="off"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <IsEncryptedField />
            </Row>
        </div>
    );
}

function IsEncryptedField() {
    const t = useStudioTranslation("databases");
    const { control, formState } = useFormContext<FormData>();

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);
    const isAllowEncryptedDatabasesOverHttp = useAppSelector(accessManagerSelectors.isAllowEncryptedDatabasesOverHttp);

    return (
        <div className="d-flex align-items-center justify-content-center">
            <ConditionalPopover
                conditions={[
                    {
                        isActive: !hasEncryption,
                        message: <EncryptionUnavailableMessage />,
                    },
                    {
                        isActive: !isAllowEncryptedDatabasesOverHttp && !isSecureServer,
                        message: <AuthenticationOffMessage />,
                    },
                ]}
                popoverPlacement="top"
            >
                <FormSwitch
                    color="primary"
                    control={control}
                    name="basicInfoStep.isEncrypted"
                    disabled={
                        !hasEncryption ||
                        (!isAllowEncryptedDatabasesOverHttp && !isSecureServer) ||
                        formState.isSubmitting
                    }
                >
                    <Icon icon="encryption" />
                    {t("createDatabase.encryptAtRest")}
                    {!hasEncryption && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                </FormSwitch>
            </ConditionalPopover>
            {hasEncryption && isSecureServer && (
                <PopoverWithHoverWrapper
                    message={
                        <StudioTrans
                            ns="databases"
                            i18nKey="createDatabase.encryptionInfo"
                            components={{ code: <code /> }}
                        />
                    }
                    placement="right"
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            )}
        </div>
    );
}
