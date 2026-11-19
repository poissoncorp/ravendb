import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormRadio, FormGroup, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { useAppSelector } from "components/store";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FlexGrow } from "components/common/FlexGrow";
import useUniqueId from "components/hooks/useUniqueId";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import { useRef } from "react";
import { flushSync } from "react-dom";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesPermissionsField() {
    const t = useStudioTranslation("certificates");
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));

    const { control, formState } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesEditFormData
    >();

    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({
        control,
        name: "databasePermissions",
    });

    const permissionsListRef = useRef<HTMLDivElement>(null);
    const databaseSelectId = useUniqueId("database-permissions-select");

    const removePermission = (idx: number) => {
        flushSync(() => permissionsFieldArray.remove(idx));

        const removeButtons = permissionsListRef.current?.querySelectorAll<HTMLElement>("[data-remove-permission]");
        const nextFocusTarget = removeButtons?.length
            ? removeButtons[Math.min(idx, removeButtons.length - 1)]
            : document.getElementById(databaseSelectId);
        nextFocusTarget?.focus();
    };

    return (
        <>
            <FormLabel htmlFor={databaseSelectId}>{t("permissions.label")}</FormLabel>
            <AccessToAllDatabasesInfo securityClearance={formValues.securityClearance} />
            <Collapse in={formValues.securityClearance === "ValidUser"}>
                <div>
                    <FormGroup>
                        <SelectCreatable
                            inputId={databaseSelectId}
                            placeholder={t("permissions.databasePlaceholder")}
                            isClearable
                            options={allDatabaseNames
                                .filter((x) => !formValues.databasePermissions.map((x) => x.databaseName).includes(x))
                                .map((x) => ({
                                    value: x,
                                    label: x,
                                }))}
                            onChange={(value) => {
                                permissionsFieldArray.append({
                                    databaseName: value.value,
                                    accessLevel: "ReadWrite",
                                });
                                requestAnimationFrame(() =>
                                    permissionsListRef.current?.lastElementChild?.scrollIntoView({ block: "nearest" })
                                );
                            }}
                            isClearedAfterSelect
                            isDisabled={formState.isSubmitting}
                        />
                    </FormGroup>
                    <div
                        ref={permissionsListRef}
                        className="vstack gap-1 overflow-auto mb-3"
                        style={{ maxHeight: "240px" }}
                    >
                        {permissionsFieldArray.fields.map((field, idx) => (
                            <Card key={field.id} className="hstack rounded px-3 py-1 well">
                                {field.databaseName}
                                <FlexGrow />
                                <div
                                    role="radiogroup"
                                    aria-label={t("permissions.accessLevelFor", { database: field.databaseName })}
                                    className="hstack gap-3"
                                >
                                    <FormRadio
                                        control={control}
                                        name={`databasePermissions.${idx}.accessLevel`}
                                        value="Admin"
                                        className="text-success"
                                        color="secondary"
                                    >
                                        {t("access.admin")}
                                    </FormRadio>
                                    <FormRadio
                                        control={control}
                                        name={`databasePermissions.${idx}.accessLevel`}
                                        value="ReadWrite"
                                        className="text-warning"
                                        color="secondary"
                                    >
                                        {t("access.readWrite")}
                                    </FormRadio>
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: !hasReadOnlyCertificates,
                                            message: (
                                                <LicenseRestrictedMessage>
                                                    <StudioTrans
                                                        ns="certificates"
                                                        i18nKey="access.readOnlyRestricted"
                                                        components={{
                                                            br: <br />,
                                                            strong: <strong className="text-info" />,
                                                            icon: <Icon icon="access-read" margin="m-0" />,
                                                        }}
                                                    />
                                                </LicenseRestrictedMessage>
                                            ),
                                        }}
                                    >
                                        <FormRadio
                                            control={control}
                                            name={`databasePermissions.${idx}.accessLevel`}
                                            value="Read"
                                            className="text-info"
                                            color="secondary"
                                            disabled={!hasReadOnlyCertificates}
                                        >
                                            {t("access.read")}
                                        </FormRadio>
                                    </ConditionalPopover>
                                </div>
                                <Button
                                    variant="outline-danger"
                                    size="sm"
                                    className="border-0 ms-3"
                                    title={t("permissions.remove", { database: field.databaseName })}
                                    onClick={() => removePermission(idx)}
                                    data-remove-permission
                                >
                                    <Icon icon="trash" margin="m-0" />
                                </Button>
                            </Card>
                        ))}
                    </div>
                </div>
            </Collapse>
        </>
    );
}

function AccessToAllDatabasesInfo({ securityClearance }: { securityClearance: SecurityClearance }) {
    if (securityClearance === "Operator" || securityClearance === "ClusterAdmin") {
        return (
            <FormGroup>
                <RichAlert variant="info">
                    <StudioTrans
                        ns="certificates"
                        i18nKey="permissions.allDatabasesInfo"
                        components={{ strong: <strong /> }}
                        options={{ context: securityClearance === "ClusterAdmin" ? "clusterAdmin" : "operator" }}
                    />
                </RichAlert>
            </FormGroup>
        );
    }

    return null;
}
