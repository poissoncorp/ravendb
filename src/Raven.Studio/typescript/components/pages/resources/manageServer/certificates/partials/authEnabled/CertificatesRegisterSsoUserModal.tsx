import { yupResolver } from "@hookform/resolvers/yup";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormInput, FormLabel, FormRadio, FormSelect, FormSwitch } from "components/common/Form";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import Modal from "components/common/Modal";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import {
    SsoIdentifierDto,
    SsoProvider,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { useAppDispatch, useAppSelector } from "components/store";
import { exhaustiveStringTuple, tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Card from "react-bootstrap/Card";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import * as yup from "yup";
import useUniqueId from "components/hooks/useUniqueId";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

// exhaustiveStringTuple makes the compiler enforce that every SsoProvider is listed here.
const ssoProviders = exhaustiveStringTuple<SsoProvider>()("Github", "Google", "Microsoft", "Windows");

const ssoProviderOptions: SelectOption[] = ssoProviders.map((p) => ({ value: p, label: p }));

function getIdentifierPlaceholderByProvider(t: StudioTranslate<"certificates">): Record<SsoProvider, string> {
    return {
        Windows: t("ssoUser.usernamePlaceholder"),
        Google: "email@gmail.com",
        Github: t("ssoUser.usernameOrEmailPlaceholder"),
        Microsoft: t("ssoUser.usernameOrEmailPlaceholder"),
    };
}

type SsoUserFormClearance = "ValidUser" | "Operator" | "ClusterAdmin";

// Narrow the server's broad SecurityClearance union down to the three values the SSO user form supports,
// defaulting anything else (or nothing) to "ValidUser" - avoids an unchecked `as` cast.
function toFormSecurityClearance(
    clearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance | undefined
): SsoUserFormClearance {
    return clearance === "Operator" || clearance === "ClusterAdmin" ? clearance : "ValidUser";
}

export default function CertificatesRegisterSsoUserModal() {
    const t = useStudioTranslation("certificates");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));
    const ssoServerCertificates = useAppSelector(certificatesSelectors.ssoServerCertificates);
    const ssoUserToEdit = useAppSelector(certificatesSelectors.ssoUserToEdit);
    const ssoUserToClone = useAppSelector(certificatesSelectors.ssoUserToClone);

    const isEditing = ssoUserToEdit != null;
    const isCloning = ssoUserToClone != null;

    const sourceForDefaults = ssoUserToEdit ?? ssoUserToClone;

    const form = useForm<FormData>({
        resolver: yupResolver(getSchema(t)),
        defaultValues: {
            displayName: sourceForDefaults?.Name ?? "",
            ssoIdentifiers:
                sourceForDefaults?.SsoIdentifiers?.length > 0
                    ? sourceForDefaults.SsoIdentifiers.map((id) => ({
                          provider: id.Provider,
                          domain: id.Domain ?? "",
                          identifier: id.Identifier,
                      }))
                    : [{ provider: "Github", domain: "", identifier: "" }],
            ssoServerPublicKeyPinningHashes: sourceForDefaults?.SsoServerPublicKeyPinningHashes ?? [],
            allowAnySso: sourceForDefaults?.AllowAnySsoServer ?? false,
            securityClearance: toFormSecurityClearance(sourceForDefaults?.SecurityClearance),
            databasePermissions:
                sourceForDefaults != null ? certificatesUtils.mapDatabasePermissionsFromDto(sourceForDefaults) : [],
        },
    });

    const { control, formState, handleSubmit, reset } = form;
    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({ control, name: "databasePermissions" });
    const identifiersFieldArray = useFieldArray({ control, name: "ssoIdentifiers" });

    const handleClose = () => dispatch(certificatesActions.isRegisterSsoUserModalOpenToggled());
    const titleId = useUniqueId("sso-user-title");

    const handleSubmitForm: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            const permissions =
                formData.securityClearance === "Operator" || formData.securityClearance === "ClusterAdmin"
                    ? null
                    : Object.fromEntries(
                          formData.databasePermissions.map(({ databaseName, accessLevel }) => [
                              databaseName,
                              accessLevel,
                          ])
                      );

            const ssoIdentifiers: SsoIdentifierDto[] = formData.ssoIdentifiers.map((id) => ({
                Provider: id.provider,
                Domain: id.provider === "Windows" && id.domain ? id.domain : undefined,
                Identifier: id.identifier,
            }));

            if (isEditing) {
                reportEvent("certificates", "edit-sso-user");

                await manageServerService.updateCertificate(
                    {
                        Name: formData.displayName,
                        Thumbprint: ssoUserToEdit.Thumbprint,
                        SecurityClearance: formData.securityClearance,
                        Permissions: permissions,
                        TwoFactorAuthenticationKey: null,
                        SsoServerPublicKeyPinningHashes: formData.allowAnySso
                            ? []
                            : formData.ssoServerPublicKeyPinningHashes,
                        AllowAnySsoServer: formData.allowAnySso,
                        SsoIdentifiers: ssoIdentifiers,
                    },
                    false
                );
            } else {
                reportEvent("certificates", "register-sso-user");

                await manageServerService.registerSsoUserEntry({
                    Name: formData.displayName,
                    Usage: "SsoClient",
                    SsoIdentifiers: ssoIdentifiers,
                    SsoServerPublicKeyPinningHashes: formData.allowAnySso
                        ? []
                        : formData.ssoServerPublicKeyPinningHashes,
                    AllowAnySsoServer: formData.allowAnySso,
                    SecurityClearance: formData.securityClearance,
                    Permissions: permissions ?? {},
                });
            }

            reset(formData);
            dispatch(certificatesActions.fetchData());
            handleClose();
        });
    };

    const identifierPlaceholderByProvider = getIdentifierPlaceholderByProvider(t);

    const ssoServerOptions = ssoServerCertificates.map((cert) => ({
        value: cert.PublicKeyPinningHash,
        label: cert.Name,
    })) satisfies SelectOption[];

    return (
        <Modal
            show
            size="lg"
            centered
            contentClassName="modal-border bulge-primary"
            backdrop="static"
            onHide={handleClose}
            aria-labelledby={titleId}
        >
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleSubmitForm)}>
                    <Modal.Header className="pt-3 pb-0" onCloseClick={handleClose}>
                        <Icon
                            icon="user"
                            addon={isEditing ? "edit" : isCloning ? "copy" : "plus"}
                            size="lg"
                            color="primary"
                            margin="me-3"
                        />
                        <h3 id={titleId} className="mb-0">
                            {t("ssoUser.title", { context: isEditing ? "edit" : isCloning ? "clone" : "add" })}
                        </h3>
                    </Modal.Header>
                    <Modal.Body>
                        <FormGroup>
                            <FormLabel>{t("ssoUser.displayName")}</FormLabel>
                            <FormInput
                                control={control}
                                type="text"
                                name="displayName"
                                placeholder={t("ssoUser.displayNamePlaceholder")}
                            />
                        </FormGroup>
                        <FormGroup>
                            <div className="hstack gap-1 mb-1">
                                <FormLabel className="mb-0 d-flex align-items-center gap-1">
                                    {t("ssoUser.identifiers")}{" "}
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: true,
                                            message: (
                                                <StudioTrans
                                                    ns="certificates"
                                                    i18nKey="ssoUser.identifiersInfo"
                                                    components={{ br: <br /> }}
                                                />
                                            ),
                                        }}
                                        popoverPlacement="right"
                                    >
                                        <Icon icon="info" margin="m-0" className="small" color="info" />
                                    </ConditionalPopover>
                                </FormLabel>
                                <FlexGrow />
                                <Button
                                    variant="link"
                                    size="sm"
                                    onClick={() =>
                                        identifiersFieldArray.append({
                                            provider: "Github",
                                            domain: "",
                                            identifier: "",
                                        })
                                    }
                                >
                                    <Icon icon="plus" margin="me-1" />
                                    {t("ssoUser.addIdentifier")}
                                </Button>
                            </div>
                            <div className="vstack gap-2">
                                {identifiersFieldArray.fields.map((field, idx) => {
                                    const provider = formValues.ssoIdentifiers?.[idx]?.provider;
                                    return (
                                        <Card key={field.id}>
                                            <div className="hstack gap-2 align-items-stretch">
                                                <div style={{ minWidth: "130px" }}>
                                                    <FormSelect
                                                        control={control}
                                                        name={`ssoIdentifiers.${idx}.provider`}
                                                        options={ssoProviderOptions}
                                                        placeholder={t("ssoUser.providerPlaceholder")}
                                                    />
                                                </div>
                                                {provider === "Windows" && (
                                                    <FormInput
                                                        control={control}
                                                        type="text"
                                                        name={`ssoIdentifiers.${idx}.domain`}
                                                        placeholder={t("ssoUser.domainPlaceholder")}
                                                    />
                                                )}
                                                <FormInput
                                                    control={control}
                                                    type="text"
                                                    name={`ssoIdentifiers.${idx}.identifier`}
                                                    placeholder={
                                                        identifierPlaceholderByProvider[provider] ??
                                                        t("ssoUser.usernameOrEmailPlaceholder")
                                                    }
                                                />
                                                <Button
                                                    variant="danger"
                                                    onClick={() => identifiersFieldArray.remove(idx)}
                                                    disabled={identifiersFieldArray.fields.length === 1}
                                                >
                                                    <Icon icon="trash" margin="m-0" />
                                                </Button>
                                            </div>
                                        </Card>
                                    );
                                })}
                            </div>
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>{t("ssoUser.securityClearance")}</FormLabel>
                            <FormSelect
                                control={control}
                                name="securityClearance"
                                options={
                                    [
                                        {
                                            value: "ClusterAdmin",
                                            label: t("clearance.clusterAdmin"),
                                            isDisabled: !isClusterAdminOrClusterNode,
                                        },
                                        { value: "Operator", label: t("clearance.operator") },
                                        { value: "ValidUser", label: t("clearance.user") },
                                    ] satisfies SelectOption[]
                                }
                            />
                            {(formValues.securityClearance === "Operator" ||
                                formValues.securityClearance === "ClusterAdmin") && (
                                <RichAlert variant="warning" className="mt-2">
                                    <StudioTrans
                                        ns="certificates"
                                        i18nKey="ssoUser.mfaWarning"
                                        components={{ strong: <strong /> }}
                                        options={{
                                            context:
                                                formValues.securityClearance === "ClusterAdmin"
                                                    ? "clusterAdmin"
                                                    : "operator",
                                        }}
                                    />
                                </RichAlert>
                            )}
                        </FormGroup>
                        <hr />
                        <FormGroup>
                            <div className="hstack justify-content-between align-items-center mb-1">
                                <FormLabel className="mb-0">{t("ssoUser.authorizingSso")}</FormLabel>
                                <FormSwitch control={control} name="allowAnySso">
                                    {t("ssoUser.allowAnySso")}
                                </FormSwitch>
                            </div>
                            <Collapse in={!formValues.allowAnySso}>
                                <div>
                                    <FormSelect
                                        control={control}
                                        name="ssoServerPublicKeyPinningHashes"
                                        isMulti
                                        placeholder={t("ssoUser.ssoServersPlaceholder")}
                                        options={ssoServerOptions}
                                    />
                                </div>
                            </Collapse>
                        </FormGroup>
                        <hr />
                        <FormLabel>{t("ssoUser.databasePermissions")}</FormLabel>
                        {(formValues.securityClearance === "Operator" ||
                            formValues.securityClearance === "ClusterAdmin") && (
                            <FormGroup>
                                <RichAlert variant="info">
                                    <StudioTrans
                                        ns="certificates"
                                        i18nKey="ssoUser.allDatabasesInfo"
                                        components={{ strong: <strong /> }}
                                        options={{
                                            context:
                                                formValues.securityClearance === "ClusterAdmin"
                                                    ? "clusterAdmin"
                                                    : "operator",
                                        }}
                                    />
                                </RichAlert>
                            </FormGroup>
                        )}
                        <Collapse in={formValues.securityClearance === "ValidUser"}>
                            <div>
                                <FormGroup>
                                    <div className="hstack gap-2">
                                        <SelectCreatable
                                            className="flex-grow-1"
                                            placeholder={t("ssoUser.databasePlaceholder")}
                                            isClearable
                                            options={allDatabaseNames
                                                .filter(
                                                    (x) =>
                                                        !(formValues.databasePermissions ?? [])
                                                            .map((p) => p.databaseName)
                                                            .includes(x)
                                                )
                                                .map((x) => ({ value: x, label: x }))}
                                            onChange={(value) =>
                                                permissionsFieldArray.append({
                                                    databaseName: value.value,
                                                    accessLevel: "ReadWrite",
                                                })
                                            }
                                            isClearedAfterSelect
                                            isDisabled={formState.isSubmitting}
                                        />
                                    </div>
                                </FormGroup>
                                <FormGroup className="vstack gap-2">
                                    {permissionsFieldArray.fields.map((field, idx) => (
                                        <Card key={field.id} className="hstack rounded px-3 py-1 well">
                                            {field.databaseName}
                                            <FlexGrow />
                                            <div className="hstack gap-3">
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
                                                variant="link"
                                                className="px-0 ms-3"
                                                onClick={() => permissionsFieldArray.remove(idx)}
                                            >
                                                <Icon icon="trash" margin="m-0" className="text-danger" />
                                            </Button>
                                        </Card>
                                    ))}
                                </FormGroup>
                            </div>
                        </Collapse>
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="link" onClick={handleClose} className="link-muted">
                            {commonT("cancel")}
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            {t("ssoUser.submit", { context: isEditing ? "edit" : "add" })}
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </Form>
            </FormProvider>
        </Modal>
    );
}

function getSchema(t: StudioTranslate<"certificates">) {
    const ssoIdentifierSchema = yup.object({
        provider: yup.string<SsoProvider>().required(t("ssoUser.providerRequired")),
        domain: yup.string().when("provider", {
            is: "Windows",
            then: (s) => s.required(t("ssoUser.domainRequired")),
            otherwise: (s) => s.optional(),
        }),
        identifier: yup.string().required(t("ssoUser.identifierRequired")),
    });

    return yup.object({
        displayName: yup.string().required(t("ssoUser.displayNameRequired")),
        ssoIdentifiers: yup.array().of(ssoIdentifierSchema).min(1, t("ssoUser.identifiersRequired")).required(),
        ssoServerPublicKeyPinningHashes: yup
            .array()
            .of(yup.string())
            .when("allowAnySso", {
                is: false,
                then: (s) => s.min(1, t("ssoUser.ssoServersRequired")),
                otherwise: (s) => s.optional(),
            }),
        allowAnySso: yup.boolean().required(),
        securityClearance: yup.string<"ValidUser" | "Operator" | "ClusterAdmin">().required(),
        databasePermissions: certificatesUtils.databasePermissionsSchema,
    });
}

type FormData = yup.InferType<ReturnType<typeof getSchema>>;
