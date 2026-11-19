import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
} from "components/common/RichPanel";
import Collapse from "react-bootstrap/Collapse";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";

import { Icon } from "components/common/Icon";
import { FormInput, FormLabel } from "components/common/Form";
import { HStack } from "components/common/utilities/HStack";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import useConfirm from "components/common/ConfirmDialog";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { tryHandleSubmit } from "components/utils/common";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface IntegrationsUserListProps {
    initialUsername: string;
    removeUser: () => void;
}

// TODO Implement Test Connection button when server-side is ready. https://issues.hibernatingrhinos.com/issue/RavenDB-22189/PostgreSQL-test-credentials

export default function IntegrationsUserList(props: IntegrationsUserListProps) {
    const t = useStudioTranslation("integrations");
    const commonT = useStudioTranslation("common");
    const { initialUsername, removeUser } = props;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { control, formState, handleSubmit, reset, setValue } = useForm<FormData>({
        resolver: formResolver,
        defaultValues: {
            username: initialUsername,
            password: "",
        },
    });
    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control });

    const isNew = !formState.defaultValues.username;

    const { databasesService } = useServices();
    const confirm = useConfirm();

    const asyncGeneratePassword = useAsyncCallback(databasesService.generateSecret, {
        onSuccess(result) {
            setValue("password", result);
        },
    });

    const asyncDeleteUser = useAsyncCallback(
        () => databasesService.deleteIntegrationsPostgreSqlCredentials(databaseName, initialUsername),
        {
            onSuccess() {
                removeUser();
            },
        }
    );

    const onDeleteUser = async () => {
        const isConfirmed = await confirm({
            title: t("item.deleteConfirmTitle"),
            message: <span>{t("item.deleteConfirmMessage", { username: initialUsername })}</span>,
            actionColor: "danger",
            icon: "trash",
            confirmText: commonT("delete"),
        });

        if (isConfirmed) {
            await asyncDeleteUser.execute();
        }
    };

    const saveCredentials: SubmitHandler<FormData> = (formData) => {
        return tryHandleSubmit(async () => {
            databasesService.saveIntegrationsPostgreSqlCredentials(databaseName, formData.username, formData.password);
            reset(formData);
        });
    };

    return (
        <RichPanel>
            <Form onSubmit={handleSubmit(saveCredentials)}>
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {formValues.username || t("item.newName")}
                            {(formState.isDirty || isNew) && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    {hasDatabaseAdminAccess && (
                        <RichPanelActions>
                            {isNew ? (
                                <>
                                    <ButtonWithSpinner
                                        type="submit"
                                        variant="success"
                                        title={t("item.save")}
                                        isSpinning={formState.isSubmitting}
                                        icon="save"
                                    >
                                        {t("item.save")}
                                    </ButtonWithSpinner>
                                    <Button
                                        type="button"
                                        variant="secondary"
                                        title={t("item.discardTitle")}
                                        onClick={removeUser}
                                    >
                                        <Icon icon="cancel" />
                                        {t("item.discard")}
                                    </Button>
                                </>
                            ) : (
                                <ButtonWithSpinner
                                    type="button"
                                    variant="danger"
                                    title={t("item.deleteTitle")}
                                    onClick={onDeleteUser}
                                    isSpinning={asyncDeleteUser.loading}
                                    icon="trash"
                                />
                            )}
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                <Collapse in={isNew}>
                    <div>
                        <RichPanelDetails className="vstack gap-3 p-4">
                            <InputGroup className="vstack mb-1">
                                <FormLabel>{t("item.usernameLabel")}</FormLabel>
                                <FormInput
                                    control={control}
                                    name="username"
                                    type="text"
                                    placeholder={t("item.usernamePlaceholder")}
                                    autoComplete="off"
                                />
                            </InputGroup>
                            <InputGroup className="vstack">
                                <FormLabel>{t("item.passwordLabel")}</FormLabel>
                                <HStack className="gap-1">
                                    <div className="position-relative flex-grow">
                                        <FormInput
                                            control={control}
                                            name="password"
                                            type="password"
                                            placeholder={t("item.passwordPlaceholder")}
                                            passwordPreview
                                        />
                                    </div>
                                    <ButtonWithSpinner
                                        variant="secondary"
                                        type="button"
                                        title={t("item.generatePasswordTitle")}
                                        onClick={asyncGeneratePassword.execute}
                                        icon="random"
                                        isSpinning={asyncGeneratePassword.loading}
                                    >
                                        {t("item.generatePassword")}
                                    </ButtonWithSpinner>
                                    <Button
                                        variant="secondary"
                                        type="button"
                                        title={t("item.copyTitle")}
                                        onClick={() =>
                                            copyToClipboard.copy(formValues.password, t("item.passwordCopied"))
                                        }
                                    >
                                        <Icon icon="copy-to-clipboard" margin="m-0" />
                                    </Button>
                                </HStack>
                            </InputGroup>
                        </RichPanelDetails>
                    </div>
                </Collapse>
            </Form>
        </RichPanel>
    );
}

const schema = yup.object({
    username: yup.string().required(),
    password: yup.string().required(),
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;
