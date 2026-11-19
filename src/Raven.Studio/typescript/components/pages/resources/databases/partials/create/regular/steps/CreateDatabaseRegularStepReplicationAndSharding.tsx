import { FormInput, FormRange, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useRavenLink } from "components/hooks/useRavenLink";
import classNames from "classnames";
import { createDatabaseRegularDataUtils } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularDataUtils";
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const shardingImg = require("Content/img/createDatabase/sharding.svg");

export default function CreateDatabaseRegularStepReplicationAndSharding() {
    const t = useStudioTranslation("databases");
    const hasDynamicNodesDistribution = useAppSelector(licenseSelectors.statusValue("HasDynamicNodesDistribution"));
    const maxReplicationFactorForSharding =
        useAppSelector(licenseSelectors.statusValue("MaxReplicationFactorForSharding")) ?? Infinity;

    const { appUrl } = useAppUrls();
    const docsShardingLink = useRavenLink({
        hash: "VKF52P",
    });

    const { control, setValue, watch } = useFormContext<CreateDatabaseRegularFormData>();
    const {
        basicInfoStep: { isEncrypted },
        replicationAndShardingStep: { isSharded, shardsCount, replicationFactor, isManualReplication },
    } = useWatch({
        control,
    });

    const nodeTagsCount = useAppSelector(clusterSelectors.allNodes).length;
    const availableNodesCount = nodeTagsCount || 1;

    const maxReplicationFactor = isSharded
        ? Math.min(maxReplicationFactorForSharding, availableNodesCount)
        : availableNodesCount;

    const isReplicationFactorVisible = !isManualReplication || isSharded;
    const isReplicationFactorWarning = isSharded && maxReplicationFactorForSharding < availableNodesCount;
    const isSingleNodeOnly = availableNodesCount === 1;

    const isNotBootstrapped = nodeTagsCount === 0;
    const isManualReplicationRequiredForEncryption =
        createDatabaseRegularDataUtils.getIsManualReplicationRequiredForEncryption(nodeTagsCount, isEncrypted);

    // Disable prefixes for shards when sharding is disabled
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "replicationAndShardingStep.isSharded" && !values.replicationAndShardingStep.isSharded) {
                setValue("replicationAndShardingStep.isPrefixesForShards", false, { shouldValidate: true });
            }
        });

        return () => unsubscribe();
    }, [setValue, watch]);

    useEffect(() => {
        if (isSharded && replicationFactor > maxReplicationFactorForSharding) {
            setValue("replicationAndShardingStep.replicationFactor", maxReplicationFactorForSharding);
        }
    }, [replicationFactor, isSharded, maxReplicationFactorForSharding, setValue]);

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={shardingImg} alt={t("createDatabase.replicationAndSharding.imageAlt")} className="step-img" />
            </div>

            <h2 className="text-center">{t("createDatabase.steps.replicationAndSharding")}</h2>

            <Row>
                <Col lg={{ span: 8, offset: 2 }} className="text-center">
                    <p>{t("createDatabase.replicationAndSharding.intro")}</p>
                    <div className="mb-3">
                        <span>
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        <p>
                                            <StudioTrans
                                                ns="databases"
                                                i18nKey="createDatabase.replicationAndSharding.shardingInfo"
                                                components={{
                                                    strong: <strong className="text-shard" />,
                                                    shardingIcon: <Icon icon="sharding" margin="m-0" />,
                                                    shardIcon: <Icon icon="shard" margin="m-0" />,
                                                }}
                                            />
                                        </p>
                                        <p>
                                            <StudioTrans
                                                ns="databases"
                                                i18nKey="createDatabase.replicationAndSharding.shardingBenefits"
                                                components={{ strong: <strong /> }}
                                            />
                                        </p>
                                        <a href={docsShardingLink}>
                                            {t("createDatabase.replicationAndSharding.learnMore")}{" "}
                                            <Icon icon="newtab" margin="m-0" />
                                        </a>
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="m-0" />
                            </PopoverWithHoverWrapper>{" "}
                            {t("createDatabase.replicationAndSharding.whatIsSharding")}
                        </span>
                    </div>
                </Col>
            </Row>

            <Row>
                <Col lg={{ offset: 1, span: 10 }}>
                    <Row className="pt-2">
                        <Col sm="6" className="d-flex gap-1 align-items-center">
                            <PopoverWithHoverWrapper
                                message={
                                    <StudioTrans
                                        ns="databases"
                                        i18nKey="createDatabase.replicationAndSharding.addMoreNodes"
                                        components={{
                                            strong: <strong className="text-node" />,
                                            icon: <Icon icon="node" margin="m-0" />,
                                            a: <a href={appUrl.forCluster()} />,
                                        }}
                                    />
                                }
                            >
                                <Icon id="ReplicationInfo" icon="info" color="info" margin="m-0" />
                            </PopoverWithHoverWrapper>{" "}
                            {t("createDatabase.replicationAndSharding.availableNodes")}{" "}
                            <Icon icon="node" color="node" margin="ms-1" />{" "}
                            <strong className={classNames({ "text-warning": isReplicationFactorWarning })}>
                                {maxReplicationFactor}{" "}
                                {isReplicationFactorWarning && (
                                    <PopoverWithHoverWrapper
                                        message={
                                            <LicenseRestrictedMessage>
                                                <StudioTrans
                                                    ns="databases"
                                                    i18nKey="createDatabase.replicationAndSharding.replicationFactorLicenseLimit"
                                                    options={{ limit: maxReplicationFactorForSharding }}
                                                    components={{ strong: <strong /> }}
                                                />
                                            </LicenseRestrictedMessage>
                                        }
                                    >
                                        <Icon icon="warning" margin="m-0" />
                                    </PopoverWithHoverWrapper>
                                )}
                            </strong>
                        </Col>
                        <Col sm="6">
                            <FormSwitch
                                control={control}
                                name="replicationAndShardingStep.isSharded"
                                color="shard"
                                className="mt-1"
                            >
                                <StudioTrans
                                    ns="databases"
                                    i18nKey="createDatabase.replicationAndSharding.enableSharding"
                                    components={{
                                        strong: <strong className="text-shard" />,
                                        icon: <Icon icon="sharding" margin="m-0" />,
                                    }}
                                />
                            </FormSwitch>
                        </Col>
                    </Row>
                    <Row className="pt-2">
                        <Col sm="6">
                            <Collapse in={isReplicationFactorVisible}>
                                <div>
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: isSingleNodeOnly,
                                            message: (
                                                <span>
                                                    <StudioTrans
                                                        ns="databases"
                                                        i18nKey="createDatabase.replicationAndSharding.singleNodeOnly"
                                                        components={{ a: <a href={appUrl.forCluster()} /> }}
                                                    />
                                                </span>
                                            ),
                                        }}
                                        popoverPlacement="top"
                                        className="w-100"
                                    >
                                        <InputGroup>
                                            <InputGroup.Text>
                                                {t("createDatabase.replicationAndSharding.replicationFactor")}
                                            </InputGroup.Text>
                                            <FormInput
                                                type="number"
                                                control={control}
                                                name="replicationAndShardingStep.replicationFactor"
                                                className="replication-input"
                                                min="1"
                                                max={maxReplicationFactor}
                                                disabled={isSingleNodeOnly}
                                            />
                                        </InputGroup>
                                    </ConditionalPopover>
                                    {!isSingleNodeOnly && (
                                        <FormRange
                                            control={control}
                                            name="replicationAndShardingStep.replicationFactor"
                                            min="1"
                                            max={maxReplicationFactor}
                                            className="mt-3"
                                        />
                                    )}
                                </div>
                            </Collapse>
                        </Col>
                        <Col sm="6">
                            <Collapse in={isSharded}>
                                <div>
                                    <InputGroup>
                                        <InputGroup.Text>
                                            {t("createDatabase.replicationAndSharding.numberOfShards")}
                                        </InputGroup.Text>
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="replicationAndShardingStep.shardsCount"
                                            className="replication-input"
                                            min="1"
                                            max="100"
                                        />
                                    </InputGroup>
                                    <FormSwitch
                                        control={control}
                                        name="replicationAndShardingStep.isPrefixesForShards"
                                        color="primary"
                                        className="mt-3"
                                    >
                                        <StudioTrans
                                            ns="databases"
                                            i18nKey="createDatabase.replicationAndSharding.prefixesForShards"
                                            components={{
                                                strong: <strong />,
                                                br: <br />,
                                                small: <small className="text-muted" />,
                                            }}
                                        />
                                    </FormSwitch>
                                </div>
                            </Collapse>
                        </Col>
                    </Row>
                    <RichAlert variant="info" className="mt-4">
                        <Collapse in={isSharded}>
                            <div>
                                <StudioTrans
                                    ns="databases"
                                    i18nKey="createDatabase.replicationAndSharding.shardsSummary"
                                    options={{ count: shardsCount }}
                                    components={{ strong: <strong />, icon: <Icon icon="shard" margin="m-0" /> }}
                                />
                                <br />
                            </div>
                        </Collapse>
                        {replicationFactor > 1 ? (
                            <StudioTrans
                                ns="databases"
                                i18nKey={
                                    isSharded
                                        ? "createDatabase.replicationAndSharding.eachShardReplicationSummary"
                                        : "createDatabase.replicationAndSharding.dataReplicationSummary"
                                }
                                options={{ count: replicationFactor }}
                                components={{ strong: <strong />, icon: <Icon icon="node" margin="m-0" /> }}
                            />
                        ) : (
                            t("createDatabase.replicationAndSharding.noReplicationSummary")
                        )}
                    </RichAlert>
                </Col>
            </Row>

            <Row className="mt-4">
                <Col lg={{ offset: 1, span: 5 }}>
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasDynamicNodesDistribution,
                            message: (
                                <LicenseRestrictedMessage>
                                    <StudioTrans
                                        ns="databases"
                                        i18nKey="createDatabase.replicationAndSharding.dynamicDistributionLicense"
                                        components={{ strong: <strong className="text-info" /> }}
                                    />
                                </LicenseRestrictedMessage>
                            ),
                        }}
                        popoverPlacement="top"
                    >
                        <FormSwitch
                            control={control}
                            name="replicationAndShardingStep.isDynamicDistribution"
                            color="primary"
                            disabled={!hasDynamicNodesDistribution}
                        >
                            <StudioTrans
                                ns="databases"
                                i18nKey="createDatabase.replicationAndSharding.dynamicDistribution"
                                components={{ br: <br />, small: <small className="text-muted" /> }}
                            />
                        </FormSwitch>
                    </ConditionalPopover>
                </Col>
                <Col lg={{ span: 5 }}>
                    <ConditionalPopover
                        conditions={[
                            {
                                isActive: isNotBootstrapped,
                                message: (
                                    <span>
                                        <StudioTrans
                                            ns="databases"
                                            i18nKey="createDatabase.replicationAndSharding.bootstrapCluster"
                                            components={{ a: <a href={appUrl.forCluster()} /> }}
                                        />
                                    </span>
                                ),
                            },
                            {
                                isActive: isManualReplicationRequiredForEncryption,
                                message: t(
                                    "createDatabase.replicationAndSharding.manualReplicationRequiredForEncryption"
                                ),
                            },
                        ]}
                        popoverPlacement="top"
                    >
                        <FormSwitch
                            control={control}
                            name="replicationAndShardingStep.isManualReplication"
                            color="primary"
                            disabled={isNotBootstrapped || isManualReplicationRequiredForEncryption}
                        >
                            <StudioTrans
                                ns="databases"
                                i18nKey="createDatabase.replicationAndSharding.manualReplication"
                                components={{ br: <br />, small: <small className="text-muted" /> }}
                            />
                        </FormSwitch>
                    </ConditionalPopover>
                </Col>
            </Row>
        </div>
    );
}
