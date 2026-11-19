import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { FeatureAvailabilityData, FeatureAvailabilityFootnote } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { aiConnectionStringUtils } from "./editForms/aiConnectionStringUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export interface ConnectionStringsLicenseFeatures {
    hasRavenEtl: boolean;
    hasSqlEtl: boolean;
    hasSnowflakeEtl: boolean;
    hasOlapEtl: boolean;
    hasElasticSearchEtl: boolean;
    hasQueueEtl: boolean;
    hasEmbeddingsGeneration: boolean;
}

interface ConnectionStringsLicense {
    features: ConnectionStringsLicenseFeatures;
    hasNone: boolean;
    hasAll: boolean;
    featureAvailability: FeatureAvailabilityData[];
}

export default function useConnectionStringsLicense(): ConnectionStringsLicense {
    const t = useStudioTranslation("connectionStrings");
    const hasRavenEtl = useAppSelector(licenseSelectors.statusValue("HasRavenEtl"));
    const hasSqlEtl = useAppSelector(licenseSelectors.statusValue("HasSqlEtl"));
    const hasSnowflakeEtl = useAppSelector(licenseSelectors.statusValue("HasSnowflakeEtl"));
    const hasOlapEtl = useAppSelector(licenseSelectors.statusValue("HasOlapEtl"));
    const hasElasticSearchEtl = useAppSelector(licenseSelectors.statusValue("HasElasticSearchEtl"));
    const hasQueueEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasEmbeddingsGeneration = useAppSelector(licenseSelectors.statusValue("HasEmbeddingsGeneration"));

    const embeddedConnectorFootnote: FeatureAvailabilityFootnote = {
        title: t("featureAvailability.availableConnectors"),
        items: aiConnectorOptions.filter((x) => x.value === "embeddedSettings"),
    };

    const allConnectorsFootnote: FeatureAvailabilityFootnote = {
        title: t("featureAvailability.availableConnectors"),
        items: aiConnectorOptions,
    };

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.ai"),
            featureIcon: "ai-etl",
            community: { value: true, footnote: embeddedConnectorFootnote },
            professional: { value: true, footnote: embeddedConnectorFootnote },
            enterprise: { value: true, footnote: allConnectorsFootnote },
            quill: { value: true, footnote: allConnectorsFootnote },
        },
        {
            featureName: "RavenDB ETL",
            featureIcon: "ravendb-etl",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: "SQL ETL",
            featureIcon: "sql-etl",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: "Snowflake ETL",
            featureIcon: "snowflake-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: "OLAP ETL",
            featureIcon: "olap-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: "Elasticsearch ETL",
            featureIcon: "elastic-search-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: "Kafka ETL",
            featureIcon: "kafka-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: "RabbitMQ ETL",
            featureIcon: "rabbitmq-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.azureQueueStorageEtl"),
            featureIcon: "azure-queue-storage-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.amazonSqsEtl"),
            featureIcon: "amazon-sqs-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.azureServiceBusEtl"),
            featureIcon: "azure-service-bus",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];

    // Don't include HasEmbeddingsGeneration because user can create connection strings for embedded model
    const allFeatures = [hasRavenEtl, hasSqlEtl, hasSnowflakeEtl, hasOlapEtl, hasElasticSearchEtl, hasQueueEtl];
    const hasNone = allFeatures.every((x) => !x);
    const hasAll = allFeatures.every((x) => x);

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "ai-etl").featureName,
                value: true,
                footnote: hasEmbeddingsGeneration ? allConnectorsFootnote : embeddedConnectorFootnote,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "ravendb-etl").featureName,
                value: hasRavenEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "sql-etl").featureName,
                value: hasSqlEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "snowflake-etl").featureName,
                value: hasSnowflakeEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "olap-etl").featureName,
                value: hasOlapEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "elastic-search-etl").featureName,
                value: hasElasticSearchEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "kafka-etl").featureName,
                value: hasQueueEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "rabbitmq-etl").featureName,
                value: hasQueueEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "azure-queue-storage-etl")
                    .featureName,
                value: hasQueueEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "amazon-sqs-etl").featureName,
                value: hasQueueEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "azure-service-bus").featureName,
                value: hasQueueEtl,
            },
        ],
    });

    return {
        features: {
            hasRavenEtl,
            hasSqlEtl,
            hasSnowflakeEtl,
            hasOlapEtl,
            hasElasticSearchEtl,
            hasQueueEtl,
            hasEmbeddingsGeneration,
        },
        hasNone,
        hasAll,
        featureAvailability,
    };
}

const aiConnectorOptions = aiConnectionStringUtils.allConnectorOptions;
