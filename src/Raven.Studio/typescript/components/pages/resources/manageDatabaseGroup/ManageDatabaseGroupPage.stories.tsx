﻿import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { ManageDatabaseGroupPage } from "components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage";
import React from "react";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import licenseModel from "models/auth/licenseModel";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { ClusterStubs } from "test/stubs/ClusterStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Manage Database Group",
    component: ManageDatabaseGroupPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ManageDatabaseGroupPage>;

function commonInit() {
    const { accessManager } = mockStore;
    accessManager.with_securityClearance("ClusterAdmin");

    const { useClusterTopologyManager } = mockHooks;
    useClusterTopologyManager.with_Single();

    licenseModel.licenseStatus({
        HasDynamicNodesDistribution: true,
    } as any);
}

export const SingleNode: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    mockStore.databases.withDatabases([db.toDto()]);

    return <ManageDatabaseGroupPage db={db} />;
};

export const NotAllNodesUsed: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    // needed for old inner component (add node dialog)
    clusterTopologyManager.default.topology(ClusterStubs.clusterTopology());

    commonInit();

    const { useClusterTopologyManager } = mockHooks;

    useClusterTopologyManager.with_Cluster();
    mockStore.databases.with_Single();

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};

export const Cluster: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    mockStore.databases.with_Cluster();

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};

export const Sharded: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useClusterTopologyManager } = mockHooks;

    mockStore.databases.with_Sharded();
    useClusterTopologyManager.with_Cluster();

    const db = DatabasesStubs.shardedDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};

export const ClusterWithDeletion: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    mockStore.databases.with_Cluster((x) => {
        x.deletionInProgress = ["HARD", "SOFT"];
    });

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};

export const ClusterWithFailure: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    mockStore.databases.with_Cluster((x) => {
        x.nodes[0].lastStatus = "HighDirtyMemory";
        x.nodes[0].lastError = "This is some node error, which might be quite long in some cases...";
        x.nodes[0].responsibleNode = "X";
        x.nodes[0].type = "Rehab";
    });

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};

export const PreventDeleteIgnore: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    mockStore.databases.with_Single((x) => {
        x.lockMode = "PreventDeletesIgnore";
    });

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};

export const PreventDeleteError: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    mockStore.databases.with_Single((x) => {
        x.lockMode = "PreventDeletesError";
    });

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return <ManageDatabaseGroupPage db={db} />;
};