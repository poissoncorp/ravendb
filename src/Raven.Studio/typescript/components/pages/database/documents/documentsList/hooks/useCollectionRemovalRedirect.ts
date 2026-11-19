import appUrl from "common/appUrl";
import messagePublisher from "common/messagePublisher";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import router from "plugins/router";
import { useEffect, useRef } from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function useCollectionRemovalRedirect(databaseName: string, collectionName: string | null) {
    const t = useStudioTranslation("documentsList");
    const collectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const wasCollectionSeenRef = useRef(false);
    const isRemovalExpectedRef = useRef(false);

    useEffect(() => {
        if (collectionName === null) {
            return;
        }

        if (collectionNames.includes(collectionName)) {
            wasCollectionSeenRef.current = true;
            return;
        }

        if (wasCollectionSeenRef.current && !isRemovalExpectedRef.current) {
            messagePublisher.reportWarning(t("collectionRemoved", { collectionName }));
        }

        redirectToAllDocuments(databaseName);
    }, [collectionName, collectionNames, databaseName, t]);

    return {
        onCollectionDeletionStarted: () => {
            isRemovalExpectedRef.current = true;
        },
        onCollectionDeletionFailed: () => {
            isRemovalExpectedRef.current = false;
        },
    };
}

function redirectToAllDocuments(databaseName: string) {
    router.navigate(appUrl.forDocuments(null, databaseName), { replace: true, trigger: true });
}

export type CollectionDeletionCallbacks = ReturnType<typeof useCollectionRemovalRedirect>;
