import React from "react";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export interface IndexInfoForDelete {
    indexName: string;
    reduceOutputCollection: string;
    referenceCollection: string;
}

export interface DeleteIndexesConfirmBodyProps {
    lockedIndexNames: string[];
    indexesInfoForDelete: IndexInfoForDelete[];
}

export default function DeleteIndexesConfirmBody({
    lockedIndexNames,
    indexesInfoForDelete,
}: DeleteIndexesConfirmBodyProps) {
    const t = useStudioTranslation("indexesShared");
    const showWarning = indexesInfoForDelete.some((x) => x.reduceOutputCollection);

    return (
        <div>
            {indexesInfoForDelete.length > 0 && (
                <div>
                    <p>
                        <StudioTrans
                            ns="indexesShared"
                            i18nKey="deleteConfirm.intro"
                            components={{ strong: <strong /> }}
                            options={{ count: indexesInfoForDelete.length }}
                        />
                    </p>
                    <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                        {indexesInfoForDelete.map((x) => (
                            <li key={x.indexName}>
                                <strong>{x.indexName}</strong>
                                {x.reduceOutputCollection && (
                                    <small className="ms-1">
                                        {t("deleteConfirm.reduceResultsCollections", {
                                            reduceOutputCollection: x.reduceOutputCollection,
                                            referenceCollection: x.referenceCollection,
                                            context: x.referenceCollection ? "withReference" : undefined,
                                        })}
                                    </small>
                                )}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
            {lockedIndexNames.length > 0 && (
                <div>
                    <p>{t("deleteConfirm.skippingLocked")}</p>
                    <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                        {lockedIndexNames.map((x) => (
                            <li key={x}>
                                <strong>{x}</strong>
                            </li>
                        ))}
                    </ul>
                </div>
            )}
            {showWarning && (
                <>
                    <hr />
                    <RichAlert variant="warning" className="align-items-start">
                        <div>
                            {t("deleteConfirm.reduceResultsWarning")}
                            <ul className="mt-2 mb-2 ps-3">
                                <li>
                                    <StudioTrans
                                        ns="indexesShared"
                                        i18nKey="deleteConfirm.reduceResultsNote"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="indexesShared"
                                        i18nKey="deleteConfirm.reduceResultsImportant"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                            </ul>
                        </div>
                    </RichAlert>
                </>
            )}
        </div>
    );
}
