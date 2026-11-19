import { useAppDispatch, useAppSelector } from "components/store";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import AceEditor from "components/common/ace/AceEditor";
import aceDiff from "common/helpers/text/aceDiff";
import ReactAce from "react-ace/lib/ace";
import { useRef, useEffect } from "react";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useEditGenAiTaskTests } from "../hooks/useEditGenAiTaskTests";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import EditGenAiTaskReadOnlyVirtualList from "./EditGenAiTaskReadOnlyVirtualList";
import SizeGetter from "components/common/SizeGetter";
import Badge from "react-bootstrap/Badge";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { EmptySet } from "components/common/EmptySet";
import AceUnifiedDiff from "components/common/ace/AceUnifiedDiff";
import { Switch } from "components/common/Checkbox";
import useBoolean from "components/hooks/useBoolean";
import genUtils from "common/generalUtils";
import AiTokensUsagePopoverBody from "components/common/AiTokensUsagePopoverBody";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function EditGenAiTaskTestResults() {
    const t = useStudioTranslation("editGenAiTask");
    const dispatch = useAppDispatch();

    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);
    const contextTest = useAppSelector(editGenAiTaskSelectors.contextTest);
    const modelInputTest = useAppSelector(editGenAiTaskSelectors.modelInputTest);

    const { handleContextTest, handleModelInputTest } = useEditGenAiTaskTests();

    return (
        <div className="vstack test-results">
            {currentStep === "context" && (
                <>
                    <div className="hstack mb-3 justify-content-between">
                        <h3 className="mb-0">
                            <Icon icon="test" />
                            {t("testResults.contextTitle")}
                        </h3>
                        <div className="hstack gap-2">
                            <ButtonWithSpinner
                                variant="primary"
                                className="rounded-pill text-nowrap"
                                onClick={handleContextTest}
                                icon="reset"
                                isSpinning={contextTest.status === "loading"}
                            >
                                {t("testResults.rerun")}
                            </ButtonWithSpinner>
                            <Button
                                variant="secondary"
                                className="rounded-pill"
                                onClick={() => dispatch(editGenAiTaskActions.isTestOpenSet(false))}
                            >
                                <Icon icon="cancel" />
                                {t("testResults.close")}
                            </Button>
                        </div>
                    </div>
                    {contextTest.data?.length === 0 ? (
                        <EmptySet>{t("testResults.noResults")}</EmptySet>
                    ) : (
                        <EditGenAiTaskReadOnlyVirtualList data={contextTest.data} name="playgroundContexts" />
                    )}
                </>
            )}
            {currentStep === "modelInput" && (
                <>
                    <div className="hstack mb-3 justify-content-between">
                        <h3 className="mb-0">
                            <Icon icon="test" />
                            {t("testResults.modelTitle")}
                        </h3>
                        <div className="hstack gap-2">
                            <ButtonWithSpinner
                                variant="primary"
                                className="rounded-pill text-nowrap"
                                icon="reset"
                                isSpinning={modelInputTest.status === "loading"}
                                onClick={handleModelInputTest}
                            >
                                {t("testResults.rerun")}
                            </ButtonWithSpinner>
                            <Button
                                variant="secondary"
                                className="rounded-pill"
                                onClick={() => dispatch(editGenAiTaskActions.isTestOpenSet(false))}
                            >
                                <Icon icon="cancel" />
                                {t("testResults.close")}
                            </Button>
                        </div>
                    </div>
                    <ModelUsage />
                    {modelInputTest.data?.length === 0 ? (
                        <EmptySet>{t("testResults.noResults")}</EmptySet>
                    ) : (
                        <EditGenAiTaskReadOnlyVirtualList data={modelInputTest.data} name="playgroundModelOutputs" />
                    )}
                </>
            )}
            {currentStep === "updateScript" && <UpdateScriptResult />}
        </div>
    );
}

function ModelUsage() {
    const t = useStudioTranslation("editGenAiTask");
    const modelUsage = useAppSelector(editGenAiTaskSelectors.modelUsage);

    if (modelUsage.status !== "success") {
        return null;
    }

    return (
        <div>
            <PopoverWithHoverWrapper
                message={
                    <AiTokensUsagePopoverBody
                        prompt={modelUsage.data.promptTokens}
                        completion={modelUsage.data.completionTokens}
                        cached={modelUsage.data.cachedTokens}
                        reasoning={modelUsage.data.reasoningTokens}
                        total={modelUsage.data.totalTokens}
                    />
                }
                placement="left"
            >
                <Badge bg="info">
                    <Icon icon="info" />
                    {t("testResults.tokensUsed", { tokens: genUtils.formatAiTokens(modelUsage.data.totalTokens) })}
                </Badge>
            </PopoverWithHoverWrapper>
        </div>
    );
}

function UpdateScriptResult() {
    const t = useStudioTranslation("editGenAiTask");
    const dispatch = useAppDispatch();

    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);
    const { handleUpdateScriptTest } = useEditGenAiTaskTests();

    const { value: isSplitDiff, toggle: toggleIsSplitDiff } = useBoolean(false);

    return (
        <>
            <div className="hstack mb-3 justify-content-between">
                <h3 className="mb-0">
                    <Icon icon="test" />
                    {t("testResults.updateScriptTitle")}
                </h3>
                <div className="hstack gap-2">
                    <ButtonWithSpinner
                        variant="primary"
                        className="rounded-pill text-nowrap"
                        icon="reset"
                        isSpinning={updateScriptTest.status === "loading"}
                        onClick={handleUpdateScriptTest}
                    >
                        {t("testResults.rerun")}
                    </ButtonWithSpinner>
                    <Button
                        variant="secondary"
                        className="rounded-pill"
                        onClick={() => dispatch(editGenAiTaskActions.isTestOpenSet(false))}
                    >
                        <Icon icon="cancel" />
                        {t("testResults.close")}
                    </Button>
                </div>
            </div>
            {updateScriptTest.data == null ? (
                <EmptySet>{t("testResults.noResults")}</EmptySet>
            ) : (
                <div className="flex-grow-1 update-script-result">
                    <div className="hstack justify-content-between">
                        <div>{t("testResults.testResult")}</div>
                        <Switch selected={isSplitDiff} toggleSelection={toggleIsSplitDiff} color="primary">
                            {t("testResults.splitView")}
                        </Switch>
                    </div>
                    <SizeGetter
                        isHeighRequired
                        render={({ height }) =>
                            isSplitDiff ? (
                                <UpdateScriptDiffSplit height={height} />
                            ) : (
                                <UpdateScriptDiffUnified height={height} />
                            )
                        }
                    />
                </div>
            )}
        </>
    );
}

function UpdateScriptDiffSplit({ height }: { height: number }) {
    const t = useStudioTranslation("editGenAiTask");
    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);

    const oldDoc = useAppSelector(editGenAiTaskSelectors.updateScriptDocumentInput);
    const newDoc = updateScriptTest.data ?? "";

    const oldDocRef = useRef<ReactAce>(null);
    const newDocRef = useRef<ReactAce>(null);

    useEffect(() => {
        if (!oldDocRef.current || !newDocRef.current) {
            return;
        }

        // We have different ace versions, lets just use 'any' here instead of editing aceDiff class
        const aceDiffC = new aceDiff(oldDocRef.current.editor as any, newDocRef.current.editor as any, false);
        aceDiffC.refresh(false);
    }, [oldDoc, newDoc]);

    if (oldDoc.status !== "success") {
        return null;
    }

    return (
        <div className="vstack gap-2 update-script-result">
            <div className="diff-wrapper">
                <div className="diff-header">{t("testResults.originalDocument")}</div>
                <AceEditor
                    aceRef={oldDocRef}
                    value={oldDoc.data}
                    mode="json"
                    height={`${height / 2 - 100}px`}
                    readOnly
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                />
            </div>
            <div className="diff-wrapper">
                <div className="diff-header">{t("testResults.modifiedDocument")}</div>
                <AceEditor
                    aceRef={newDocRef}
                    value={newDoc}
                    mode="json"
                    height={`${height / 2 - 100}px`}
                    readOnly
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                />
            </div>
        </div>
    );
}

function UpdateScriptDiffUnified({ height }: { height: number }) {
    const t = useStudioTranslation("editGenAiTask");
    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);

    const oldDoc = useAppSelector(editGenAiTaskSelectors.updateScriptDocumentInput);
    const newDoc = updateScriptTest.data ?? "";

    if (oldDoc.status !== "success") {
        return null;
    }

    return (
        <div className="diff-wrapper">
            <div className="diff-header">{t("testResults.modifiedDocument")}</div>
            <AceUnifiedDiff
                value1={oldDoc.data}
                value2={newDoc}
                height={`${height - 100}px`}
                mode="json"
                actions={[{ component: <AceEditor.FullScreenAction /> }]}
            />
        </div>
    );
}
