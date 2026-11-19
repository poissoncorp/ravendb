import React from "react";
import Spinner from "react-bootstrap/Spinner";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function LoadingView() {
    const t = useStudioTranslation("commonComponents");

    return (
        <div className="d-flex justify-content-center align-items-center flex-column gap-3 mt-4" data-testid="loader">
            <Spinner className="spinner-gradient" />
            <h3>{t("loadingView.loading")}</h3>
        </div>
    );
}
