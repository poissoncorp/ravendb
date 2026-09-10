import React from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";

interface InsertButtonProps {
    onInsert: () => void;
}

/**
 * Used by the methods reference, where the snippets are one-liners that illustrate a single call.
 * Replacing the whole editor with such a snippet is never what the user wants, so it inserts at the caret.
 * The whole-content replacement lives on {@link LoadButton}, used by the sample scripts tab.
 */
export default function InsertButton({ onInsert }: InsertButtonProps) {
    return (
        <Button variant="link" className="text-emphasis" title="Insert at cursor" onClick={onInsert}>
            <Icon icon="arrow-right" margin="me-1" />
            Insert
        </Button>
    );
}
