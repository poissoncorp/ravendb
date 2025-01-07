import { composeStories } from "@storybook/react";
import * as stories from "./DocumentIdentities.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";

const { DocumentIdentitiesStory } = composeStories(stories);

const selectors = {
    newIdentityBtn: "Add new identity",
    tableEditColumn: "Edit",
};

describe("DocumentIdentities", () => {
    it("should be disabled 'Add New Identity' button when database access is read-only", () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseRead" />);

        const addNewIdentityBtn = screen.getByRole("button", { name: selectors.newIdentityBtn });
        expect(addNewIdentityBtn).toBeInTheDocument();
        expect(addNewIdentityBtn).toBeDisabled();
    });

    it("should be enabled 'Add New Identity' button when database access is read-write", () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseReadWrite" />);

        const addNewIdentityBtn = screen.getByRole("button", { name: selectors.newIdentityBtn });
        expect(addNewIdentityBtn).toBeInTheDocument();
        expect(addNewIdentityBtn).not.toBeDisabled();
    });

    it("should not display the edit column in table when database access is read-only", () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseRead" />);

        expect(screen.queryByText(selectors.tableEditColumn)).not.toBeInTheDocument();
    });

    it("should display the edit column in table when database access is read-write", () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseReadWrite" />);

        expect(screen.queryByText(selectors.tableEditColumn)).toBeInTheDocument();
    });
});
