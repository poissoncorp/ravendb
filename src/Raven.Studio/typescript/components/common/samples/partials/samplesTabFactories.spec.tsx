import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { createMethodsTab, createSampleScriptsTab } from "./samplesTabFactories";
import { SamplesTabContentContext } from "./samplesTypes";

function ctx(overrides?: Partial<SamplesTabContentContext>): SamplesTabContentContext {
    return { onSelect: jest.fn(), onInsert: jest.fn(), search: "", ...overrides };
}

describe("samplesTabFactories", () => {
    it("createSampleScriptsTab returns a scripts tab without search", () => {
        const tab = createSampleScriptsTab([]);

        expect(tab.key).toBe("scripts");
        expect(tab.label).toBe("Sample scripts");
        expect(tab.icon).toBe("document");
        expect(tab.hasSearch).toBeUndefined();
        expect(typeof tab.content).toBe("function");
    });

    it("createSampleScriptsTab uses the provided label", () => {
        const tab = createSampleScriptsTab([], { label: "Sample object" });

        expect(tab.label).toBe("Sample object");
    });

    it("createSampleScriptsTab content renders the provided scripts", () => {
        const tab = createSampleScriptsTab([{ title: "T1", description: "", script: "x" }]);

        const { screen } = rtlRender(<>{tab.content(ctx())}</>);

        expect(screen.getByText("T1")).toBeInTheDocument();
    });

    it("createMethodsTab returns a methods tab with search", () => {
        const tab = createMethodsTab([]);

        expect(tab.key).toBe("methods");
        expect(tab.label).toBe("Methods");
        expect(tab.icon).toBe("indent");
        expect(tab.hasSearch).toBe(true);
        expect(tab.searchPlaceholder).toBe("Search methods");
        expect(typeof tab.content).toBe("function");
    });

    it("createMethodsTab content shows methods matching the search", () => {
        const tab = createMethodsTab([{ category: "Cat", methods: [{ signature: "sig()", description: "d" }] }]);

        const { screen } = rtlRender(<>{tab.content(ctx())}</>);

        expect(screen.getByText("sig()")).toBeInTheDocument();
    });

    it("createMethodsTab content hides methods not matching the search", () => {
        const tab = createMethodsTab([{ category: "Cat", methods: [{ signature: "sig()", description: "d" }] }]);

        const { screen } = rtlRender(<>{tab.content(ctx({ search: "zzz" }))}</>);

        expect(screen.queryByText("sig()")).not.toBeInTheDocument();
        expect(screen.getByText("No available methods match your search.")).toBeInTheDocument();
    });

    // one render per test: react-testing-library only clears the document between tests
    const keywordTab = () =>
        createMethodsTab([
            {
                category: "Cryptographic methods",
                methods: [{ signature: "sig()", description: "d", keywords: ["base64"] }],
            },
        ]);

    it("createMethodsTab content matches the search against keywords", () => {
        const { screen } = rtlRender(<>{keywordTab().content(ctx({ search: "base64" }))}</>);

        expect(screen.getByText("sig()")).toBeInTheDocument();
    });

    it("createMethodsTab content matches the search against the category name", () => {
        const { screen } = rtlRender(<>{keywordTab().content(ctx({ search: "crypto" }))}</>);

        expect(screen.getByText("sig()")).toBeInTheDocument();
    });

    const noticeTab = () =>
        createMethodsTab([{ category: "Cat", methods: [{ signature: "sig()", description: "d" }] }], {
            notice: <span>Heads up</span>,
        });

    it("createMethodsTab renders the notice when not searching", () => {
        const { screen } = rtlRender(<>{noticeTab().content(ctx())}</>);

        expect(screen.getByText("Heads up")).toBeInTheDocument();
    });

    it("createMethodsTab hides the notice while searching", () => {
        const { screen } = rtlRender(<>{noticeTab().content(ctx({ search: "sig" }))}</>);

        expect(screen.queryByText("Heads up")).not.toBeInTheDocument();
        expect(screen.getByText("sig()")).toBeInTheDocument();
    });

    it("createMethodsTab offers no example for a method that must not be called", () => {
        const tab = createMethodsTab([
            {
                category: "Not available",
                methods: [{ signature: "nope()", description: "d", sampleScript: "nope()", isUnavailable: true }],
            },
        ]);

        const { screen } = rtlRender(<>{tab.content(ctx())}</>);

        expect(screen.getByText("nope()")).toBeInTheDocument();
        expect(screen.queryByText("Example usage")).not.toBeInTheDocument();
    });

    it("createMethodsTab highlights examples as RQL by default", () => {
        const tab = createMethodsTab([
            { category: "Cat", methods: [{ signature: "sig()", description: "d", sampleScript: "from Orders" }] },
        ]);

        const { screen } = rtlRender(<>{tab.content(ctx())}</>);

        // "rql" is highlighted using the "sql" grammar, hence language-sql
        expect(screen.getByClassName("language-sql")).toBeInTheDocument();
    });

    it("createMethodsTab highlights examples using the requested language", () => {
        const tab = createMethodsTab(
            [{ category: "Cat", methods: [{ signature: "sig()", description: "d", sampleScript: "const a = 1;" }] }],
            { language: "javascript" }
        );

        const { screen } = rtlRender(<>{tab.content(ctx())}</>);

        expect(screen.getByClassName("language-javascript")).toBeInTheDocument();
    });
});
