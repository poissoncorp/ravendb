export default {
    title_new: "New CDC Sink task",
    title_edit: "Edit CDC Sink task",
    loadError: "Unable to load configuration",
    rawView: {
        switchLabel: "Raw config",
        formDataConversionError:
            "The current form data cannot be converted. Please fix validation errors and try again.",
        rawConfigConversionError: "The raw configuration cannot be converted. Please fix the JSON and try again.",
        validationErrorsWarning: "The configuration has validation errors. Please fix them in the form view.",
    },
    about: {
        heading: "About this view",
        description: "Get additional info on this feature",
        intro: "A <strong>CDC Sink task</strong> consumes <strong>Change Data Capture</strong> events from tables in an external relational database, including inserts, updates, and deletes. Each captured change is applied as a document operation in RavenDB. Supported source databases include:<br/><strong>PostgreSQL, SQL Server, and MySQL / MariaDB</strong>.",
        configurationIncludes: "Task configuration includes:",
        connectionStringItem:
            "Provide a SQL <a>connection string</a> that points to the source database where CDC is enabled.",
        discoverTablesItem:
            'Click "Discover tables" under "Schema Explorer" to <strong>fetch the available tables</strong> from the source database.',
        configureTablesItem: "Configure how each source table is converted to documents:",
        targetCollectionItem:
            "<strong>Target collection</strong><br/>Set the target collection where documents generated from rows in the source table will be stored.",
        fieldMappingsItem:
            "<strong>Field mappings</strong><br/>Map source columns to document fields and choose how to store each value:<br/>as a regular property, parsed JSON, or a RavenDB attachment.",
        documentIdItem:
            "<strong>Document ID</strong><br/>Specify the source primary key column or columns used to uniquely identify a row.<br/>Their values are used to derive the RavenDB document ID.",
        relatedTablesItem:
            "<strong>Related tables</strong><br/>When a source table references another table, you can configure how the related row is represented in the RavenDB document:",
        embeddedTableItem:
            "<strong>Embedded table</strong> - include the related row’s data in the main document as an embedded object.",
        linkedTableItem:
            "<strong>Linked table</strong> - store the related row as a separate related document.<br/>The related document ID is derived from the linked collection name you specify and the join column values.",
        onDeleteItem:
            "<strong>Advanced: On-delete behavior</strong><br/>Choose whether deletes from the source are ignored, propagated as document deletions, or transformed by a custom patch script.",
        patchScriptsItem:
            "<strong>Advanced: Patch scripts</strong><br/>You can define JavaScript snippets that customize how changes are applied before they are written to RavenDB. Use a <em>patch script</em> to transform documents on inserts and updates, and a <em>delete patch script</em> to customize what happens when source rows are deleted.",
        testing:
            "<strong>Testing:</strong><br/>You can test a table configuration to preview the document that will be generated from a sample row before saving the task.",
        initialLoad:
            "<strong>Initial load:</strong><br/>By default, the task takes a snapshot of existing rows before streaming new changes.<br/>Enable <strong>Skip initial load</strong> to stream only new changes.",
        ongoingProcessing:
            "<strong>Ongoing processing:</strong><br/>Once the task is active, every committed change in the source tables is captured and applied to RavenDB. Progress is tracked using the last processed checkpoint.",
        docsLink: "Docs - CDC Sink Overview",
    },
    featureAvailability: {
        cdcSinkTask: "CDC Sink task",
    },
    basic: {
        title: "Configure basic settings",
        description: "Specify the task name, select a connection string, and configure task options.",
        taskName: "Task Name",
        taskNamePlaceholder: "My CDC Sink task",
        taskState: "Task State",
        stateEnabled: "Enabled",
        stateDisabled: "Disabled",
        postgresInfo:
            "For PostgreSQL connections, you can specify a custom publication name and replication slot name. Leave these fields empty to let the server auto-generate both values.",
        publicationName: "Publication Name",
        slotName: "Slot Name",
        skipInitialLoad: "Skip initial load",
        skipInitialLoadInfo:
            "By default, the task takes a snapshot of existing rows before streaming new changes.<br/>Enable this option to stream only new changes.",
    },
    discovery: {
        title: "Schema Explorer",
        description:
            'Discover existing tables from the configured connection.<br/>After tables are shown, select the ones you want and click "Configure selected tables" to add them below.',
        connectionStringRequired: "A connection string is required to discover tables.",
        discoverTablesInfo: "Click to fetch tables from the source database.",
        discoverTables: "Discover tables",
        schemasInfo:
            "Leave schemas empty to fetch tables from the default schema of the configured connection.<br/>Add one or more schemas to fetch tables from specific schemas.",
        schemas: "Schemas",
        addSchema: "Add schema",
        noSchemas: "No schemas added",
        discover: "Discover",
        notRequested: 'Click "Discover tables" to fetch available tables from the source database.',
        loadError: "Unable to discover tables",
        availableTables: "Available tables",
        selectedCount_one: "<b>{{count}}</b> selected",
        selectedCount_other: "<b>{{count}}</b> selected",
        configureSelectedTables: "Configure selected tables",
        unavailableTables: "Unavailable tables",
        tableWarnings: "Table warnings",
        cdcSetupRequired: "CDC setup required",
        cdcNotEnabled: "CDC is not enabled. Ask a database administrator to enable CDC for this table.",
        tableNotSupported: "This table cannot be configured for CDC.",
    },
    columns: {
        tableName: "Table name",
        primaryKeys: "Primary keys",
        columnsCount: "Columns count",
        warnings: "Warnings",
        error: "Error",
    },
    tables: {
        title: "Configured Tables",
        description: "Configure how source tables are mapped to target collections.",
        missingRelatedTables_one:
            "Linked tables reference {{count}} source table that is not configured as a root table. Related documents for the referenced table will not be created.",
        missingRelatedTables_other:
            "Linked tables reference {{count}} source tables that are not configured as root tables. Related documents for the referenced tables will not be created.",
        addMissingRootTables_one: "Add root table",
        addMissingRootTables_other: "Add {{count}} root tables",
        addRootTablesTitle: "Add root tables",
        addSelectedRootTables_one: "Add {{count}} root table",
        addSelectedRootTables_other: "Add {{count}} root tables",
    },
    tablesExplorer: {
        title: "Tables",
        addRootTable: "Add new root table",
        collapseAll: "Collapse all tables",
        expandAll: "Expand all tables",
        settings: "Table settings",
        expandFieldMapping: "Expand field mapping",
        filterPlaceholder: "Filter tables",
        noTables: "Use the Schema Explorer to discover existing tables or add new manually.",
        noMatchingTables: "No tables match the filter.",
        defaultSchema: "(default schema)",
        unassignedTable: "Unassigned table",
        collapseTable: "Collapse table",
        expandTable: "Expand table",
        tableActions: "Table actions",
        addEmbeddedTable: "Add new embedded table",
        addLinkedTable: "Add new linked table",
        enable: "Enable",
        disable: "Disable",
        remove: "Remove",
    },
    tableEditor: {
        noActiveTable: "Select a table to view its configuration details.",
        testInfo: "Preview documents generated from sample rows.",
        test: "Test",
        sourceSchema: "Source schema",
        sourceSchemaPlaceholder: "Select or enter source schema",
        sourceTable: "Source table",
        sourceTablePlaceholder: "Select or enter source table",
        targetProperty: "Target property",
        primaryKeyColumns: "Primary key columns",
        addPrimaryKeyColumn: "Add primary key column",
        joinColumns: "Join columns",
        addJoinColumn: "Add join column",
        advancedSettings: "Advanced settings",
        root: {
            targetCollection: "Target collection",
            targetCollectionInfo:
                "The RavenDB collection where documents generated from rows in the source table will be stored.",
            primaryKeyColumnsInfo:
                "Columns that uniquely identify each source row. Their values are used to derive the RavenDB document ID.",
        },
        embedded: {
            targetPropertyInfo: "The RavenDB document field where the embedded related data will be stored.",
            relationType: "Relation type",
            relationTypeInfo:
                "How related rows are embedded in the parent document:<br/><strong>Array</strong> - multiple rows stored as an array of objects.<br/><strong>Map</strong> - multiple rows stored as a keyed object (dictionary).<br/><strong>Value</strong> - a single row stored directly as an object.",
            relationTypeArray: "Array",
            relationTypeMap: "Map",
            relationTypeValue: "Value",
            primaryKeyColumnsInfo: "Columns that uniquely identify rows in this related table.",
            joinColumnsInfo: "Columns used to match rows in this related table with rows from the root table.",
            caseSensitiveKeys: "Case-sensitive keys",
            caseSensitiveKeysInfo:
                "When enabled, string values in primary key columns are compared case-sensitively when matching embedded items. Applies only to <strong>Array</strong> and <strong>Map</strong> embedded tables.",
        },
        linked: {
            targetPropertyInfo:
                "The RavenDB document field that will hold the reference to the linked (related) document.",
            linkedCollection: "Linked collection",
            linkedCollectionInfo:
                "The RavenDB collection name for the related documents.<br/>The related document ID is derived from this collection name and the join column values.",
            joinColumnsInfo:
                "The foreign key columns used to join this linked table to the parent table. Their values, combined with the linked collection name, form the related document ID.",
        },
    },
    fieldMapping: {
        title: "Field mapping",
        add: "Add field mapping",
        empty: "No field mappings defined.",
        sourceColumn: "Source column",
        targetPropertyInfo: "The RavenDB document field where the source column value will be stored.",
        type: "Type",
        typeInfo:
            "How the source column value is stored in the document:<br/><strong>Default</strong> - stored as a regular property.<br/><strong>JSON</strong> - parsed as JSON before storing.<br/><strong>Attachment</strong> - stored as a RavenDB attachment.",
        typeDefault: "Default",
        typeAttachment: "Attachment",
    },
    syntaxHelp: {
        availableVariables: "Available variables",
    },
    patch: {
        label: "Patch",
        info: "A patch is a JavaScript snippet that runs on <strong>INSERT</strong> and <strong>UPDATE</strong> AFTER column mapping is applied and before the document is written to RavenDB.",
        usage: "Use it when the mapped document needs additional changes, such as computed fields, aggregate values, metadata updates, or data loaded from related documents.",
        placeholder:
            "// Optional. Transforms 'this' (the document) AFTER column mapping for INSERT and UPDATE events.\n// e.g. this.FullName = $row.first_name + ' ' + $row.last_name;\n// Click the (?) icon on the right for syntax help and more examples.",
        variableThis:
            "<code>this</code> - the document AFTER column mapping has been applied<br/>(already contains the new values from the CDC row).",
        variableRow: "<code>$row</code> - the raw CDC row with all columns as-is from the source database.",
        variableOld:
            "<code>$old</code> - the state stored in RavenDB BEFORE the CDC event was processed<br/>(<strong>for root patches</strong>: the full document; <strong>for embedded patches</strong>: the matched embedded item).<br/>Null for inserts.",
        computeFieldExample: "Compute a field from source columns:",
        unmappedColumnExample: "Computed field from an unmapped columns:",
        deltaComputationExample: "Delta computation using <code>$old</code>:",
        conditionalEnrichmentExample: "Conditionally enrich the document:",
        loadRelatedDocumentExample: "Load a related document:",
    },
    onDelete: {
        skipDeletion: "Skip deletion",
        skipDeletionDefault:
            "<strong>Default (OFF):</strong><br/>DELETE events from the source are applied to RavenDB: root-table documents are deleted, embedded items are removed from the parent document, and <code>Value</code>-type embedded properties are set to <code>null</code>.",
        skipDeletionEnabled:
            "<strong>When enabled:</strong><br/>DELETE events are <strong>not applied</strong> to the target document. The document, embedded item, or <code>Value</code>-type property is kept.",
        skipDeletionPatch:
            "If a delete patch is configured, it still runs even when <em>Skip deletion</em> is on.<br/>You can use it to mark the document as archived,<br/>for example: <code>this.Archived = true</code>.",
        deletePatch: "Delete patch",
        deletePatchInfo:
            "A delete patch is a JavaScript snippet that runs when RavenDB processes a <strong>DELETE</strong> event from the source.",
        deletePatchRootTables:
            "<strong>Root tables:</strong> <code>this</code> is the existing document.<br/>When deletion is not skipped, RavenDB deletes the document after the patch runs.",
        deletePatchEmbeddedTables:
            "<strong>Embedded tables:</strong> <code>this</code> is the parent document. When deletion is not skipped, the embedded item is removed before the patch runs; use <code>$old</code> to access the removed item.",
        deletePatchSkipDeletion:
            "When <em>Skip deletion</em> is enabled, the automatic deletion or removal is not applied, but the patch still runs.",
        deletePatchUsage:
            "Use it to mark retained documents as archived, write audit records with <code>put()</code>, or update parent-level aggregates after an embedded item is removed.",
        placeholder:
            '// Optional. Runs when a DELETE event is processed, even when "Skip deletion" is enabled.\n// e.g.  this.Archived = true;   // combine with "Skip deletion" for soft-delete\n// Click the (?) icon below for syntax and more examples.',
        variableThis:
            "<code>this</code> -<br/><strong>root deletes</strong>: the document about to be deleted.<br/><strong>Embedded deletes</strong>: the parent document, with the embedded item already removed.<br/>If <em>Skip deletion</em> is on, the item is still there - the deletion hasn't been applied yet.",
        variableRow:
            "<code>$row</code> -<br/>the raw CDC row from the DELETE event, with all columns as-is from the source database.",
        variableOld:
            "<code>$old</code> -<br/><strong>root deletes</strong>: the full document (same as <code>this</code>).<br/><strong>Embedded deletes</strong>: the embedded item being removed.<br/>Always populated on DELETE.",
        archiveExample: "Archive the document instead of deleting it:",
        auditTrailExample: "Write an audit record before deletion:",
        recomputeAggregateExample: "Recompute a parent aggregate after an embedded item is removed:",
    },
    testPanel: {
        title: "Test mapping",
        settings: "Settings",
        rowSelector: "Row selector",
        rowSelectorFirst: "First row",
        rowSelectorByPrimaryKey: "Primary Key",
        maxRows: "Max rows",
        primaryKeyValues: "Primary key values",
        addPrimaryKeyValue: "Add primary key value",
        simulateOnDelete: "Simulate on delete",
        runTest: "Run test",
        loadError: "Failed to test table",
    },
    tableWarnings: {
        duplicateRootTable:
            "Source table {{sourceTableLabel}} is already configured as another root table. CDC Sink can process a source table only once.",
        embeddedRootTableConflict:
            "This source table is already configured as a root table.\nCDC Sink can process a source table only once, so embedded updates may be routed to the root table instead.",
        relatedCollectionMissingRootTable:
            'Related documents in the "{{linkedCollectionName}}" collection will not be created because {{sourceTableLabel}} is not configured as a root table.',
        relatedCollectionDisabledRootTable:
            'Related documents in the "{{linkedCollectionName}}" collection will not be created because the {{sourceTableLabel}} root table is disabled.',
        relatedCollectionOtherTarget_one:
            'Related documents in the "{{linkedCollectionName}}" collection will not be created because the {{sourceTableLabel}} root table targets the {{collectionNames}} collection instead.',
        relatedCollectionOtherTarget_other:
            'Related documents in the "{{linkedCollectionName}}" collection will not be created because the {{sourceTableLabel}} root table targets the {{collectionNames}} collections instead.',
        relatedCollectionDifferentTarget:
            'Related documents in the "{{linkedCollectionName}}" collection will not be created because the {{sourceTableLabel}} root table targets a different collection.',
    },
    validation: {
        sourceSchemaRequired:
            'Source schema is required. Enter the schema the source table belongs to (e.g. "public" or "dbo").',
        columnMappingRequired: "At least one column mapping is required",
        sourceColumnsUnique: "Source columns must be unique",
        targetPropertiesUnique: "Target properties must be unique",
        joinColumnRequired: "At least one join column is required",
        joinColumnsUnique: "Join columns must be unique",
        primaryKeyColumnRequired: "At least one primary key column is required",
        primaryKeyColumnsUnique: "Primary key columns must be unique",
        tableRequired: "At least one table is required",
        collectionNamesUnique: "Collection names must be unique",
    },
    verification: {
        buttonInfo:
            "Runs the CDC flow once against the source database, reading one row from each configured table. Nothing is saved in RavenDB. CDC objects the run needs on the source database are created temporarily and removed afterwards.",
        verifying: "Verifying tables...",
        failed: "Verification failed",
        verifiedWithWarnings: "Tables verified with warnings",
        verified: "Tables verified",
        verify: "Verify tables",
        failedTitle: "Data source verification failed for the configured tables.",
        passedWithWarningsTitle: "Data source verification passed with warnings.",
        showDetails: "Show details",
        hideDetails: "Hide details",
        dryRunFailed: "The CDC dry run failed.",
        requestFailed: "The dry run request failed. Check the Notification Center for details.",
        connectionStringNotFound: "Connection string '{{connectionStringName}}' was not found.",
    },
    footer: {
        save: "Save task configuration",
    },
    saveAnywayConfirm: {
        title: "Save the task configuration anyway?",
        confirmText: "Save anyway",
    },
} as const;
