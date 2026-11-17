export default {
    title: "Database Record",
    loadError: "Unable to load database record",
    fetchError: "Error fetching database record!",
    editButton: "Edit record",
    hideEmptyValuesSwitch: "Hide empty values",
    expandButton: "Expand record",
    collapseButton: "Collapse record",
    editConfirm: {
        title: "You're about to enter edit mode",
        confirmButton: "I understand the risk and want to proceed",
        riskWarning:
            "Tampering with the Database Record may result in unwanted behavior including loss of the database along with all its data.",
    },
    saveConfirm: {
        title: "Do you want to save changes?",
        confirmButton: "Yes, save changes",
    },
    discardConfirm: {
        title: "Do you want to discard changes?",
        confirmButton: "Yes, discard changes",
    },
    noAccess: {
        badge: "Insufficient access",
        message: "You are not authorized to view this page",
    },
    about: {
        storesItem:
            "The Database Record stores all settings and configurations for the database and is consistently updated. Each node in the cluster maintains an up-to-date copy of this document.",
        editingItem:
            "Editing the Database Record is strongly discouraged.<br/>Changes may lead to unintended consequences, including the loss of the entire database and its data.",
        docsLink: "Docs - Database Record",
    },
} as const;
