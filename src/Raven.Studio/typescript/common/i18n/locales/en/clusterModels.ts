export default {
    addNode: {
        nodeTagPattern: "Node tag must contain upper case letters only.",
    },
    graph: {
        state: {
            waiting: "WAITING",
            passive: "PASSIVE",
            voting: "VOTING",
        },
    },
    node: {
        osName32Bit: "{{name}} 32-bit",
        osTooltip: {
            name: "OS Name: <strong>{{name}}</strong>",
            version: "Version: <strong>{{version}}</strong>",
            buildVersion_kernel: "Kernel Version: <strong>{{version}}</strong>",
            buildVersion_build: "Build Version: <strong>{{version}}</strong>",
        },
        memoryTooltip: "License limit on single node.<br> Doesn't apply to memory-mapped files managed by the OS.",
        state: {
            waiting: "Waiting",
            passive: "Passive",
            voting: "Voting",
            active: "Active",
            error: "Error",
        },
    },
} as const;
