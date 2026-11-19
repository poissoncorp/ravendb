export default {
    readBalanceBehavior: {
        none: "None",
        roundRobin: "Round Robin",
        fastestNode: "Fastest node",
    },
    loadBalanceBehavior: {
        none: "None",
        useSessionContext: "Use Session Context",
    },
    validation: {
        identityPartsSeparatorPipe: "Identity parts separator cannot be set to '|'",
    },
} as const;
