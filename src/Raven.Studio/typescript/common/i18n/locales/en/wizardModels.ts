export default {
    certificate: {
        cnRequired: "Certificate must contain at least one CN or Subject Alternative Name.",
    },
    continueSetup: {
        invalidNodesConfiguration: "Invalid nodes configuration in zip file",
    },
    domain: {
        invalidCharacters: "Domain name can only contain A-Z, a-z, 0-9, '-'",
        invalidHyphen: "Domain name cannot start or end with '-'",
        taken: "Sorry, domain name is taken.",
    },
    ip: {
        localhostOnDocker: "A localhost IP Address is not allowed when running on Docker",
        urlNotAllowed: "Expected valid IP Address/Hostname, not URL",
    },
    node: {
        reservedTag: "It is a reserved tag.",
        upperCaseTag: "Node tag must contain only upper case letters.",
        externalIpRequired: "This field is required when an address contains Hostname or 0.0.0.0",
        hostnameRequired: "This field is required",
        ipRequired: "Please define at least one IP for this node",
    },
    setup: {
        packageText_zipOnly:
            "Setup Wizard will only create a Setup Package for external setup. Current server will NOT be modified.",
        packageText_unsecured:
            "Setup Wizard will set up the current server. A Setup Package will be created only if multiple nodes are configured.",
        packageText_secure: "Setup Wizard will create a Setup Package AND set up the current server.",
        nodeRequired: "At least one node is required",
    },
} as const;
