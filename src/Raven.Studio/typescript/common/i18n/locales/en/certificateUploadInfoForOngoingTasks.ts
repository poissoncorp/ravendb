export default {
    instructions:
        "Please apply the following so that the Destination server will trust receiving data from this Source server where the task is defined:",
    download: {
        start: "Download the Server Certificate from this",
        source: "Source",
        beforeLink: "server (in the",
        certificatesView: "Certificates View",
        end: ").",
    },
    upload: {
        start: "Go to the Certificates View in the",
        destination: "Destination",
        end: "server and upload that certificate.",
    },
} as const;
