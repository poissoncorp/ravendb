import { useState } from "react";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import { Icon } from "components/common/Icon";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import Code, { CodeLanguage } from "components/common/Code";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type ClientLanguage = Extract<CodeLanguage, "csharp" | "nodejs" | "python" | "java">;

const serverUrlPlaceholder = "https://your_RavenDB_server_URL";
const pfxPathPlaceholder = "C:\\path_to_your_pfx_file\\cert.pfx";
const dbNamePlaceholder = "your_database_name";

export default function CertificatesReadyModalApiAccess() {
    const t = useStudioTranslation("certificates");
    const docsLink = useRavenLink({ hash: "RSFSL5" });

    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const localNode = useAppSelector(clusterSelectors.localNode);

    const serverUrl = localNode?.serverUrl ?? serverUrlPlaceholder;
    const databaseName = dbName ?? dbNamePlaceholder;

    const [clientTab, setClientTab] = useState<ClientLanguage>("csharp");

    return (
        <Tab.Pane eventKey="api">
            <NumberedList>
                <NumberedListItem stepKey={1}>
                    <h4 className="mb-1">{t("apiAccess.locateHeading")}</h4>
                    <p className="mb-0">
                        <StudioTrans ns="certificates" i18nKey="apiAccess.locateText" components={{ code: <code /> }} />
                    </p>
                </NumberedListItem>
                <NumberedListItem stepKey={2}>
                    <h4 className="mb-2">{t("apiAccess.configureHeading")}</h4>
                    <p className="mb-2">
                        <StudioTrans
                            ns="certificates"
                            i18nKey="apiAccess.configureText"
                            components={{ code: <code /> }}
                        />
                    </p>
                    <Tab.Container activeKey={clientTab} onSelect={(key: ClientLanguage) => setClientTab(key)}>
                        <div className="segmented-tab-container">
                            <Nav className="flex-nowrap">
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="csharp" className="text-center">
                                        C#
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="java" className="text-center">
                                        Java
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="nodejs" className="text-center">
                                        Node.js
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="python" className="text-center">
                                        Python
                                    </Nav.Link>
                                </Nav.Item>
                            </Nav>
                            <Tab.Content className="p-2">
                                <Tab.Pane eventKey="csharp">
                                    <Code language="csharp" code={csharpSnippet(serverUrl, databaseName)} />
                                </Tab.Pane>
                                <Tab.Pane eventKey="java">
                                    <Code language="java" code={javaSnippet(serverUrl, databaseName)} />
                                </Tab.Pane>
                                <Tab.Pane eventKey="nodejs">
                                    <Code language="javascript" code={nodejsSnippet(serverUrl, databaseName)} />
                                </Tab.Pane>
                                <Tab.Pane eventKey="python">
                                    <Code language="python" code={pythonSnippet(serverUrl, databaseName)} />
                                </Tab.Pane>
                            </Tab.Content>
                        </div>
                    </Tab.Container>
                    <p className="mt-2 mb-0">
                        <StudioTrans
                            ns="certificates"
                            i18nKey="apiAccess.docsText"
                            components={{
                                a: <a href={docsLink} target="_blank" />,
                                icon: <Icon icon="newtab" margin="m-0" />,
                            }}
                        />
                    </p>
                </NumberedListItem>
                <NumberedListItem stepKey={3}>
                    <h4 className="mb-1">{t("apiAccess.restartHeading")}</h4>
                    <p className="mb-0">{t("apiAccess.restartText")}</p>
                </NumberedListItem>
            </NumberedList>
        </Tab.Pane>
    );
}

function csharpSnippet(serverUrl: string, databaseName: string): string {
    return `// Load an X.509 certificate
X509Certificate2 clientCertificate = new X509Certificate2("${pfxPathPlaceholder}");

using (IDocumentStore store = new DocumentStore()
{
    // Set the 'Certificate' property with your client certificate
    Certificate = clientCertificate,
    Database = "${databaseName}",
    Urls = new[] {"${serverUrl}"}
}.Initialize())
{
    // Do your work here
}`;
}

function javaSnippet(serverUrl: string, databaseName: string): string {
    return `// Load an X.509 certificate into a KeyStore
KeyStore clientStore = KeyStore.getInstance("PKCS12");
clientStore.load(new FileInputStream("${pfxPathPlaceholder}"), "your_password".toCharArray());

try (IDocumentStore store = new DocumentStore(
        new String[]{"${serverUrl}"},
        "${databaseName}",
        new KeyStoreOptions(clientStore, "your_password"))) {
    store.initialize();
    // Do your work here
}`;
}

function nodejsSnippet(serverUrl: string, databaseName: string): string {
    return `import * as fs from "fs";
import { DocumentStore } from "ravendb";

const authOptions = {
    certificate: fs.readFileSync("${pfxPathPlaceholder}"),
    type: "pfx",
    password: "your_password"
};

const store = new DocumentStore(["${serverUrl}"], "${databaseName}", authOptions);
store.initialize();
// Do your work here`;
}

function pythonSnippet(serverUrl: string, databaseName: string): string {
    return `from ravendb import DocumentStore

store = DocumentStore(
    urls=["${serverUrl}"],
    database="${databaseName}",
)
store.certificate_pem_path = "path_to_your_cert.pem"
store.initialize()
# Do your work here`;
}
