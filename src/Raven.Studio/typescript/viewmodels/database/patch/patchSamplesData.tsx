import React from "react";
import { MethodGroup, SampleScript } from "components/common/samples/partials/samplesTypes";
import { createTranslator } from "common/i18n/i18n";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const t = createTranslator("patch");

const descriptionTags = { code: <code /> };

export const scripts: SampleScript[] = [
    {
        get title() {
            return t("samples.filterArrayItem");
        },
        description: "",
        script: `// Removes a specific line item (product ID 'products/1-A') from all order documents.
from Orders
update {
    this.Lines = this.Lines.filter(l => l.Product != 'products/1-A');
}`,
    },
    {
        get title() {
            return t("samples.denormalizeCompanyName");
        },
        description: "",
        script: `// Adds a CompanyName field to all orders, populated from a related document.
from Orders as o
load o.Company as c
update {
    o.CompanyName = c.Name;
}`,
    },
    {
        get title() {
            return t("samples.useJavaScript");
        },
        description: "",
        script: `// Conditionally adds a new lowercase company name field to orders over 10,000.
from index 'Orders/Totals' as i
where i.Total > 10000
load i.Company as c
update {
    i.LowerName = c.Name.toLowerCase();
}`,
    },
    {
        get title() {
            return t("samples.accessMetadata");
        },
        description: "",
        script: `// Adds the document ID and collection name as two new fields to every order.
from Orders
update {
    this.DocumentId = id(this);
    this.DocumentCollection = this["@metadata"]["@collection"];
}`,
    },
    {
        get title() {
            return t("samples.addTimeSeriesEntry");
        },
        description: "",
        script: `// Appends a time series entry to a specific employee document.
from Employees as e
where id() = 'employees/1-A'
update {
    timeseries(e, "HeartRates")
        .append("2020-06-25T10:48:14.794", [120, 80], "TagName");
}`,
    },
    {
        get title() {
            return t("samples.updateCollectionField");
        },
        description: "",
        script: `// Increases the Freight value by 10 on every order.
from Orders as o
update {
    o.Freight += 10;
}`,
    },
    {
        get title() {
            return t("samples.renameField");
        },
        description: "",
        script: `// Copies Freight into a new ShippingCost field, then removes the old field.
from Orders as o
update {
    o.ShippingCost = o.Freight;
    delete o.Freight;
}`,
    },
    {
        get title() {
            return t("samples.updateArrayItems");
        },
        description: "",
        script: `// Applies a 10% discount to every line item in all orders.
from Orders as o
update {
    o.Lines.forEach(line => line.Discount = 0.1);
}`,
    },
    {
        get title() {
            return t("samples.incrementCounter");
        },
        description: "",
        script: `// Increments a 'Views' counter by 1 on every product.
from Products as p
update {
    incrementCounter(p, "Views", 1);
}`,
    },
    {
        get title() {
            return t("samples.createRelatedDocument");
        },
        description: "",
        script: `// Writes a new audit document for each order, using a server-generated ID.
from Orders as o
update {
    put("auditLogs/", { OrderId: id(o), PatchedAt: new Date().toISOString() });
}`,
    },
    {
        get title() {
            return t("samples.patchById");
        },
        description: "",
        script: `// Sets a field on a specific set of documents selected by their IDs.
from @all_docs as d
where id() in ('orders/1-A', 'companies/1-A')
update {
    d.Updated = true;
}`,
    },
    {
        get title() {
            return t("samples.deleteByQuery");
        },
        description: "",
        script: `// Deletes all orders with freight over 200.
from Orders as o
where o.Freight > 200
update {
    del(id(o));
}`,
    },
    {
        get title() {
            return t("samples.changeCollection");
        },
        description: "",
        script: `// Moves matching orders into a new collection by deleting and re-creating each document.
from Orders as o
update {
    o["@metadata"]["@collection"] = "Archived_Orders";
    del(id(o));
    put(id(o), o);
}`,
    },
];

export const methodGroups: MethodGroup[] = [
    {
        get category() {
            return t("methods.documents.category");
        },
        methods: [
            {
                signature: "id(document)",
                returnType: "string",
                description: <StudioTrans ns="patch" i18nKey="methods.documents.id" components={descriptionTags} />,
                sampleScript: `// Copy document IDs from the current document, query alias, and loaded related document.
from Orders as o
load o.Company as c
update {
    o.OrderId = id(this);       // Current document
    o.OrderAliasId = id(o);     // Query alias
    o.CompanyId = id(c);        // Loaded related document
}`,
            },
            {
                signature: "getMetadata(document)",
                returnType: "object",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.documents.getMetadata" components={descriptionTags} />
                ),
                sampleScript: `// Read the collection name from metadata into a field.
from Orders as o
update {
    o.Collection = getMetadata(o)["@collection"];
}`,
            },
            {
                signature: "lastModified(document)",
                returnType: "number",
                get description() {
                    return t("methods.documents.lastModified");
                },
                sampleScript: `// Store each order's last-modified time (ms) in a field.
from Orders as o
update {
    o.LastModified = lastModified(o);
}`,
            },
            {
                signature: "load(documentId)",
                returnType: "object | object[] | undefined",
                get description() {
                    return t("methods.documents.load");
                },
                sampleScript: `// Load the related company and copy its name.
from Orders as o
update {
    o.CompanyName = load(o.Company).Name;
}`,
            },
            {
                signature: "loadPath(document, pathString)",
                returnType: "object | object[] | null",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.documents.loadPath" components={descriptionTags} />
                ),
                sampleScript: `// Load all product documents referenced by the order lines.
from Orders as o
update {
    o.Products = loadPath(o, "Lines[].Product");
}`,
            },
            {
                signature: "put(id, document[, changeVector])",
                returnType: "string",
                description: <StudioTrans ns="patch" i18nKey="methods.documents.put" components={descriptionTags} />,
                sampleScript: `// Create a new audit-log document for each order.
from Orders as o
update {
    put("auditLogs/", { OrderId: id(o), PatchedAt: new Date().toISOString() });
}`,
            },
            {
                signature: "del(documentId[, changeVector])",
                returnType: "boolean",
                description: <StudioTrans ns="patch" i18nKey="methods.documents.del" components={descriptionTags} />,
                sampleScript: `// Delete orders with freight over 200.
from Orders as o
where o.Freight > 200
update {
    del(id(o));
}`,
            },
            {
                signature: "archived.archiveAt(document, utcDateString)",
                returnType: "void",
                get description() {
                    return t("methods.documents.archiveAt");
                },
                sampleScript: `// Schedule each order to be archived at a future date.
from Orders as o
update {
    archived.archiveAt(o, "2030-01-01T00:00:00Z");
}`,
            },
            {
                signature: "archived.unarchive(document)",
                returnType: "void",
                get description() {
                    return t("methods.documents.unarchive");
                },
                sampleScript: `// Unarchive all orders.
from Orders as o
update {
    archived.unarchive(o);
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.counters.category");
        },
        methods: [
            {
                signature: "counter(document/documentId, name)",
                returnType: "number | null",
                get description() {
                    return t("methods.counters.counter");
                },
                sampleScript: `// Read the "Views" counter into a field.
from Products as p
update {
    p.Views = counter(p, "Views");
}`,
            },
            {
                signature: "counterRaw(document/documentId, name)",
                returnType: "object",
                get description() {
                    return t("methods.counters.counterRaw");
                },
                sampleScript: `// Read the per-node "Views" counter values into a field.
from Products as p
update {
    p.ViewsPerNode = counterRaw(p, "Views");
}`,
            },
            {
                signature: "incrementCounter(document/documentId, name, value = 1)",
                returnType: "boolean",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.counters.incrementCounter" components={descriptionTags} />
                ),
                sampleScript: `// Increment the "Views" counter by 1 on every product.
from Products as p
update {
    incrementCounter(p, "Views", 1);
}`,
            },
            {
                signature: "deleteCounter(document/documentId, name)",
                returnType: "boolean",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.counters.deleteCounter" components={descriptionTags} />
                ),
                sampleScript: `// Delete the "Views" counter from every product.
from Products as p
update {
    deleteCounter(p, "Views");
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.timeSeries.category");
        },
        methods: [
            {
                signature: "timeseries(document/documentId, name).get() / .get(from, to)",
                returnType: "object[]",
                get description() {
                    return t("methods.timeSeries.getEntries");
                },
                sampleScript: `// Read all "HeartRates" time series entries into a field.
from Employees as e
update {
    e.HeartRates = timeseries(e, "HeartRates").get();
}`,
            },
            {
                signature: "timeseries(document/documentId, name).append(timestamp, values, tag = null)",
                returnType: "void",
                get description() {
                    return t("methods.timeSeries.append");
                },
                sampleScript: `// Append a new entry to a specific employee's "HeartRates" time series.
from Employees as e
where id() = 'employees/1-A'
update {
    timeseries(e, "HeartRates").append("2024-01-01T08:00:00Z", [78], "watch");
}`,
            },
            {
                signature: "timeseries(document/documentId, name).increment([timestamp,] values)",
                returnType: "void",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.timeSeries.increment" components={descriptionTags} />
                ),
                sampleScript: `// Increment today's value in the "DailyViews" time series.
from Products as p
update {
    timeseries(p, "DailyViews").increment([1]);
}`,
            },
            {
                signature: "timeseries(document/documentId, name).delete() / .delete(from, to)",
                returnType: "void",
                get description() {
                    return t("methods.timeSeries.deleteEntries");
                },
                sampleScript: `// Delete all "HeartRates" time series entries.
from Employees as e
update {
    timeseries(e, "HeartRates").delete();
}`,
            },
            {
                signature: "timeseries(document/documentId, name).getStats()",
                returnType: "object",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.timeSeries.getStats" components={descriptionTags} />
                ),
                sampleScript: `// Store "HeartRates" time series statistics in a field.
from Employees as e
update {
    e.HeartRateStats = timeseries(e, "HeartRates").getStats();
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.attachments.category");
        },
        methods: [
            {
                signature: "attachments(document/documentId, name).delete()",
                returnType: "void",
                get description() {
                    return t("methods.attachments.deleteAttachment");
                },
                sampleScript: `// Delete the "image.jpg" attachment from each category.
from Categories as c
update {
    attachments(c, "image.jpg").delete();
}`,
            },
            {
                signature:
                    "attachments(targetDocument/targetDocumentId, targetName).copyFrom(sourceDocument/sourceDocumentId, sourceName)",
                returnType: "boolean",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.attachments.copyFrom" components={descriptionTags} />
                ),
                sampleScript: `// Copy an attachment from one category to another.
from Categories as c
where id() = 'categories/2-A'
update {
    attachments(c, "copied-image.jpg").copyFrom('categories/1-A', "image.jpg");
}`,
            },
            {
                signature: "attachments(document/documentId, name).remote(identifier, at)",
                returnType: "void",
                get description() {
                    return t("methods.attachments.remote");
                },
                sampleScript: `// Schedule an attachment for upload to remote storage.
from Categories as c
update {
    attachments(c, "image.jpg").remote("s3-archive", "2030-01-01T00:00:00Z");
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.compareExchange.category");
        },
        methods: [
            {
                signature: "cmpxchg(compareExchangeKey)",
                returnType: "any",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.compareExchange.cmpxchg" components={descriptionTags} />
                ),
                sampleScript: `// Look up the USD rate from a compare-exchange value.
from Orders as o
update {
    o.UsdRate = cmpxchg("rates/USD");
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.strings.category");
        },
        methods: [
            {
                signature: "startsWith(inputString, prefix)",
                returnType: "boolean",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.strings.startsWith" components={descriptionTags} />
                ),
                sampleScript: `// Flag companies whose name starts with "Alfreds".
from Companies as c
update {
    c.IsAlfreds = startsWith(c.Name, "Alfreds");
}`,
            },
            {
                signature: "endsWith(inputString, suffix)",
                returnType: "boolean",
                description: <StudioTrans ns="patch" i18nKey="methods.strings.endsWith" components={descriptionTags} />,
                sampleScript: `// Flag companies whose name ends with "Ltda".
from Companies as c
update {
    c.IsLtda = endsWith(c.Name, "Ltda");
}`,
            },
            {
                signature: "regex(inputString, regex)",
                returnType: "boolean",
                description: <StudioTrans ns="patch" i18nKey="methods.strings.regex" components={descriptionTags} />,
                sampleScript: `// Flag companies whose name contains a digit.
from Companies as c
update {
    c.NameHasDigit = regex(c.Name, "[0-9]");
}`,
            },
            {
                signature: "String.prototype.format(arg1, arg2, ...)",
                returnType: "string",
                description: <StudioTrans ns="patch" i18nKey="methods.strings.format" components={descriptionTags} />,
                sampleScript: `// Build a label string from the order ID and company.
from Orders as o
update {
    o.Label = "Order {0} for {1}".format(id(o), o.Company);
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.arrays.category");
        },
        methods: [
            {
                signature: "Object.map(input, mapFunction, context)",
                returnType: "any[]",
                description: <StudioTrans ns="patch" i18nKey="methods.arrays.map" components={descriptionTags} />,
                sampleScript: `// Compute a line-total for each order line.
from Orders as o
update {
    o.LineTotals = Object.map(o.Lines, line => line.PricePerUnit * line.Quantity);
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.math.category");
        },
        methods: [
            {
                signature: "Raven_Min(value1, value2)",
                returnType: "number | string | boolean",
                get description() {
                    return t("methods.math.min");
                },
                sampleScript: `// Cap each order's freight at 100.
from Orders as o
update {
    o.CappedFreight = Raven_Min(o.Freight, 100);
}`,
            },
            {
                signature: "Raven_Max(value1, value2)",
                returnType: "number | string | boolean",
                get description() {
                    return t("methods.math.max");
                },
                sampleScript: `// Apply a minimum charged freight of 10.
from Orders as o
update {
    o.MinChargedFreight = Raven_Max(o.Freight, 10);
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.spatial.category");
        },
        methods: [
            {
                signature: 'spatial.distance(lat1, lng1, lat2, lng2, units = "kilometers")',
                returnType: "number",
                description: <StudioTrans ns="patch" i18nKey="methods.spatial.distance" components={descriptionTags} />,
                sampleScript: `// Compute each company's distance to London (km).
from Companies as c
update {
    c.DistanceToLondon = spatial.distance(c.Address.Location.Latitude, c.Address.Location.Longitude, 51.5, -0.12);
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.conversion.category");
        },
        methods: [
            {
                signature: "scalarToRawString(document, lambdaToField)",
                returnType: "string | number | boolean | null",
                description: (
                    <StudioTrans
                        ns="patch"
                        i18nKey="methods.conversion.scalarToRawString"
                        components={descriptionTags}
                    />
                ),
                sampleScript: `// Read the raw, immutable Freight value.
from Orders as o
update {
    o.RawFreight = scalarToRawString(o, x => x.Freight);
}`,
            },
            {
                signature: "convertJsTimeToTimeSpanString(milliseconds)",
                returnType: "string",
                description: (
                    <StudioTrans
                        ns="patch"
                        i18nKey="methods.conversion.convertJsTimeToTimeSpanString"
                        components={descriptionTags}
                    />
                ),
                sampleScript: `// Compute how long ago each order was modified.
from Orders as o
update {
    o.Age = convertJsTimeToTimeSpanString(Date.now() - lastModified(o));
}`,
            },
            {
                signature:
                    "convertToTimeSpanString(ticks | hours, minutes, seconds | days, hours, minutes, seconds[, milliseconds])",
                returnType: "string",
                description: (
                    <StudioTrans
                        ns="patch"
                        i18nKey="methods.conversion.convertToTimeSpanString"
                        components={descriptionTags}
                    />
                ),
                sampleScript: `// Store a fixed 1h30m processing time as a TimeSpan.
from Orders as o
update {
    o.ProcessingTime = convertToTimeSpanString(1, 30, 0);
}`,
            },
            {
                signature: 'compareDates(date1, date2, operationType = "Subtract")',
                returnType: "string | boolean",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.conversion.compareDates" components={descriptionTags} />
                ),
                sampleScript: `// Compute the time between order and shipping dates.
from Orders as o
where o.ShippedAt != null
update {
    o.TimeToShip = compareDates(o.ShippedAt, o.OrderedAt);
}`,
            },
            {
                signature: "toStringWithFormat(object, format?, culture?)",
                returnType: "string",
                get description() {
                    return t("methods.conversion.toStringWithFormat");
                },
                sampleScript: `// Format Freight as a currency string.
from Orders as o
update {
    o.FreightText = toStringWithFormat(o.Freight, "C", "en-US");
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.crypto.category");
        },
        methods: [
            {
                signature: "crypto.randomUUID()",
                returnType: "string",
                get description() {
                    return t("methods.crypto.randomUUID");
                },
                sampleScript: `// Assign a random trace ID to each order.
from Orders as o
update {
    o.TraceId = crypto.randomUUID();
}`,
            },
            {
                signature: "crypto.getRandomValues(typedArray)",
                returnType: "TypedArray",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.crypto.getRandomValues" components={descriptionTags} />
                ),
                sampleScript: `// Generate a random 16-byte nonce.
from Orders as o
update {
    var nonce = crypto.getRandomValues(new Uint8Array(16));
    o.Nonce = Array.from(nonce);
}`,
            },
            {
                signature: "crypto.getRandomValuesBase64(lenInBytes)",
                returnType: "string",
                description: (
                    <StudioTrans
                        ns="patch"
                        i18nKey="methods.crypto.getRandomValuesBase64"
                        components={descriptionTags}
                    />
                ),
                sampleScript: `// Generate a random Base64 token.
from Orders as o
update {
    o.Token = crypto.getRandomValuesBase64(16);
}`,
            },
            {
                signature: "crypto.digest(algorithm, data)",
                returnType: "string",
                description: <StudioTrans ns="patch" i18nKey="methods.crypto.digest" components={descriptionTags} />,
                sampleScript: `// Compute a SHA-256 hash of the order ID.
from Orders as o
update {
    o.Hash = crypto.digest("SHA-256", id(o));
}`,
            },
            {
                signature: "crypto.sign(hash, key, data)",
                returnType: "string",
                description: <StudioTrans ns="patch" i18nKey="methods.crypto.sign" components={descriptionTags} />,
                sampleScript: `// Compute an HMAC signature over the order ID.
from Orders as o
update {
    o.Signature = crypto.sign("SHA-256", "secret-key", id(o));
}`,
            },
            {
                signature: "crypto.verify(hash, key, signature, data)",
                returnType: "boolean",
                description: <StudioTrans ns="patch" i18nKey="methods.crypto.verify" components={descriptionTags} />,
                sampleScript: `// Verify an HMAC signature over the order ID.
from Orders as o
update {
    var signature = crypto.sign("SHA-256", "secret-key", id(o));
    o.SignatureValid = crypto.verify("SHA-256", "secret-key", signature, id(o));
}`,
            },
            {
                signature: "crypto.encryptAesGcm(iv, key, data)",
                returnType: "string",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.crypto.encryptAesGcm" components={descriptionTags} />
                ),
                sampleScript: `// Encrypt the company field with AES-GCM.
from Orders as o
update {
    var key = crypto.getRandomValuesBase64(32);
    var iv = crypto.getRandomValuesBase64(12);
    o.EncryptedCompany = crypto.encryptAesGcm(iv, key, o.Company);
}`,
            },
            {
                signature: "crypto.decryptAesGcm(iv, key, data, outputType?)",
                returnType: "string | ArrayBuffer",
                description: (
                    <StudioTrans ns="patch" i18nKey="methods.crypto.decryptAesGcm" components={descriptionTags} />
                ),
                sampleScript: `// Decrypt an encrypted company field.
from Orders as o
update {
    var key = crypto.getRandomValuesBase64(32);
    var iv = crypto.getRandomValuesBase64(12);
    var encrypted = crypto.encryptAesGcm(iv, key, o.Company);
    o.DecryptedCompany = crypto.decryptAesGcm(iv, key, encrypted, "string");
}`,
            },
        ],
    },
    {
        get category() {
            return t("methods.debugging.category");
        },
        methods: [
            {
                signature: "output(message)",
                returnType: "void",
                description: <StudioTrans ns="patch" i18nKey="methods.debugging.output" components={descriptionTags} />,
                sampleScript: `// Print a debug message for each order being patched.
from Orders as o
update {
    output("Patching order " + id(o));
}`,
            },
        ],
    },
];
