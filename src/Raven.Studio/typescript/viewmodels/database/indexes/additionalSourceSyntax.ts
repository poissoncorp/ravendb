import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import prismjs = require("prismjs");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("editIndex");

class additionalSourceSyntax extends dialogViewModelBase {

    view = require("views/database/indexes/additionalSourceSyntax.html");
    additionalTextView = require("views/database/indexes/additionalTabsCommonText.html");

    whereToAddHtml = t("additionalSyntax.whereToAdd");
    csharpExampleHtml = t("syntaxSamples.exampleHeading", { language: "C#" });
    javascriptExampleHtml = t("syntaxSamples.exampleHeading", { language: "JavaScript" });
    
    dialogContainer: Element;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("additionalSourceSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = [...additionalSourceSyntax.csharpSamples, ...additionalSourceSyntax.javascriptSamples]
            .find(x => x.title === sampleTitle).text;

        copyToClipboard.copy(sampleText, t("syntaxSamples.copied"), this.dialogContainer);
    }

    static readonly additionalSourceCsharpText =
`// *Additional Source*
public static class PeopleUtil
{
    public static string CalculatePersonEmail(string name)
    {
        return $"{name}@example.com";
    }
}`;

    static readonly usageInMapCsharpText =
`// *Usage in the index Map statement* 
docs.People.Select(person => new {
    Name = person.Name,
    _ = this.CreateField("Email", 
            // use method from source file
            PeopleUtil.CalculatePersonEmail(person.Name), 
            stored: true)
})


`;

    static readonly additionalSourceJavascriptText =
`// *Additional Source*


function calculatePersonEmail(name) 
{
    return name + '@example.com';
}

`;

    static readonly usageInMapJavascriptText =
`// *Usage in the index Map statement* 
map('People', function (person) { return {
       Name: person.Name, 
       _: { $name:'Email',
            // use method from source file
            $value: calculatePersonEmail(person.Name),
            $options:{ store: true }
        }
    };
})`;

    static readonly csharpSamples: Array<sampleCode> = [
        {
            title: "Csharp - Additional Source",
            text: additionalSourceSyntax.additionalSourceCsharpText,
            html: prismjs.highlight(additionalSourceSyntax.additionalSourceCsharpText, prismjs.languages.csharp, "csharp")
        },
        {
            title: "Csharp - Usage in Map",
            text: additionalSourceSyntax.usageInMapCsharpText,
            html: prismjs.highlight(additionalSourceSyntax.usageInMapCsharpText, prismjs.languages.csharp, "csharp")
        }
    ];

    static readonly javascriptSamples: Array<sampleCode> = [
        {
            title: "Javascript - Additional Source",
            text: additionalSourceSyntax.additionalSourceJavascriptText,
            html: prismjs.highlight(additionalSourceSyntax.additionalSourceJavascriptText, prismjs.languages.javascript, "csharp")
        },
        {
            title: "Javascript - Usage in Map",
            text: additionalSourceSyntax.usageInMapJavascriptText,
            html: prismjs.highlight(additionalSourceSyntax.usageInMapJavascriptText, prismjs.languages.javascript, "csharp")
        }
    ];
}

export = additionalSourceSyntax;
