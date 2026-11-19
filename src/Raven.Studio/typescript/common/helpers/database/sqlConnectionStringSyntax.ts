

import i18nModule = require("common/i18n/i18n");
import generalUtils = require("common/generalUtils");

const t = i18nModule.createTranslator("commonHelpers");

class sqlConnectionStringSyntax {
    
    private constructor() {
        // empty 
    }

    private static syntax(connectionString: string, fullSyntaxUrl: string) {
        return `${t("sqlSyntax.example", { connectionString: generalUtils.escapeHtml(connectionString) })}
                <br />
                <small>
                    ${t("sqlSyntax.moreExamples").replace("<a>", `<a href="${fullSyntaxUrl}" target="_blank"><i class="icon-link"></i> `)}
                </small>`;
    }

    static get mssqlSyntax() {
        return sqlConnectionStringSyntax.syntax("Data Source=10.0.0.107;Database=SourceDB;User ID=sa;Password=secret;", "https://ravendb.net/l/38S9OQ");
    }
    
    static get mysqlSyntax() {
        return sqlConnectionStringSyntax.syntax("server=10.0.0.103;port=3306;userid=root;password=secret;", "https://ravendb.net/l/BSS8YH");
    }
    
    static get npgsqlSyntax() {
        return sqlConnectionStringSyntax.syntax("Host=10.0.0.105;Port=5432;Username=postgres;Password=secret", "https://ravendb.net/l/FWEBWD");
    }
    
    static get oracleSyntax() {
        return sqlConnectionStringSyntax.syntax("Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.0.101)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)));User Id=SYS;DBA Privilege=SYSDBA;password=secret;", "https://ravendb.net/l/TG851N");
    }
}

export = sqlConnectionStringSyntax;
