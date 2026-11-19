import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("studioCommon");

class downloader {
    $downloadFrame = $("#downloadFrame");

    download(db: database | string, url: string): Promise<void> {
        const fullUrl = appUrl.forDatabaseQuery(db) + url;

        return downloader.canDownload(fullUrl).then((canDownload) => {
            if (canDownload) {
                this.$downloadFrame.attr("src", fullUrl);
            }
        });
    }

    reset() {
        this.$downloadFrame.attr("src", "");
    }

    static async canDownload(url: string, init?: RequestInit): Promise<boolean> {
        const abortController = new AbortController();

        try {
            const response = await fetch(url, { ...init, signal: abortController.signal });

            if (response.ok) {
                return true;
            }

            messagePublisher.reportError(t("downloadFailed"), await response.text(), response.statusText);
            return false;
        } catch (e) {
            messagePublisher.reportError(t("downloadFailed"), e.message);
            return false;
        } finally {
            abortController.abort();
        }
    }
    
    static fillHiddenFields(object: any, targetForm: JQuery) {
        targetForm.empty();
        
        const addField = (key: string, value: any) => {
            if (typeof value === "undefined") {
                return;
            }
            const $input = $("<input />")
                .attr("type", "hidden")
                .attr("name", key)
                .val(value);
            
            targetForm.append($input);
        }
        
        Object.keys(object).forEach(key => {
            const value = object[key];
            
            if (Array.isArray(value)) {
                value.forEach(v => addField(key, v));
            } else {
                addField(key, value);
            }
        });
    }
}

export = downloader
