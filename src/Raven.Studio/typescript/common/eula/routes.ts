/// <reference path="../../../typings/tsd.d.ts"/>

import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("eula");

class routes {

    static get(): Array<DurandalRouteConfiguration> {
        return [
            {
                route: ["", "license"],
                title: t("routeTitle"),
                moduleId: require("viewmodels/eula/license"),
                nav: false
            }
        ];
    }

}

export = routes;
