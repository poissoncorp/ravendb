const lodash = require("lodash");
const knockout = require("knockout");
require("knockout-postbox");
const jquery = require("jquery");

global._ = lodash;
global.ko = knockout;
global.$ = jquery;
global.jQuery = jquery;

require("bootstrap/dist/js/bootstrap");

require("../typescript/test/mocks");

const customHooks = require("../typescript/components/hooks/hooksForAutoMock.json").hooks;

customHooks.forEach(hook => {
    jest.mock("hooks/" + hook);
});

jest.mock("../typescript/common/eventsCollector");
jest.mock("../typescript/common/bindingHelpers/aceEditorBindingHandler");

jest.mock("../typescript/common/versionProvider");

jest.mock('plugins/router', () => ({
    activate: jest.fn(),
    navigate: jest.fn()
}));
jest.mock('plugins/dialog', () => ({
}));
jest.mock('durandal/app', () => ({
}));