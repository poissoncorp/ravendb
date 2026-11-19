import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AiAssistantDisabledInSettingsMessage() {
    return (
        <div>
            <StudioTrans ns="aiAssistant" i18nKey="disabledInSettings" components={{ br: <br /> }} />
        </div>
    );
}
