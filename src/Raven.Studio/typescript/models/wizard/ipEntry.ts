/// <reference path="../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardModels");

class ipEntry {
    
   ip = ko.observable<string>();
   validationGroup: KnockoutValidationGroup;
   
   isLocalNetwork: KnockoutComputed<boolean>;

   static runningOnDocker = false;
   
   constructor(allowHostname: boolean) {
       
       const extenders = {
           required: true
       } as any;
       
       if (allowHostname) {
           extenders.validAddressWithoutPort = true;
       } else {
           extenders.validIpWithoutPort = true;
       }
       
       extenders.validation = [
           {
               validator: (ip: string) => (ipEntry.runningOnDocker && !genUtils.isLocalhostIpAddress(ip)) || !ipEntry.runningOnDocker,
               message: () => t("ip.localhostOnDocker")
           },
           {
               validator: (ip: string) => !_.startsWith(ip, "http://") && !_.startsWith(ip, "https://"),
               message: () => t("ip.urlNotAllowed")
           }];
       
       
       this.ip.extend(extenders);
       
       this.validationGroup = ko.validatedObservable({
           ip: this.ip
       });
       
       this.isLocalNetwork = ko.pureComputed(() => {
           const ip = this.ip();
           
           if (!ip || !this.validationGroup.isValid()) {
               return false;
           }
           
           return ip === "localhost" || ip === "::1" || ip.startsWith("127.");
       });
   }
   
   static forIp(ip: string, allowHostname: boolean) {
       const entry = new ipEntry(allowHostname);
       entry.ip(ip);
       return entry;
   }
    
    static formatIpAddress(ip: string): string {
        const address = genUtils.getAddressInfo(ip);
        if (address.Type === "ipv6" && !ip.startsWith("[") && !ip.endsWith("]")) {
            return `[${ip}]`;
        }
        return ip;
    }
}

export = ipEntry;
