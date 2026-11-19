/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");

class ongoingTaskReplicationHubEditModel {

    taskId: number;
    taskName = ko.observable<string>();
    taskType = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType>();

    disabled = ko.observable<boolean>();
    stateText: KnockoutComputed<string>;

    responsibleNode = ko.observable<Raven.Client.ServerWide.Operations.NodeId>();
    
    manualChooseMentor = ko.observable<boolean>();
    pinMentorNode = ko.observable<boolean>(false);
    mentorNode = ko.observable<string>();
    nodeTag: string = null;
    
    delayReplicationTime = ko.observable<number>();
    showDelayReplication = ko.observable<boolean>();
    humaneDelayDescription: KnockoutComputed<string>;
    
    allowReplicationFromHubToSink = ko.observable<boolean>(true);
    allowReplicationFromSinkToHub = ko.observable<boolean>();
    replicationMode: KnockoutComputed<Raven.Client.Documents.Operations.Replication.PullReplicationMode>;
   
    preventDeletions = ko.observable<boolean>();
    withFiltering = ko.observable<boolean>();
    
    validationGroupForSave: KnockoutValidationGroup;
    validationGroupForExport: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        this.humaneDelayDescription = ko.pureComputed(() => {
            const delayTimeHumane = generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true);
            return this.showDelayReplication() && this.delayReplicationTime.isValid() && this.delayReplicationTime() !== 0 ?
                t("delayDescription", { delayTimeHumane: generalUtils.escapeHtml(delayTimeHumane) }) : "";
        });
        
        this.replicationMode = ko.pureComputed(() => {
            
            if (this.allowReplicationFromHubToSink() && this.allowReplicationFromSinkToHub()) {
                return "HubToSink,SinkToHub" as Raven.Client.Documents.Operations.Replication.PullReplicationMode;
            }

            return (this.allowReplicationFromHubToSink()) ? "HubToSink" :
                    this.allowReplicationFromSinkToHub() ? "SinkToHub" : "None";
        })

        this.stateText = ko.pureComputed(() => {
            if (this.disabled()) {
                return t("state.disabled");
            }
            
            return t("state.enabled");
        });
    }
    
    update(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        this.taskName(dto.Name);
        this.taskId = dto.TaskId;
        this.disabled(dto.Disabled);

        this.manualChooseMentor(!!dto.MentorNode);
        this.pinMentorNode(dto.PinToMentorNode);
        this.mentorNode(dto.MentorNode);

        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);
        
        this.allowReplicationFromHubToSink(dto.Mode.includes("HubToSink"));
        this.allowReplicationFromSinkToHub(dto.Mode.includes("SinkToHub"));
        
        this.preventDeletions(dto.PreventDeletionsMode === "PreventSinkToHubDeletions");
        this.withFiltering(dto.WithFiltering);
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.PullReplicationDefinition {
        return {
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            TaskId: taskId,
            DelayReplicationFor: this.showDelayReplication() ? generalUtils.formatAsTimeSpan(this.delayReplicationTime() * 1000) : null,
            Mode: this.replicationMode(),
            Disabled: this.disabled(),
            PreventDeletionsMode: this.preventDeletions() ? "PreventSinkToHubDeletions" : "None",
            WithFiltering: this.withFiltering()
        };
    }
    
    initValidation() {
        this.taskName.extend({
            required: true
        });
        
        this.mentorNode.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
        
        this.delayReplicationTime.extend({
            validation: [
                {
                    validator: () => !this.showDelayReplication() || !!this.delayReplicationTime(),
                    message: t("validation.valueGreaterThanZero")
                }
            ],
            min: 0
        });
        
        this.replicationMode.extend({
            validation: [
                {
                    validator: () => this.replicationMode() !== "None",
                    message: t("validation.replicationModeRequired")
                }
            ]
        })
        
        this.validationGroupForSave = ko.validatedObservable({
            taskName: this.taskName,
            mentorNode: this.mentorNode,
            delayReplicationTime: this.delayReplicationTime,
            replicationMode: this.replicationMode
        });

        this.validationGroupForExport = ko.validatedObservable({
            taskName: this.taskName,
            mentorNode: this.mentorNode,
            delayReplicationTime: this.delayReplicationTime
        });
    }

    static empty(): ongoingTaskReplicationHubEditModel {
        return new ongoingTaskReplicationHubEditModel({
            Name: "",
            DelayReplicationFor: null,
            Disabled: false,
            MentorNode: null,
            TaskId: null,
            PreventDeletionsMode: "None",
            WithFiltering: false,
            Mode: "HubToSink"
        } as Raven.Client.Documents.Operations.Replication.PullReplicationDefinition);
    }

    togglePinMentorNode() {
        this.pinMentorNode.toggle();
    }
}

export = ongoingTaskReplicationHubEditModel;
