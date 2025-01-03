/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import { exhaustiveStringTuple } from "components/utils/common";

class vectorOptions {
    dimensions = ko.observable<number>();
    sourceEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>();
    destinationEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>();
    numberOfCandidatesForIndexing = ko.observable<number>();
    numberOfEdges = ko.observable<number>();

    vectorEmbeddingTypes = ko.pureComputed(() => exhaustiveStringTuple<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>()(
        "Single", "Int8", "Text", "Binary"
    ))

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Indexes.Vector.VectorOptions) {
        this.dimensions(dto.Dimensions);
        this.sourceEmbeddingType(dto.SourceEmbeddingType);
        this.destinationEmbeddingType(dto.DestinationEmbeddingType);
        this.numberOfCandidatesForIndexing(dto.NumberOfCandidatesForIndexing);
        this.numberOfEdges(dto.NumberOfEdges);

        this.initValidation()

        this.dirtyFlag = new ko.DirtyFlag([
            this.dimensions,
            this.sourceEmbeddingType,
            this.destinationEmbeddingType,
            this.numberOfCandidatesForIndexing,
            this.numberOfEdges,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    toDto(): Raven.Client.Documents.Indexes.Vector.VectorOptions {
        return {
            Dimensions: this.dimensions(),
            SourceEmbeddingType: this.sourceEmbeddingType(),
            DestinationEmbeddingType: this.destinationEmbeddingType(),
            NumberOfCandidatesForIndexing: this.numberOfCandidatesForIndexing(),
            NumberOfEdges: this.numberOfEdges()
        };
    }

    private static getDefaultDto(): Raven.Client.Documents.Indexes.Vector.VectorOptions {
        return {
            Dimensions: null,
            SourceEmbeddingType: "Single",
            DestinationEmbeddingType: "Single",
            NumberOfCandidatesForIndexing: null,
            NumberOfEdges: null
        }
    }
    
    static empty(): vectorOptions {
        return new vectorOptions(this.getDefaultDto());
    }

    private initValidation() {
        this.destinationEmbeddingType.extend({
            required: true,
            validation: [
                {
                    validator: () => this.destinationEmbeddingType() !== "Text",
                    message: "Destination embedding type cannot be Text."
                }
            ]
        });

        this.sourceEmbeddingType.extend({
            required: true,
            validation: [
                {
                    validator: () => !(this.sourceEmbeddingType() === "Text" && this.dimensions() !== null),
                    message: "Dimensions are set internally by the embedder."
                },
                {
                    validator: () => !(this.sourceEmbeddingType() === "Int8" && this.destinationEmbeddingType() !== "Int8"),
                    message: "Quantization cannot be performed on already quantized vector."
                },
                {
                    validator: () => !(this.sourceEmbeddingType() === "Binary" && this.destinationEmbeddingType() !== "Binary"),
                    message: "Quantization cannot be performed on already quantized vector."
                }
            ]
        });

        this.dimensions.extend({
            validation: [
                {
                    validator: () => this.dimensions() >= 0,
                    message: "Number of vector dimensions has to be positive."
                }
            ]
        });

        this.numberOfEdges.extend({
            validation: [
                {
                    validator: () => this.numberOfEdges() >= 0,
                    message: "Number of edges has to be positive."
                },
            ]
        });

        this.numberOfCandidatesForIndexing.extend({
            validation: [
                {
                    validator: () => this.numberOfCandidatesForIndexing() >= 0,
                    message: "Number of candidate nodes has to be positive."
                },
            ]
        });
    }
}
export = vectorOptions; 
