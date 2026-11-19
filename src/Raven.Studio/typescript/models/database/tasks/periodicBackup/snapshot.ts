import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");

interface compressionLevelOption {
    name: string;
    fullName: System.IO.Compression.CompressionLevel;
}

interface compressionAlgorithmLevelOption {
    name: string;
    fullName: Sparrow.Backups.SnapshotBackupCompressionAlgorithm;
}

class snapshot {
    static get compressionLevelDictionary(): compressionLevelOption[] {
        return [
            { name: t("backup.compressionLevels.optimal"), fullName: "Optimal" },
            { name: t("backup.compressionLevels.fastest"), fullName: "Fastest" },
            { name: t("backup.compressionLevels.noCompression"), fullName: "NoCompression" },
            { name: t("backup.compressionLevels.smallestSize"), fullName: "SmallestSize" }
        ];
    }

    static get compressionAlgorithmDictionary(): compressionAlgorithmLevelOption[] {
        return [
            { name: t("backup.compressionAlgorithms.default"), fullName: null },
            { name: "Zstd", fullName: "Zstd" },
            { name: "Deflate", fullName: "Deflate" }
        ];
    }

    compressionLevelOptions = snapshot.compressionLevelDictionary.map(x => x.name);

    compressionAlgorithmOptions = snapshot.compressionAlgorithmDictionary.map(x => x.name);

    compressionLevel = ko.observable<string>();

    compressionAlgorithm = ko.observable<string>();

    excludeIndexes = ko.observable<boolean>(false);
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.SnapshotSettings) {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.fullName === dto.CompressionLevel);
        const compressionAlgorithm = snapshot.compressionAlgorithmDictionary.find(x => x.fullName === dto.CompressionAlgorithm);

        this.compressionLevel(compressionLevel.name);
        this.compressionAlgorithm(compressionAlgorithm.name);
        this.excludeIndexes(dto.ExcludeIndexes);
    }

    useCompression(option: string) {
        this.compressionLevel(option);
    }

    useAlgorithm(option: string) {
        this.compressionAlgorithm(option);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.SnapshotSettings {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.name === this.compressionLevel());
        const compressionAlgorithm = snapshot.compressionAlgorithmDictionary.find(x => x.name === this.compressionAlgorithm());

        return {
            CompressionLevel: compressionLevel.fullName,
            CompressionAlgorithm: compressionAlgorithm.fullName,
            ExcludeIndexes: this.excludeIndexes()
        }
    }

    static empty(): snapshot {
        return new snapshot({
            CompressionLevel: "Fastest",
            CompressionAlgorithm: "Zstd",
            ExcludeIndexes: false
        });
    }
}

export = snapshot;
